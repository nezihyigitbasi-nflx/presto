/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.raptor.NodeSupplier;
import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.backup.BackupService;
import com.facebook.presto.raptor.storage.StorageService;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.IDBI;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.collect.Maps.filterValues;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.round;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class BucketBalancer
{
    private static final Logger log = Logger.get(BucketBalancer.class);

    private final MetadataDao metadataDao;
    private final NodeSupplier nodeSupplier;
    private final ShardManager shardManager;
    private final StorageService storageService;
    private final Duration interval;
    private final boolean enabled;
    private final ScheduledExecutorService executor;

    private final AtomicBoolean started = new AtomicBoolean();

    private final CounterStat bucketsMoved = new CounterStat();
    private final CounterStat jobErrors = new CounterStat();

    @Inject
    public BucketBalancer(
            @ForMetadata IDBI dbi,
            NodeManager nodeManager,
            NodeSupplier nodeSupplier,
            ShardManager shardManager,
            StorageService storageService,
            BucketBalancerConfig config,
            BackupService backupService,
            RaptorConnectorId connectorId)
    {
        this(dbi,
                nodeSupplier,
                shardManager,
                storageService,
                config.getInterval(),
                config.isEnabled() &&
                        backupService.isBackupAvailable() &&
                        nodeManager.getCoordinators().contains(nodeManager.getCurrentNode()),
                connectorId.toString());
    }

    public BucketBalancer(
            IDBI dbi,
            NodeSupplier nodeSupplier,
            ShardManager shardManager,
            StorageService storageService,
            Duration interval,
            boolean enabled,
            String connectorId)
    {
        this.metadataDao = onDemandDao(dbi, MetadataDao.class);
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.interval = requireNonNull(interval, "interval is null");
        this.enabled = enabled;
        this.executor = newScheduledThreadPool(1, daemonThreadsNamed("bucket-balancer-" + connectorId));
    }

    @PostConstruct
    public void start()
    {
        if (enabled && !started.getAndSet(true)) {
            startJob();
        }
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Managed
    @Nested
    public CounterStat getBucketsMoved()
    {
        return bucketsMoved;
    }

    @Managed
    @Nested
    public CounterStat getJobErrors()
    {
        return jobErrors;
    }

    private void startJob()
    {
        executor.scheduleWithFixedDelay(() -> {
            try {
                process();
            }
            catch (Throwable t) {
                log.error(t, "Error balancing buckets");
                jobErrors.update(1);
            }
        }, 0, interval.toMillis(), MILLISECONDS);
    }

    @VisibleForTesting
    void process()
    {
        for (long distributionId : metadataDao.getActiveDistributionIds()) {
            process(distributionId);
        }
    }

    private void process(long distributionId)
    {
        // get active nodes
        Set<String> activeNodes = nodeSupplier.getWorkerNodes().stream()
                .map(Node::getNodeIdentifier)
                .collect(toSet());

        // get the size of shards per bucket, largest to smallest
        List<BucketNodeSize> bucketNodeSize = shardManager.getBucketNodeSizes(distributionId).stream()
                .sorted(comparingLong(BucketNodeSize::getSize).reversed())
                .collect(toList());

        // compute size of each node
        Map<String, Long> nodeSize = new HashMap<>();
        for (BucketNodeSize info : bucketNodeSize) {
            nodeSize.merge(info.getNodeIdentifier(), info.getSize(), Math::addExact);
        }
        for (String node : activeNodes) {
            nodeSize.putIfAbsent(node, 0L);
        }

        // get buckets by node, largest to smallest
        ListMultimap<String, BucketNodeSize> bucketsByNode = ArrayListMultimap.create();



        // get average active node size
        long averageSize = round(nodeSize.entrySet().stream()
                .filter(entry -> activeNodes.contains(entry.getKey()))
                .map(Entry::getValue)
                .mapToLong(Long::longValue)
                .average()
                .orElse(0));
        long maxSize = round(averageSize * 1.01);

        // get nodes above max size, largest to smallest
        List<String> sourceNodes = reverse(nodeSize.entrySet().stream()
                .sorted(comparing(Entry::getValue))
                .filter(entry -> (entry.getValue() > maxSize))
                .map(Entry::getKey)
                .collect(toList()));

        // get nodes below average size, smallest to largest
        List<String> targetNodes = nodeSize.entrySet().stream()
                .sorted(comparing(Entry::getValue))
                .filter(entry -> (entry.getValue() <= averageSize))
                .map(Entry::getKey)
                .collect(toList());

        // skip if not above max
        if (nodeSize <= maxSize) {
            return;
        }

        // only include nodes that are below threshold
        nodeSize = new HashMap<>(filterValues(nodeSize, size -> size <= averageSize));

        // get node shards by size, largest to smallest
        List<ShardMetadata> shards = shardManager.getNodeShards(currentNode).stream()
                .sorted(comparingLong(ShardMetadata::getCompressedSize).reversed())
                .collect(toList());

        // eject shards while current node is above max
        Queue<ShardMetadata> queue = new ArrayDeque<>(shards);
        while ((nodeSize > maxSize) && !queue.isEmpty()) {
            ShardMetadata shard = queue.remove();
            long shardSize = shard.getCompressedSize();
            UUID shardUuid = shard.getShardUuid();

            // verify backup exists
            if (!backupStore.get().shardExists(shardUuid)) {
                log.warn("No backup for shard: %s", shardUuid);
            }

            // pick target node
            String target = pickTargetNode(nodeSize, shardSize, averageSize);
            if (target == null) {
                return;
            }
            long targetSize = nodeSize.get(target);

            // stats
            log.info("Moving shard %s to node %s (shard: %s, node: %s, average: %s, target: %s)",
                    shardUuid, target, shardSize, nodeSize, averageSize, targetSize);
            bucketsMoved.update(1);

            // update size
            nodeSize.put(target, targetSize + shardSize);
            nodeSize -= shardSize;

            // move assignment
            shardManager.assignShard(shard.getTableId(), shardUuid, target);
            shardManager.unassignShard(shard.getTableId(), shardUuid, currentNode);

            // delete local file
            File file = storageService.getStorageFile(shardUuid);
            if (file.exists() && !file.delete()) {
                log.warn("Failed to delete shard file: %s", file);
            }
        }
    }

    private static String pickTargetNode(Map<String, Long> nodes, long shardSize, long maxSize)
    {
        while (!nodes.isEmpty()) {
            String node = pickCandidateNode(nodes);
            if ((nodes.get(node) + shardSize) <= maxSize) {
                return node;
            }
            nodes.remove(node);
        }
        return null;
    }

    private static String pickCandidateNode(Map<String, Long> nodes)
    {
        checkArgument(!nodes.isEmpty());
        if (nodes.size() == 1) {
            return nodes.keySet().iterator().next();
        }

        // pick two random candidates, then choose the smaller one
        List<String> candidates = new ArrayList<>(nodes.keySet());
        Collections.shuffle(candidates);
        String first = candidates.get(0);
        String second = candidates.get(1);
        return (nodes.get(first) <= nodes.get(second)) ? first : second;
    }
}
