package com.facebook.presto.raptor.metadata;

import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.spi.NodeManager;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Comparator.comparingLong;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;

public class OldestQueryManager
{
    private static final Logger log = Logger.get(OldestQueryManager.class);

    private final String currentNode;
    private final boolean coordinator;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean();
    private final Map<String, Long> queries = new ConcurrentHashMap<>();

    @Inject
    public OldestQueryManager(RaptorConnectorId connectorId, NodeManager nodeManager)
    {
        this(connectorId.toString(),
                nodeManager.getCurrentNode().getNodeIdentifier(),
                nodeManager.getCoordinators().contains(nodeManager.getCurrentNode()));
    }

    public OldestQueryManager(String connectorId, String currentNode, boolean coordinator)
    {
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.coordinator = coordinator;
        this.scheduler = newScheduledThreadPool(1, daemonThreadsNamed(connectorId + "-oldest-query-manager"));
    }

    @PostConstruct
    public void start()
    {
        if (coordinator && !started.getAndSet(true)) {
            scheduler.scheduleWithFixedDelay(() -> {
                try {
                    update();
                }
                catch (Throwable t) {
                    log.error(t, "Error updating oldest query");
                }
            }, 0, 1, MINUTES);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        scheduler.shutdownNow();
    }

    public void queryStarted(String queryId)
    {
        queries.put(queryId, System.currentTimeMillis());
    }

    public void queryFinished(String queryId)
    {
        queries.remove(queryId);
    }

    @VisibleForTesting
    void update()
    {
        Optional<Entry<String, Long>> entry = queries.entrySet().stream()
                .min(comparingLong(Entry::getValue));

        if (entry.isPresent()) {
            // create or update
        }
        else {
            // delete
        }
    }
}
