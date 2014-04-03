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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.SplitSource;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final JdbcClient jdbcClient;

    @Inject
    public JdbcSplitManager(JdbcConnectorId connectorId, JdbcClient jdbcClient)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.jdbcClient = checkNotNull(jdbcClient, "client is null");
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof JdbcTableHandle && ((JdbcTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public PartitionResult getPartitions(TableHandle tableHandle, TupleDomain tupleDomain)
    {
        checkArgument(tableHandle instanceof JdbcTableHandle, "tableHandle is not an instance of ExampleTableHandle");
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) tableHandle;

        // example connector has only one partition
        return jdbcClient.getPartitions(jdbcTableHandle, tupleDomain);
    }

    @Override
    public SplitSource getPartitionSplits(TableHandle tableHandle, List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        checkArgument(partitions.size() == 1, "Expected one partition but got %s", partitions.size());
        Partition partition = partitions.get(0);

        checkArgument(partition instanceof JdbcPartition, "partition is not an instance of ExamplePartition");
        JdbcPartition jdbcPartition = (JdbcPartition) partition;

        return jdbcClient.getPartitionSplits(jdbcPartition);
    }
}
