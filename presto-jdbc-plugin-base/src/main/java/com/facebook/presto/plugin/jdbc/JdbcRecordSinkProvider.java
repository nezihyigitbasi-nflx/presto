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

import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.OutputTableHandle;
import com.facebook.presto.spi.RecordSink;

import javax.inject.Inject;

import static com.facebook.presto.plugin.jdbc.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcRecordSinkProvider
        implements ConnectorRecordSinkProvider
{
    private final String connectorId;
    private final JdbcClient jdbcClient;

    @Inject
    public JdbcRecordSinkProvider(JdbcConnectorId connectorId, JdbcClient jdbcClient)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.jdbcClient = checkNotNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public boolean canHandle(OutputTableHandle handle)
    {
        return (handle instanceof JdbcOutputTableHandle) && ((JdbcOutputTableHandle) handle).getConnectorId().equals(connectorId);
    }

    @Override
    public RecordSink getRecordSink(OutputTableHandle handle)
    {
        return new JdbcRecordSink(checkType(handle, JdbcOutputTableHandle.class, "handle"), jdbcClient);
    }
}
