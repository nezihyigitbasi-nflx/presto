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

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class MysqlJdbcClient
        extends BaseJdbcClient
{
    @Inject
    public MysqlJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config);
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
                ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase();
                // skip the internal schemas
                if (schemaName.equals("information_schema") || schemaName.equals("mysql")) {
                    continue;
                }
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Set<String> getTableNames(String schema)
    {
        checkNotNull(schema, "schema is null");
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getTables(schema, null, null, null)) {
                ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
                while (resultSet.next()) {
                    tableNames.add(resultSet.getString(3).toLowerCase());
                }
                return tableNames.build();
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        checkNotNull(schemaTableName, "schemaTableName is null");
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metaData = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            try (ResultSet resultSet = metaData.getTables(jdbcSchemaName, null, jdbcTableName, null)) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(connectorId, schemaTableName, resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
                }
                if (tableHandles.isEmpty() || tableHandles.size() > 1) {
                    return null;
                }
                return Iterables.getOnlyElement(tableHandles);
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public String buildSql(JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        Joiner.on(", ").appendTo(sql, Iterables.transform(columnHandles, nameGetter()));
        sql.append(" FROM `").append(split.getCatalogName()).append("`.`").append(split.getTableName()).append("`");
        System.out.println(sql);
        return sql.toString();
    }

    public static Function<JdbcColumnHandle, String> nameGetter()
    {
        return new Function<JdbcColumnHandle, String>()
        {
            @Override
            public String apply(JdbcColumnHandle columnHandle)
            {
                return "`" + columnHandle.getColumnName() + "`";
            }
        };
    }
}
