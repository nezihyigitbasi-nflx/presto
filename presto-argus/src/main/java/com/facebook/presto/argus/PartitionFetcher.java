package com.facebook.presto.argus;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class PartitionFetcher
{
    private final String username;
    private final HostAndPort prestoGateway;

    public PartitionFetcher(String username, HostAndPort prestoGateway)
    {
        this.username = checkNotNull(username, "username is null");
        this.prestoGateway = checkNotNull(prestoGateway, "prestoGateway is null");
    }

    public Set<String> getDatePartitions(String partitionName, String namespace, String table)
    {
        String url = format("jdbc:presto://%s/", prestoGateway);
        String sql = format("SHOW PARTITIONS FROM \"%s\"", table);

        try (Connection connection = DriverManager.getConnection(url, username, null)) {
            connection.setCatalog("prism");
            connection.setSchema(namespace);

            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(sql)) {
                if (!hasColumnLabel(rs, partitionName)) {
                    throw new RuntimeException(format("table %s.%s has no '%s' column", partitionName, namespace, table));
                }
                ImmutableSet.Builder<String> set = ImmutableSet.builder();
                while (rs.next()) {
                    String date = rs.getString(partitionName);
                    if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
                        set.add(date);
                    }
                }
                return set.build();
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private static boolean hasColumnLabel(ResultSet rs, String name)
            throws SQLException
    {
        ResultSetMetaData metadata = rs.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            if (metadata.getColumnLabel(i).equals(name)) {
                return true;
            }
        }
        return false;
    }
}
