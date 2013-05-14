package com.facebook.presto.argus;

import com.google.common.collect.ImmutableMap;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ReportMapper
        implements ResultSetMapper<Report>
{
    @Override
    public Report map(int index, ResultSet rs, StatementContext ctx)
            throws SQLException
    {
        return new Report(
                rs.getLong("report_id"),
                rs.getString("namespace"),
                rs.getString("sql_query"),
                ImmutableMap.<String, String>of()
        );
    }
}
