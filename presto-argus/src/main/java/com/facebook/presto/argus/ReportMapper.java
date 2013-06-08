package com.facebook.presto.argus;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;

public class ReportMapper
        implements ResultSetMapper<Report>
{
    private static final JsonCodec<Map<String, Object>> CODEC = JsonCodec.mapJsonCodec(String.class, Object.class);

    @Override
    public Report map(int index, ResultSet rs, StatementContext ctx)
            throws SQLException
    {
        return new Report(
                rs.getLong("report_id"),
                rs.getString("namespace"),
                rs.getString("sql_query"),
                parseVariables(rs.getString("settings")),
                rs.getLong("views"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> parseVariables(String json)
    {
        if (isNullOrEmpty(json) || "null".equals(json) || "[]".equals(json)) {
            return ImmutableMap.of();
        }
        Map<String, Object> map = CODEC.fromJson(json);
        ImmutableMap.Builder<String, String> variables = ImmutableMap.builder();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                Map<String, Object> valueMap = (Map<String, Object>) entry.getValue();
                if (valueMap.containsKey("default")) {
                    Object value = valueMap.get("default");
                    if (value instanceof String) {
                        variables.put(entry.getKey(), (String) value);
                    }
                }
            }
        }
        return variables.build();
    }
}
