package com.facebook.presto.argus;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.json.JsonCodec;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.Integer.parseInt;
import static org.joda.time.format.ISODateTimeFormat.date;

public final class ArgusReports
{
    public static final JsonCodec<Map<String, Object>> CODEC = JsonCodec.mapJsonCodec(String.class, Object.class);

    private ArgusReports() {}

    @SuppressWarnings("AssignmentToForLoopParameter")
    public static List<Report> loadReports()
            throws IOException
    {
        File file = new File("/Users/dphillips/tmp/argus.tsv");
        String data = Files.toString(file, Charsets.US_ASCII);
        data = data.replace("\r", "");

        ImmutableList.Builder<Report> reports = ImmutableList.builder();
        for (String line : Splitter.on('\n').split(data)) {
            if (line.isEmpty()) {
                continue;
            }
            line = line.replace("\\n", "\n");
            Iterator<String> iter = Splitter.on('\t').split(line).iterator();
            long reportId = Long.parseLong(iter.next());
            String namespace = iter.next();
            String query = unescape(iter.next());
            Map<String, String> variables = parseVariables(unescape(iter.next()));
            long views = Long.parseLong(iter.next());
            reports.add(new Report(reportId, namespace, query, variables, views));
        }
        return reports.build();
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

    private static String unescape(String s)
    {
        s = s.replace("\\t", "\t").replace("\\\\", "\\");
        return s.equals("NULL") ? null : s;
    }

    public static class Report
    {
        private final long reportId;
        private final String namespace;
        private final String query;
        private final Map<String, String> variables;
        private final long views;
        private final String cleanQuery;

        public Report(long reportId, String namespace, String query, Map<String, String> variables, long views)
        {
            this.reportId = reportId;
            this.namespace = checkNotNull(namespace, "namespace is null");
            this.query = checkNotNull(query, "query is null");
            this.variables = checkNotNull(variables, "variables is null");
            this.views = views;
            this.cleanQuery = cleanQuery(query, variables);
        }

        public long getReportId()
        {
            return reportId;
        }

        public String getNamespace()
        {
            return namespace;
        }

        public String getQuery()
        {
            return query;
        }

        public Map<String, String> getVariables()
        {
            return variables;
        }

        public long getViews()
        {
            return views;
        }

        public String getCleanQuery()
        {
            return cleanQuery;
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("reportId", reportId)
                    .add("namespace", namespace)
                    .add("query", query)
                    .add("variables", variables)
                    .add("views", views)
                    .add("cleanQuery", cleanQuery)
                    .toString();
        }

        private static String cleanQuery(String sql, Map<String, String> variables)
        {
            sql = replaceDateMacros(sql);
            sql = removePeregrineSettings(sql);
            sql = replaceVariables(sql, variables);
            sql = sql.replaceAll("\n+", "\n").trim();
            return sql;
        }

        private static String replaceDateMacros(String sql)
        {
            DateTime date = new DateTime().minusDays(1);
            sql = sql.replace("<DATEID>", date().print(date));

            StringBuffer sb = new StringBuffer();
            Matcher matcher = Pattern.compile("<DATEID([+-]\\d+)>").matcher(sql);
            while (matcher.find()) {
                int days = parseInt(matcher.group(1));
                matcher.appendReplacement(sb, date().print(date.plusDays(days)));
            }
            matcher.appendTail(sb);
            return sb.toString();
        }

        private static String removePeregrineSettings(String sql)
        {
            sql = sql.replaceFirst("(?i)^WITH\\s+\\d+\\s+AS\\s+mapper.buffersize\\s+", "");
            sql = sql.replaceFirst("(?i)^WITH\\s+true\\s+AS\\s+mode.exact", "");
            return sql;
        }

        private static String replaceVariables(String sql, Map<String, String> variables)
        {
            for (Map.Entry<String, String> entry : variables.entrySet()) {
                sql = sql.replace("$" + entry.getKey() + "$", entry.getValue());
            }
            return sql;
        }
    }
}
