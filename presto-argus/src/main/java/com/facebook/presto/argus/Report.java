package com.facebook.presto.argus;

import com.google.common.base.Objects;
import org.joda.time.DateTime;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Integer.parseInt;
import static org.joda.time.format.ISODateTimeFormat.date;

public class Report
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
