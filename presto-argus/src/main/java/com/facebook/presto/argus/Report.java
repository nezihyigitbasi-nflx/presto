package com.facebook.presto.argus;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class Report
{
    private final long reportId;
    private final String namespace;
    private final String originalQuery;
    private final Map<String, String> variables;
    private final String query;
    private final long views;

    public Report(long reportId, String namespace, String originalQuery, Map<String, String> variables, long views)
    {
        this.reportId = reportId;
        this.namespace = checkNotNull(namespace, "namespace is null");
        this.originalQuery = checkNotNull(originalQuery, "originalQuery is null");
        this.variables = checkNotNull(variables, "variables is null");
        this.query = removePeregrineSettings(removeTrailingTerminator(originalQuery));
        this.views = views;
    }

    public long getReportId()
    {
        return reportId;
    }

    public String getNamespace()
    {
        return namespace;
    }

    public String getOriginalQuery()
    {
        return originalQuery;
    }

    public Map<String, String> getVariables()
    {
        return variables;
    }

    public String getQuery()
    {
        return query;
    }

    public long getViews()
    {
        return views;
    }

    private static String removeTrailingTerminator(String sql)
    {
        sql = sql.trim();
        while (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        return sql;
    }

    private static String removePeregrineSettings(String sql)
    {
        sql = sql.replaceFirst("(?i)^WITH\\s+\\d+\\s+AS\\s+mapper.buffersize\\s+", "");
        sql = sql.replaceFirst("(?i)^WITH\\s+true\\s+AS\\s+mode.exact", "");
        return sql;
    }
}
