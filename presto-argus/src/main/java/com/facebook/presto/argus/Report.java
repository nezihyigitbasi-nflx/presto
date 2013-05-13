package com.facebook.presto.argus;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.PeregrineSqlParser;
import com.facebook.presto.sql.tree.Statement;
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
    private final String runnablePeregrineQuery;
    private final String translatedPrestoQuery;
    private final String runnablePrestoQuery;

    public Report(long reportId, String namespace, String query, Map<String, String> variables, long views)
    {
        this.reportId = reportId;
        this.namespace = checkNotNull(namespace, "namespace is null");
        this.query = checkNotNull(query, "query is null");
        this.variables = checkNotNull(variables, "variables is null");
        this.views = views;

        String sql = removePeregrineSettings(removeTrailingTerminator(query));
        String translated = translateQuery(sql);
        this.runnablePeregrineQuery = cleanQuery(sql, variables);
        this.translatedPrestoQuery = translated;
        this.runnablePrestoQuery = cleanQuery(translated, variables);
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

    public String getRunnablePeregrineQuery()
    {
        return runnablePeregrineQuery;
    }

    public String getTranslatedPrestoQuery()
    {
        return translatedPrestoQuery;
    }

    public String getRunnablePrestoQuery()
    {
        return runnablePrestoQuery;
    }

    private static String cleanQuery(String sql, Map<String, String> variables)
    {
        sql = replaceDateMacros(sql);
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

    private static String removeTrailingTerminator(String sql)
    {
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        return sql;
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

    private static String translateQuery(String sql)
    {
        Statement statement;
        try {
            statement = PeregrineSqlParser.createStatement(sql);
        }
        catch (ParsingException e) {
            if (false) {
                if (sql.toLowerCase().replaceAll("\\s", " ").contains(" union all ")) {
                    return sql;
                }
                if (sql.toLowerCase().replaceAll("\\s", " ").contains(" rlike ")) {
                    return sql;
                }
                if (e.getMessage().endsWith("no viable alternative at character '['")) {
                    return sql;
                }
                if (e.getMessage().endsWith("no viable alternative at input '*'")) {
                    return sql;
                }
                System.err.println(e);
                System.err.println(sql);
                System.err.println("----------");
            }
            return sql;
        }

        return SqlFormatter.formatSql(statement);
    }
}
