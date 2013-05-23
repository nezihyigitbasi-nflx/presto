package com.facebook.presto.argus;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import org.joda.time.DateTime;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.argus.ArgusConverter.PRESTO_GATEWAY;
import static com.facebook.presto.argus.ArgusConverter.TEST_USER;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Integer.parseInt;
import static org.joda.time.format.ISODateTimeFormat.date;

public class CleanedQuery
{
    private static final PartitionFetcher PARTITION_FETCHER = new PartitionFetcher(TEST_USER, PRESTO_GATEWAY);

    private final String query;
    private final boolean valid;
    private final Exception exception;

    public CleanedQuery(String namespace, String sql, Map<String, String> variables)
    {
        checkNotNull(sql, "sql is null");

        boolean valid = true;
        Exception exception = null;
        try {
            sql = cleanQuery(namespace, sql, variables);
        }
        catch (RuntimeException e) {
            valid = false;
            exception = e;
        }
        this.query = sql;
        this.valid = valid;
        this.exception = exception;
    }

    public String getQuery()
    {
        return query;
    }

    public boolean isValid()
    {
        return valid;
    }

    public Exception getException()
    {
        return exception;
    }

    private static String cleanQuery(String namespace, String sql, Map<String, String> variables)
    {
        sql = replaceDateMacros(sql);
        sql = replacePartitionMacros(namespace, sql);
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

    private static String replacePartitionMacros(String namespace, String sql)
    {
        StringBuffer sb = new StringBuffer();
        Matcher matcher = Pattern.compile("<LATEST_PARTITION,([^>]+)>").matcher(sql);
        while (matcher.find()) {
            String argString = matcher.group(1).trim();
            List<String> args = ImmutableList.copyOf(Splitter.on(',').trimResults().split(argString).iterator());
            String partition = getLatestPartition(namespace, args);
            matcher.appendReplacement(sb, partition);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static String getLatestPartition(String namespace, List<String> args)
    {
        Iterator<String> iter = args.iterator();
        if (!iter.hasNext()) {
            throw new IllegalArgumentException("No table specified");
        }
        String table = iter.next();
        if (table.isEmpty()) {
            throw new IllegalArgumentException("Table name is empty");
        }

        String partitionName = "ds";
        if (iter.hasNext()) {
            partitionName = iter.next();
            if (!partitionName.equals("ds")) {
                throw new IllegalArgumentException("Partition name must be 'ds': " + partitionName);
            }
        }

        SortedSet<String> partitions = getDatePartitions(namespace, table, partitionName);
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException("No date partitions");
        }
        String mostRecent = partitions.last();

        if (!iter.hasNext()) {
            return mostRecent;
        }
        String earliest = iter.next();
        if (mostRecent.compareTo(earliest) < 0) {
            throw new IllegalArgumentException("No partitions later than " + earliest);
        }

        if (!iter.hasNext()) {
            return mostRecent;
        }
        String latest = iter.next();
        if (iter.hasNext()) {
            throw new IllegalArgumentException("Too many arguments: " + args);
        }
        if (mostRecent.compareTo(latest) <= 0) {
            return mostRecent;
        }
        return latest;
    }

    private static SortedSet<String> getDatePartitions(String namespace, String table, String partitionName)
    {
        return ImmutableSortedSet.copyOf(PARTITION_FETCHER.getDatePartitions(partitionName, namespace, table));
    }

    private static String replaceVariables(String sql, Map<String, String> variables)
    {
        sql = sql.replace("'$date_picker$'", "'" + date().print(new DateTime().minusDays(1)) + "'");
        for (Map.Entry<String, String> entry : variables.entrySet()) {
            sql = sql.replace("$" + entry.getKey() + "$", entry.getValue());
        }
        return sql;
    }
}
