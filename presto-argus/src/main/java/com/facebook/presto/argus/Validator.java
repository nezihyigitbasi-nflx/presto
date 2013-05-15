package com.facebook.presto.argus;

import com.facebook.presto.argus.peregrine.PeregrineErrorCode;
import com.facebook.presto.argus.peregrine.PeregrineException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.units.Duration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;

public class Validator
{
    private static final Duration TIME_LIMIT = new Duration(1, TimeUnit.MINUTES);

    public enum PeregrineState
    {
        UNKNOWN, TIMEOUT, INVALID, MEMORY, FAILED, SUCCESS
    }

    public enum PrestoState
    {
        UNKNOWN, TIMEOUT, INVALID, FAILED, SUCCESS
    }

    private final String username;
    private final HostAndPort prestoGateway;
    private final Report report;

    private PeregrineState peregrineState = PeregrineState.UNKNOWN;
    private PrestoState prestoState = PrestoState.UNKNOWN;
    private boolean resultsMatch;

    private String runnablePeregrineQuery;
    private String translatedPrestoQuery;
    private String runnablePrestoQuery;

    private Exception peregrineException;
    private Exception prestoException;

    private Duration peregrineTime;
    private Duration prestoTime;

    private List<List<Object>> peregrineResults;
    private List<List<Object>> prestoResults;

    public Validator(String username, HostAndPort prestoGateway, Report report)
    {
        this.username = checkNotNull(username, "username is null");
        this.prestoGateway = checkNotNull(prestoGateway, "prestoGateway is null");
        this.report = checkNotNull(report, "report is null");
    }

    public boolean valid()
    {
        return canPeregrineExecute() &&
                canPrestoExecute() &&
                compareResults();
    }

    public Report getReport()
    {
        return report;
    }

    public PeregrineState getPeregrineState()
    {
        return peregrineState;
    }

    public PrestoState getPrestoState()
    {
        return prestoState;
    }

    public boolean resultsMatch()
    {
        return resultsMatch;
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

    public Exception getPeregrineException()
    {
        return peregrineException;
    }

    public Exception getPrestoException()
    {
        return prestoException;
    }

    public Duration getPeregrineTime()
    {
        return peregrineTime;
    }

    public Duration getPrestoTime()
    {
        return prestoTime;
    }

    public List<List<Object>> getPeregrineResults()
    {
        checkState(peregrineResults != null);
        return peregrineResults;
    }

    public List<List<Object>> getPrestoResults()
    {
        checkState(prestoResults != null);
        return prestoResults;
    }

    public String getResultsComparison()
    {
        if (resultsMatch || (peregrineResults == null) || (prestoResults == null)) {
            return "";
        }

        Multiset<List<Object>> peregrine = ImmutableSortedMultiset.copyOf(rowComparator(), getPeregrineResults());
        Multiset<List<Object>> presto = ImmutableSortedMultiset.copyOf(rowComparator(), getPrestoResults());
        StringBuilder sb = new StringBuilder();

        sb.append(format("Peregrine %s rows, Presto %s rows%n", peregrine.size(), presto.size()));
        if (peregrine.size() == presto.size()) {
            Iterator<List<Object>> peregrineIter = peregrine.iterator();
            Iterator<List<Object>> prestoIter = presto.iterator();
            int i = 0;
            int n = 0;
            while (i < peregrine.size()) {
                i++;
                List<Object> peregrineRow = peregrineIter.next();
                List<Object> prestoRow = prestoIter.next();
                if (!peregrineRow.equals(prestoRow)) {
                    sb.append(format("%s: %s: %s %s%n", i,
                            peregrineRow.equals(prestoRow),
                            peregrineRow, prestoRow));
                    n++;
                    if (n >= 100) {
                        break;
                    }
                }
            }
        }
        return sb.toString();
    }

    private boolean compareResults()
    {
        Multiset<List<Object>> peregrine = ImmutableSortedMultiset.copyOf(rowComparator(), getPeregrineResults());
        Multiset<List<Object>> presto = ImmutableSortedMultiset.copyOf(rowComparator(), getPrestoResults());
        resultsMatch = peregrine.equals(presto);
        return resultsMatch;
    }

    private boolean canPeregrineExecute()
    {
        CleanedQuery cleaned = new CleanedQuery(report.getNamespace(), report.getQuery(), report.getVariables());
        if (!cleaned.isValid()) {
            peregrineState = PeregrineState.INVALID;
            peregrineException = cleaned.getException();
            return false;
        }
        this.runnablePeregrineQuery = cleaned.getQuery();

        try (PeregrineRunner runner = new PeregrineRunner(TIME_LIMIT)) {
            long start = System.nanoTime();
            peregrineResults = runner.execute(username, report.getNamespace(), runnablePeregrineQuery);
            peregrineState = PeregrineState.SUCCESS;
            peregrineTime = nanosSince(start);
            return true;
        }
        catch (PeregrineException e) {
            peregrineException = e;
            if (isPeregrineQueryInvalid(e)) {
                peregrineState = PeregrineState.INVALID;
            }
            else if (e.getCode() == PeregrineErrorCode.OUT_OF_MEMORY) {
                peregrineState = PeregrineState.MEMORY;
            }
            else {
                peregrineState = PeregrineState.FAILED;
            }
        }
        catch (UncheckedTimeoutException e) {
            peregrineState = PeregrineState.TIMEOUT;
        }
        return false;
    }

    private boolean canPrestoExecute()
    {
        return canPrestoExecute(report.getQuery()) ||
                canPrestoExecute(QueryTranslator.translateQuery(report.getQuery()));
    }

    private boolean canPrestoExecute(String sql)
    {
        translatedPrestoQuery = sql;
        prestoState = PrestoState.UNKNOWN;
        prestoException = null;

        CleanedQuery cleaned = new CleanedQuery(report.getNamespace(), sql, report.getVariables());
        if (!cleaned.isValid()) {
            prestoState = PrestoState.INVALID;
            prestoException = cleaned.getException();
            return false;
        }
        runnablePrestoQuery = cleaned.getQuery();

        String url = format("jdbc:presto://%s/", prestoGateway);

        try (Connection connection = DriverManager.getConnection(url, username, null)) {
            connection.setCatalog("prism");
            connection.setSchema(report.getNamespace());
            long start = System.nanoTime();

            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(runnablePrestoQuery)) {
                prestoResults = convertJdbcResultSet(resultSet);
                prestoState = PrestoState.SUCCESS;
                prestoTime = nanosSince(start);
                return true;
            }
        }
        catch (SQLException e) {
            prestoException = e;
            prestoState = isPrestoQueryInvalid(e) ? PrestoState.INVALID : PrestoState.FAILED;
            return false;
        }
    }

    @SuppressWarnings("RedundantIfStatement")
    private static boolean isPeregrineQueryInvalid(PeregrineException e)
    {
        if (e.getCode().isInvalidQuery()) {
            return true;
        }
        String message = nullToEmpty(e.getMessage());
        return message.endsWith(" table not found") ||
                message.endsWith(" is offline and can not be queried") ||
                message.equals("This query is touching too much data!!!") ||
                message.equals("WITH_ONLY queries can not be executed") ||
                message.startsWith("Partition predicate not specified for any key");
    }

    private static boolean isPrestoQueryInvalid(SQLException e)
    {
        for (Throwable t = e.getCause(); t != null; t = t.getCause()) {
            if (t.toString().contains(".SemanticException:")) {
                return true;
            }
            if (t.toString().contains(".ParsingException:")) {
                return true;
            }
            if (nullToEmpty(t.getMessage()).matches("Function .* not registered")) {
                return true;
            }
        }
        return false;
    }

    private static List<List<Object>> convertJdbcResultSet(ResultSet resultSet)
            throws SQLException
    {
        int columnCount = resultSet.getMetaData().getColumnCount();
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                Object value = resultSet.getObject(i);
                if (value instanceof Integer) {
                    value = ((Integer) value).longValue();
                }
                row.add(value);
            }
            rows.add(unmodifiableList(row));
        }
        return rows.build();
    }

    private static Comparator<List<Object>> rowComparator()
    {
        final Comparator<Object> comparator = Ordering.from(columnComparator()).nullsFirst();
        return new Comparator<List<Object>>()
        {
            @Override
            public int compare(List<Object> a, List<Object> b)
            {
                checkArgument(a.size() == b.size(), "list sizes do not match");
                for (int i = 0; i < a.size(); i++) {
                    int r = comparator.compare(a.get(i), b.get(i));
                    if (r != 0) {
                        return r;
                    }
                }
                return 0;
            }
        };
    }

    private static Comparator<Object> columnComparator()
    {
        return new Comparator<Object>()
        {
            @SuppressWarnings("unchecked")
            @Override
            public int compare(Object a, Object b)
            {
                if (a.getClass() != b.getClass()) {
                    return -1;
                }
                if (!(a instanceof Comparable)) {
                    return -1;
                }
                return ((Comparable<Object>) a).compareTo(b);
            }
        };
    }
}
