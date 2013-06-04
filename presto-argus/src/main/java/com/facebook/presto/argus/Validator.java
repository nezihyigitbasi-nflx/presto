package com.facebook.presto.argus;

import com.facebook.presto.argus.peregrine.PeregrineErrorCode;
import com.facebook.presto.argus.peregrine.PeregrineException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMultiset;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Multiset;
import com.google.common.math.DoubleMath;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.units.Duration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.primitives.Doubles.isFinite;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;

public class Validator
{
    private static final int MAX_ATTEMPTS = 5;

    public enum PeregrineState
    {
        UNKNOWN, TIMEOUT, INVALID, MEMORY, FAILED, SUCCESS
    }

    public enum PrestoState
    {
        UNKNOWN, TIMEOUT, INVALID, FAILED, SUCCESS
    }

    private final PeregrineRunner peregrineRunner;
    private final String username;
    private final HostAndPort prestoGateway;
    private final Report report;

    private PeregrineState peregrineState = PeregrineState.UNKNOWN;
    private PrestoState prestoState = PrestoState.UNKNOWN;
    private boolean resultsMatch;

    private String runnablePeregrineQuery;
    private String translatedPrestoQuery;
    private String runnablePrestoQuery;

    private List<PeregrineState> peregrineAttemptList;

    private Exception peregrineException;
    private Exception prestoException;

    private Duration peregrineTime;
    private Duration prestoTime;

    private List<String> peregrineColumns;
    private List<String> prestoColumns;

    private List<List<Object>> peregrineResults;
    private List<List<Object>> prestoResults;

    public Validator(PeregrineRunner peregrineRunner, String username, HostAndPort prestoGateway, Report report)
    {
        this.peregrineRunner = checkNotNull(peregrineRunner, "peregrineRunner is null");
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

    public List<PeregrineState> getPeregrineAttempts()
    {
        return ImmutableList.copyOf(peregrineAttemptList);
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

    public List<String> getPeregrineColumns()
    {
        checkState(peregrineColumns != null);
        return peregrineColumns;
    }

    public List<String> getPrestoColumns()
    {
        checkState(prestoColumns != null);
        return prestoColumns;
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

    public void forceQueryTranslation()
    {
        translatedPrestoQuery = QueryTranslator.translateQuery(report.getQuery());
    }

    public String getResultsComparison()
    {
        if (resultsMatch || (peregrineResults == null) || (prestoResults == null)) {
            return "";
        }

        Set<String> peregrineColumnSet = ImmutableSortedSet.copyOf(peregrineColumns);
        Set<String> prestoColumnSet = ImmutableSortedSet.copyOf(prestoColumns);
        if (!peregrineColumnSet.equals(prestoColumnSet) || (peregrineColumnSet.size() != peregrineColumns.size())) {
            return "PeregrineColumns: " + peregrineColumnSet + "\n" +
                    "PrestoColumns: " + prestoColumnSet + "\n";
        }

        Multiset<List<Object>> peregrine = ImmutableSortedMultiset.copyOf(rowComparator(), getPeregrineResults());
        Multiset<List<Object>> presto = ImmutableSortedMultiset.copyOf(rowComparator(), getPrestoResults());
        StringBuilder sb = new StringBuilder();

        sb.append(format("Peregrine %s rows, Presto %s rows%n", peregrine.size(), presto.size()));
        if (peregrine.size() == presto.size()) {
            sb.append(format("Columns: %s%n", peregrineColumnSet));
            Iterator<List<Object>> peregrineIter = peregrine.iterator();
            Iterator<List<Object>> prestoIter = presto.iterator();
            int i = 0;
            int n = 0;
            while (i < peregrine.size()) {
                i++;
                List<Object> peregrineRow = peregrineIter.next();
                List<Object> prestoRow = prestoIter.next();
                if (rowComparator().compare(peregrineRow, prestoRow) != 0) {
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
        if (peregrineColumns.size() != prestoColumns.size()) {
            return false;
        }

        Set<String> peregrineColumnSet = ImmutableSortedSet.copyOf(peregrineColumns);
        Set<String> prestoColumnSet = ImmutableSortedSet.copyOf(prestoColumns);
        if (peregrineColumnSet.equals(prestoColumnSet) && (peregrineColumnSet.size() == peregrineColumns.size())) {
            peregrineResults = normalizeColumnOrder(peregrineResults, peregrineColumns, peregrineColumnSet);
            prestoResults = normalizeColumnOrder(prestoResults, prestoColumns, prestoColumnSet);
        }

        Multiset<List<Object>> peregrine = ImmutableSortedMultiset.copyOf(rowComparator(), getPeregrineResults());
        Multiset<List<Object>> presto = ImmutableSortedMultiset.copyOf(rowComparator(), getPrestoResults());
        resultsMatch = peregrine.equals(presto);
        return resultsMatch;
    }

    private boolean canPeregrineExecute()
    {
        peregrineAttemptList = new ArrayList<>();
        for (int i = 0; i < MAX_ATTEMPTS; i++) {
            boolean success = peregrineExecute();
            peregrineAttemptList.add(peregrineState);
            if (success) {
                return true;
            }
            if ((peregrineState == PeregrineState.INVALID) || (peregrineState == PeregrineState.MEMORY)) {
                return false;
            }
        }
        return false;
    }

    private boolean peregrineExecute()
    {
        CleanedQuery cleaned = new CleanedQuery(report.getNamespace(), report.getQuery(), report.getVariables());
        if (!cleaned.isValid()) {
            peregrineState = PeregrineState.INVALID;
            peregrineException = cleaned.getException();
            return false;
        }
        runnablePeregrineQuery = cleaned.getQuery();

        peregrineException = null;
        try {
            long start = System.nanoTime();
            PeregrineRunner.Results results = peregrineRunner.execute(username, report.getNamespace(), runnablePeregrineQuery);
            peregrineColumns = results.getColumns();
            peregrineResults = results.getRows();
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
            else if (e.getCode() == PeregrineErrorCode.UNKNOWN) {
                peregrineState = PeregrineState.UNKNOWN;
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
            connection.setClientInfo("ApplicationName", "argus-test");
            connection.setCatalog("prism");
            connection.setSchema(report.getNamespace());
            long start = System.nanoTime();

            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(runnablePrestoQuery)) {
                prestoColumns = getJdbcColumnLabels(resultSet);
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
                message.startsWith("wrong column count ") ||
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
            if (t.toString().contains(" is offline: Offlined by Prism (moved to ")) {
                return true;
            }
        }
        return false;
    }

    private static List<String> getJdbcColumnLabels(ResultSet resultSet)
            throws SQLException
    {
        ImmutableList.Builder<String> columns = ImmutableList.builder();
        ResultSetMetaData metadata = resultSet.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            columns.add(metadata.getColumnLabel(i));
        }
        return columns.build();
    }

    private static List<List<Object>> convertJdbcResultSet(ResultSet resultSet)
            throws SQLException
    {
        int columnCount = resultSet.getMetaData().getColumnCount();
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(resultSet.getObject(i));
            }
            rows.add(unmodifiableList(row));
        }
        return rows.build();
    }

    private static Comparator<List<Object>> rowComparator()
    {
        final Comparator<Object> comparator = columnComparator();
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
                if ((a == null) || (b == null)) {
                    return compareNull(a, b);
                }
                if (a.getClass() != b.getClass()) {
                    if ((a instanceof Number) && (b instanceof Number)) {
                        return compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
                    }
                    if ((a instanceof String) || (b instanceof String)) {
                        return a.toString().compareTo(b.toString());
                    }
                    throw new IllegalArgumentException(format("%s != %s", a.getClass().getName(), b.getClass().getName()));
                }
                if (!(a instanceof Comparable)) {
                    throw new IllegalArgumentException(a.getClass().getName());
                }
                if (a instanceof Double) {
                    return fuzzyCompare((double) a, (double) b);
                }
                if (a instanceof Long) {
                    return a.toString().compareTo(b.toString());
                }
                return ((Comparable<Object>) a).compareTo(b);
            }
        };
    }

    private static int fuzzyCompare(double a, double b)
    {
        return DoubleMath.fuzzyCompare(normalizeDouble(a), normalizeDouble(b), 0.0001);
    }

    private static int compareNull(Object a, Object b)
    {
        if ((a == null) && (b == null)) {
            return 0;
        }
        if (a == null) {
            return equalToNull(b) ? 0 : -1;
        }
        return equalToNull(a) ? 0 : 1;
    }

    private static boolean equalToNull(Object o)
    {
        if (o instanceof Number) {
            return fuzzyCompare(normalizeDouble(((Number) o).doubleValue()), 0.0) == 0;
        }
        if (o instanceof String) {
            return ((String) o).isEmpty();
        }
        return false;
    }

    private static double normalizeDouble(double d)
    {
        return isFinite(d) ? d : 0.0;
    }

    private static List<List<Object>> normalizeColumnOrder(List<List<Object>> rows, List<String> columns, Set<String> columnSet)
    {
        List<Integer> columnIndex = buildColumnIndex(columns, columnSet);
        ImmutableList.Builder<List<Object>> out = ImmutableList.builder();
        for (List<Object> row : rows) {
            checkArgument(row.size() == columnIndex.size());
            List<Object> newRow = new ArrayList<>();
            for (int index : columnIndex) {
                newRow.add(row.get(index));
            }
            out.add(unmodifiableList(newRow));
        }
        return out.build();
    }

    private static List<Integer> buildColumnIndex(List<String> columns, Set<String> columnSet)
    {
        checkArgument(columns.size() == columnSet.size());
        ImmutableList.Builder<Integer> index = ImmutableList.builder();
        for (String column : columnSet) {
            index.add(columns.indexOf(column));
        }
        return index.build();
    }
}
