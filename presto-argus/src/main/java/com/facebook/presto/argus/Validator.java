package com.facebook.presto.argus;

import com.facebook.presto.argus.peregrine.PeregrineErrorCode;
import com.facebook.presto.argus.peregrine.PeregrineException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.StatementSplitter;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMultiset;
import com.google.common.collect.Multiset;
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

import static com.facebook.presto.sql.parser.SqlParser.createStatement;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
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
                canPrestoParse() &&
                canPrestoExecute() &&
                compareResults();
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

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("username", username)
                .add("prestoGateway", prestoGateway)
                .add("report", report)
                .add("peregrineState", peregrineState)
                .add("prestoState", prestoState)
                .add("resultsMatch", resultsMatch)
                .add("peregrineException", peregrineException)
                .add("prestoException", prestoException)
                .add("peregrineTime", peregrineTime)
                .add("prestoTime", prestoTime)
                .add("peregrineResults", peregrineResults)
                .add("prestoResults", prestoResults)
                .omitNullValues()
                .toString();
    }

    private boolean compareResults()
    {
        Multiset<List<Object>> peregrine = ImmutableSortedMultiset.copyOf(rowComparator(), getPeregrineResults());
        Multiset<List<Object>> presto = ImmutableSortedMultiset.copyOf(rowComparator(), getPrestoResults());
        resultsMatch = peregrine.equals(presto);
        if (!resultsMatch) {
            System.out.printf("Peregrine %s rows, Presto %s rows%n", peregrine.size(), presto.size());
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
                        System.out.printf("%s: %s: %s %s%n", i,
                                peregrineRow.equals(prestoRow),
                                peregrineRow, prestoRow);
                        n++;
                        if (n >= 100) {
                            break;
                        }
                    }
                }
            }
        }
        return resultsMatch;
    }

    private boolean canPeregrineExecute()
    {
        try (PeregrineRunner runner = new PeregrineRunner(TIME_LIMIT)) {
            long start = System.nanoTime();
            peregrineResults = runner.execute(username, report.getNamespace(), report.getCleanQuery());
            peregrineState = PeregrineState.SUCCESS;
            peregrineTime = nanosSince(start);
            return true;
        }
        catch (PeregrineException e) {
            peregrineException = e;
            if (e.getCode().isInvalidQuery()) {
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

    private boolean canPrestoParse()
    {
        String sql = report.getCleanQuery();
        try {
            createStatement(lexQuery(sql));
            return true;
        }
        catch (ParsingException | IllegalArgumentException e) {
            prestoException = e;
            prestoState = PrestoState.INVALID;
            return false;
        }
    }

    private boolean canPrestoExecute()
    {
        String url = format("jdbc:presto://%s/", prestoGateway);
        String sql = lexQuery(report.getCleanQuery());

        try (Connection connection = DriverManager.getConnection(url, username, null)) {
            connection.setCatalog("prism");
            connection.setSchema(report.getNamespace());
            long start = System.nanoTime();

            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql)) {
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

    private static boolean isPrestoQueryInvalid(SQLException e)
    {
        for (Throwable t = e.getCause(); t != null; t = t.getCause()) {
            if (t.toString().contains(".SemanticException:")) {
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

    private static String lexQuery(String sql)
    {
        StatementSplitter splitter = new StatementSplitter(sql);
        if (splitter.getCompleteStatements().size() > 1) {
            throw new IllegalArgumentException("multiple statements", null);
        }
        if (splitter.getCompleteStatements().size() == 1) {
            return splitter.getCompleteStatements().get(0);
        }
        return splitter.getPartialStatement();
    }

    private static Comparator<List<Object>> rowComparator()
    {
        return new Comparator<List<Object>>()
        {
            @SuppressWarnings({"unchecked", "rawtypes"})
            @Override
            public int compare(List<Object> a, List<Object> b)
            {
                checkArgument(a.size() == b.size(), "list sizes do not match");
                for (int i = 0; i < a.size(); i++) {
                    Object x = a.get(i);
                    Object y = b.get(i);
                    if (x == null) {
                        return (y == null) ? 0 : -1;
                    }
                    if (y == null) {
                        return 1;
                    }
                    checkArgument(x.getClass() == y.getClass(),
                            "item types do not match: %s vs %s",
                            x.getClass().getName(), y.getClass().getName());
                    checkArgument(x instanceof Comparable, "item is not Comparable: %s", x.getClass().getName());
                    int r = ((Comparable) x).compareTo(y);
                    if (r != 0) {
                        return r;
                    }
                }
                return 0;
            }
        };
    }
}
