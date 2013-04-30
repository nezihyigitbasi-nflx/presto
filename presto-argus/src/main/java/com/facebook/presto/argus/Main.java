package com.facebook.presto.argus;

import com.facebook.presto.argus.peregrine.PeregrineClient;
import com.facebook.presto.argus.peregrine.QueryId;
import com.facebook.presto.argus.peregrine.QueryResponse;
import com.facebook.presto.argus.peregrine.UnparsedQuery;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.StatementSplitter;
import com.facebook.swift.prism.PrismNamespace;
import com.facebook.swift.service.ThriftClientConfig;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.facebook.presto.argus.ArgusReports.Report;
import static com.facebook.presto.argus.PeregrineUtil.peregrineResults;
import static com.facebook.presto.sql.parser.SqlParser.createStatement;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class Main
{
//    private static final QueryExecutor executor = QueryExecutor.create("argus-test");
//    URI uri = URI.create("http://10.78.138.47:8081");
//    ClientSession session = new ClientSession(uri, "argus-test", "prism", report.getNamespace(), false);
//    StatementClient client = executor.startQuery(session, sql);

    private Main() {}

    public static String lexQuery(String sql)
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

    public static boolean canParseQuery(Report report)
    {
        String sql = report.getCleanQuery();
        try {
            createStatement(lexQuery(sql));
            return true;
        }
        catch (ParsingException | IllegalArgumentException e) {
            if (Boolean.parseBoolean(System.getProperty("printParse"))) {
                System.out.println("Report: " + report.getReportId());
                System.out.println(e);
                System.out.println(sql);
                System.out.println("----------");
            }
            return false;
        }
    }

    private static boolean canExecute(Report report)
    {
        System.out.println("Report: " + report.getReportId());

        String url = format("jdbc:presto://%s/", "10.78.138.47:8081");
        String sql = format("SELECT * FROM (%n%s%n) LIMIT 0", lexQuery(report.getCleanQuery()));

        try (Connection connection = DriverManager.getConnection(url, "argus-test", null)) {
            connection.setCatalog("prism");
            connection.setSchema(report.getNamespace());
            long start = System.nanoTime();
            try (Statement statement = connection.createStatement();
                    ResultSet ignored = statement.executeQuery(sql)) {
                System.out.println("SUCCESS: " + nanosSince(start).convertTo(SECONDS) + "s");
                return true;
            }
        }
        catch (SQLException e) {
            System.out.println(e);
            System.out.println("Namespace: " + report.getNamespace());
            System.out.println(sql);
            return false;
        }
        finally {
            System.out.println("----------");
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        LoggingUtil.initializeLogging(false);

        ThriftClientConfig config = new ThriftClientConfig()
                .setSocksProxy(HostAndPort.fromString("localhost:1080"))
                .setReadTimeout(new Duration(10, SECONDS))
                .setWriteTimeout(new Duration(10, SECONDS));
        try (PeregrineClientFactory clientFactory = new PeregrineClientFactory(config)) {
            PrismNamespace prismNamespace = clientFactory.lookupNamespace("default_platinum");
            try (PeregrineClient client = clientFactory.create(prismNamespace.getPeregrineGateway())) {
                String sql = "" +
                        "with true as mode.exact " +
                        "select ds, file_format, dummy, " +
                        "t_string, t_tinyint, t_smallint, t_int, t_bigint, t_float, t_double " +
                        "from presto_test where ds is not null";

                QueryId queryId = client.submitQuery(new UnparsedQuery(
                        "argus-test", sql, ImmutableList.<String>of(), prismNamespace.getHiveDatabaseName()));

                QueryResponse response;
                do {
                    response = client.getResponse(queryId);
                    System.out.println(response.getStatus());
                }
                while (!response.getStatus().getState().isDone());

                for (List<Object> row : peregrineResults(response.getResult())) {
                    System.out.println(row);
                }
            }
        }

        if (true) {
            return;
        }

        List<Report> reports = ArgusReports.loadReports();

        reports = FluentIterable.from(reports)
                .filter(new Predicate<Report>()
                {
                    @Override
                    public boolean apply(Report report)
                    {
                        return report.getViews() >= 10;
                    }
                })
                .toList();

        int total = 0;
        int parseable = 0;
        int runnable = 0;
        for (Report report : reports) {
            total++;

            if (!canParseQuery(report)) {
                continue;
            }
            parseable++;

            if (canExecute(report)) {
                runnable++;
            }

            if ((total % 10) == 0) {
                System.out.printf("Progress: %s / %s / %s / %s%n", runnable, parseable, total, reports.size());
                System.out.println("----------");
            }
        }

        System.out.println(" runnable: " + runnable);
        System.out.println("parseable: " + parseable);
        System.out.println("    total: " + total);
    }
}
