package com.facebook.presto.argus;

import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

public final class Main
{
    public static final String LOG_FILE = format("%s/tmp/argus/argus-report.%s.txt", System.getProperty("user.home"), currentTimeMillis());
    public static final Duration TIME_LIMIT = new Duration(2, TimeUnit.MINUTES);

    private Main() {}

    public static void main(String[] args)
            throws Exception
    {
        LoggingUtil.initializeLogging(false);

        System.out.println("Log File: " + LOG_FILE);
        try (PrintWriter logFile = new PrintWriter(new FileOutputStream(LOG_FILE));
                PeregrineRunner peregrineRunner = new PeregrineRunner(TIME_LIMIT)) {
            IDBI dbi = new DBI(System.getProperty("argusDatabase"));
            new ArgusConverter(logFile).run(new MigrationManager(dbi), peregrineRunner);
        }
    }
}
