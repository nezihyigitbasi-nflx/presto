package com.facebook.presto.argus;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import java.io.FileOutputStream;
import java.io.PrintWriter;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

public final class Main
{
    public static final String LOG_FILE = format("/Users/dphillips/tmp/argus-report.%s.txt", currentTimeMillis());

    private Main() {}

    public static void main(String[] args)
            throws Exception
    {
        LoggingUtil.initializeLogging(false);

        try (PrintWriter logFile = new PrintWriter(new FileOutputStream(LOG_FILE))) {
            IDBI dbi = new DBI(System.getProperty("argusDatabase"));
            new ArgusConverter(logFile).run(new MigrationManager(dbi));
        }
    }
}
