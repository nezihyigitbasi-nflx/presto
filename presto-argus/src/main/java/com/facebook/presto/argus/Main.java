package com.facebook.presto.argus;

import java.io.FileOutputStream;
import java.io.PrintWriter;

public final class Main
{
    public static final String LOG_FILE = "/Users/dphillips/tmp/argus-report.txt";

    private Main() {}

    public static void main(String[] args)
            throws Exception
    {
        LoggingUtil.initializeLogging(false);

        try (PrintWriter logFile = new PrintWriter(new FileOutputStream(LOG_FILE))) {
            new ArgusConverter(logFile).run(ArgusReports.loadReports());
        }
    }
}
