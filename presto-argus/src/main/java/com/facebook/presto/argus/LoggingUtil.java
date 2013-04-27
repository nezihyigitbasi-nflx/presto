package com.facebook.presto.argus;

import com.google.common.base.Throwables;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;

import java.io.IOException;
import java.io.PrintStream;

import static com.google.common.io.ByteStreams.nullOutputStream;

public final class LoggingUtil
{
    private LoggingUtil() {}

    public static void initializeLogging(boolean debug)
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;
        try {
            if (debug) {
                Logging logging = Logging.initialize();
                logging.configure(new LoggingConfiguration());
                logging.setLevel("com.facebook.presto", Logging.Level.DEBUG);
            }
            else {
                System.setOut(nullPrintStream());
                System.setErr(nullPrintStream());

                Logging logging = Logging.initialize();
                logging.configure(new LoggingConfiguration());
                logging.disableConsole();
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }
    }

    private static PrintStream nullPrintStream()
    {
        return new PrintStream(nullOutputStream());
    }
}
