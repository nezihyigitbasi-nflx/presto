package com.facebook.presto.argus;

import com.google.common.base.Throwables;
import com.google.common.collect.EnumMultiset;
import com.google.common.collect.Multiset;
import com.google.common.net.HostAndPort;

import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.argus.Validator.PeregrineState;
import static com.facebook.presto.argus.Validator.PrestoState;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ArgusConverter
{
    public static final String TEST_USER = "argus-test";
    public static final HostAndPort PRESTO_GATEWAY = HostAndPort.fromString("10.78.138.47:8081");

    private final PrintWriter logFile;

    public ArgusConverter(PrintWriter logFile)
    {
        this.logFile = logFile;
    }

    public void run(MigrationManager manager)
            throws InterruptedException
    {
        ExecutorService executor = newFixedThreadPool(10);
        CompletionService<Validator> completionService = new ExecutorCompletionService<>(executor);
        List<Report> reports = manager.getReports();

        for (Report report : reports) {
            Validator validator = new Validator(TEST_USER, PRESTO_GATEWAY, report);
            completionService.submit(validateTask(validator), validator);
        }
        executor.shutdown();

        int total = 0;
        int valid = 0;
        int migrated = 0;
        Multiset<PeregrineState> peregrineStates = EnumMultiset.create(PeregrineState.class);
        Multiset<PrestoState> prestoStates = EnumMultiset.create(PrestoState.class);

        while (total < reports.size()) {
            total++;

            Validator validator = takeUnchecked(completionService);
            Report report = validator.getReport();

            println("Report: " + report.getReportId());
            println("Namespace: " + report.getNamespace());

            if (validator.resultsMatch()) {
                valid++;
            }

            peregrineStates.add(validator.getPeregrineState());
            prestoStates.add(validator.getPrestoState());

            println("Peregrine State: " + validator.getPeregrineState());
            println("Presto State: " + validator.getPrestoState());
            println("Results Match: " + validator.resultsMatch());

            String comparison = validator.getResultsComparison().trim();
            if (!comparison.isEmpty()) {
                println(comparison);
            }

            if (validator.getPeregrineException() != null) {
                println("Peregrine Exception: " + validator.getPeregrineException());
            }
            if (validator.getPrestoException() != null) {
                println("Presto Exception: " + validator.getPrestoException());
                println("SQL:\n" + report.getCleanQuery());
            }
            else if ((validator.getPeregrineState() == PeregrineState.SUCCESS) && (!validator.resultsMatch())) {
                println("SQL:\n" + report.getCleanQuery());
            }

            try {
                if (manager.migrateReport(report, validator, validator.resultsMatch())) {
                    migrated++;
                    println("Migrated: true");
                }
                else {
                    println("Migrated: false");
                }
            }
            catch (RuntimeException e) {
                println("Migrated: false");
                println("Migration Exception: " + e);
                printStackTrace(e);
            }

            if ((validator.getPrestoException() != null) && (validator.getPrestoState() == PrestoState.FAILED)) {
                printStackTrace(validator.getPrestoException());
            }

            println("----------");

            if ((total % 5) == 0) {
                printfln("Progress: %s / %s / %s / %s", valid, migrated, total, reports.size());
                println("----------");
            }
        }

        println("Valid: " + valid);
        println("Total: " + total);
        for (Multiset.Entry<PeregrineState> entry : peregrineStates.entrySet()) {
            printfln("Peregrine: %s: %s", entry.getElement(), entry.getCount());
        }
        for (Multiset.Entry<PrestoState> entry : prestoStates.entrySet()) {
            printfln("Presto: %s: %s", entry.getElement(), entry.getCount());
        }
    }

    private void println(String s)
    {
        System.out.println(s);
        logFile.println(s);
    }

    private void printfln(String format, Object... args)
    {
        println(format(format, args));
    }

    private static void printStackTrace(Throwable t)
    {
        System.out.flush();
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

        t.printStackTrace();

        System.err.flush();
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    private static <T> T takeUnchecked(CompletionService<T> completionService)
            throws InterruptedException
    {
        try {
            return completionService.take().get();
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    private static Runnable validateTask(final Validator validator)
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                validator.valid();
            }
        };
    }
}
