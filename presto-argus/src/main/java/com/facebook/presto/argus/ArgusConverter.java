package com.facebook.presto.argus;

import com.google.common.base.Predicate;
import com.google.common.collect.EnumMultiset;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Multiset;
import com.google.common.net.HostAndPort;

import java.io.PrintWriter;
import java.util.List;

import static com.facebook.presto.argus.Validator.PeregrineState;
import static com.facebook.presto.argus.Validator.PrestoState;
import static java.lang.String.format;

public class ArgusConverter
{
    public static final String TEST_USER = "argus-test";
    public static final HostAndPort PRESTO_GATEWAY = HostAndPort.fromString("10.78.138.47:8081");

    private final PrintWriter logFile;

    public ArgusConverter(PrintWriter logFile)
    {
        this.logFile = logFile;
    }

    public void run(List<Report> reports)
    {
        reports = FluentIterable.from(reports).filter(reportMinViews(10)).toList();

        int total = 0;
        int valid = 0;
        Multiset<PeregrineState> peregrineStates = EnumMultiset.create(PeregrineState.class);
        Multiset<PrestoState> prestoStates = EnumMultiset.create(PrestoState.class);

        for (Report report : reports) {
            total++;
            println("Report: " + report.getReportId());
            println("Namespace: " + report.getNamespace());

            Validator validator = new Validator(TEST_USER, PRESTO_GATEWAY, report);

            if (validator.valid()) {
                valid++;
            }

            peregrineStates.add(validator.getPeregrineState());
            prestoStates.add(validator.getPrestoState());

            println("Peregrine State: " + validator.getPeregrineState());
            println("Presto State: " + validator.getPrestoState());
            println("Results Match: " + validator.resultsMatch());

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

            println("----------");

            if ((total % 10) == 0) {
                printfln("Progress: %s / %s / %s", valid, total, reports.size());
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

    private static Predicate<Report> reportMinViews(final int minViews)
    {
        return new Predicate<Report>()
        {
            @Override
            public boolean apply(Report report)
            {
                return report.getViews() >= minViews;
            }
        };
    }
}
