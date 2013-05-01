package com.facebook.presto.argus;

import com.google.common.base.Predicate;
import com.google.common.collect.EnumMultiset;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Multiset;
import com.google.common.net.HostAndPort;

import java.util.List;

import static com.facebook.presto.argus.ArgusReports.Report;
import static com.facebook.presto.argus.Validator.PeregrineState;
import static com.facebook.presto.argus.Validator.PrestoState;

public final class Main
{
    public static final String TEST_USER = "argus-test";
    public static final HostAndPort PRESTO_GATEWAY = HostAndPort.fromString("10.78.138.47:8081");

    private Main() {}

    public static void main(String[] args)
            throws Exception
    {
        LoggingUtil.initializeLogging(false);

        List<Report> reports = ArgusReports.loadReports();

        reports = FluentIterable.from(reports).filter(reportMinViews(10)).toList();

        int total = 0;
        int valid = 0;
        Multiset<PeregrineState> peregrineStates = EnumMultiset.create(PeregrineState.class);
        Multiset<PrestoState> prestoStates = EnumMultiset.create(PrestoState.class);

        for (Report report : reports) {
            total++;
            System.out.println("Report: " + report.getReportId());
            System.out.println("Namespace: " + report.getNamespace());

            Validator validator = new Validator(TEST_USER, PRESTO_GATEWAY, report);

            if (validator.valid()) {
                valid++;
            }

            peregrineStates.add(validator.getPeregrineState());
            prestoStates.add(validator.getPrestoState());

            System.out.println("Peregrine State: " + validator.getPeregrineState());
            System.out.println("Presto State: " + validator.getPrestoState());
            System.out.println("Results Match: " + validator.resultsMatch());

            if (validator.getPeregrineException() != null) {
                System.out.println("Peregrine Exception: " + validator.getPeregrineException());
            }
            if (validator.getPrestoException() != null) {
                System.out.println("Presto Exception: " + validator.getPrestoException());
                System.out.println("SQL:\n" + report.getCleanQuery());
            }

            System.out.println("----------");

            if ((total % 10) == 0) {
                System.out.printf("Progress: %s / %s / %s%n", valid, total, reports.size());
                System.out.println("----------");
            }
            if (total >= 5) {
                break;
            }
        }

        System.out.println("Valid: " + valid);
        System.out.println("Total: " + total);
        for (Multiset.Entry<PeregrineState> entry : peregrineStates.entrySet()) {
            System.out.printf("Peregrine: %s: %s%n", entry.getElement(), entry.getCount());
        }
        for (Multiset.Entry<PrestoState> entry : prestoStates.entrySet()) {
            System.out.printf("Presto: %s: %s%n", entry.getElement(), entry.getCount());
        }
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
