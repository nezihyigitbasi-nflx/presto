package com.facebook.presto.argus;

import io.airlift.units.Duration;
import org.skife.jdbi.v2.IDBI;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class MigrationManager
{
    private final MigrationDao dao;

    public MigrationManager(IDBI dbi)
    {
        checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(MigrationDao.class);
    }

    public List<Report> getReports()
    {
        return dao.getReports();
    }

    public boolean migrateReport(Report report, Validator validator, boolean migrate)
    {
        return dao.migrateReport(
                migrate,
                report.getReportId(),
                report.getOriginalQuery(),
                validator.getTranslatedPrestoQuery(),
                validator.resultsMatch(),
                toString(validator.getPeregrineState()),
                toString(validator.getPrestoState()),
                toMillis(validator.getPeregrineTime()),
                toMillis(validator.getPrestoTime()),
                toString(validator.getPeregrineException()),
                toString(validator.getPrestoException()));
    }

    private static Long toMillis(Duration duration)
    {
        return (duration == null) ? null : (long) duration.toMillis();
    }

    private static String toString(Object object)
    {
        return (object == null) ? null : object.toString();
    }
}
