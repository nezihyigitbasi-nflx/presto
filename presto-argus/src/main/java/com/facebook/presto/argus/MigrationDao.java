package com.facebook.presto.argus;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.List;

@SuppressWarnings("AbstractClassNeverImplemented")
public abstract class MigrationDao
{
    @SqlQuery("" +
            "SELECT report_id, namespace, sql_query, settings, COALESCE(views, 0) views\n" +
            "FROM reports\n" +
            "LEFT JOIN (\n" +
            "  SELECT report_id, sum(views) views\n" +
            "  FROM report_views\n" +
            "  WHERE datetime >= unix_timestamp(now() - INTERVAL 180 DAY)" +
            "  GROUP BY report_id\n" +
            ") v USING (report_id)\n" +
            "WHERE connection_id = 1075\n" +
            "  AND report_id NOT IN (SELECT report_id FROM presto_migrations WHERE migrated = 1)\n" +
            "ORDER BY views DESC")
    @Mapper(ReportMapper.class)
    public abstract List<Report> getReports();

    @SqlUpdate("" +
            "UPDATE reports SET\n" +
            "  connection_id = 96\n" +
            ", sql_query = :updatedSql\n" +
            "WHERE report_id = :reportId\n" +
            "  AND sql_query = :originalSql")
    protected abstract int updateReport(
            @Bind("reportId") long reportId,
            @Bind("originalSql") String originalSql,
            @Bind("updatedSql") String updatedSql);

    @SqlUpdate("" +
            "REPLACE INTO presto_migrations SET\n" +
            "  report_id = :reportId,\n" +
            "  created_dt = CURRENT_TIMESTAMP,\n" +
            "  original_sql = :originalSql,\n" +
            "  migrated = :migrated,\n" +
            "  results_match = :resultsMatch,\n" +
            "  peregrine_state = :peregrineState,\n" +
            "  presto_state = :prestoState,\n" +
            "  peregrine_time_ms = :peregrineTimeMs,\n" +
            "  presto_time_ms = :prestoTimeMs,\n" +
            "  peregrine_exception = :peregrineException, \n" +
            "  presto_exception = :prestoException")
    protected abstract void insertMigration(
            @Bind("reportId") long reportId,
            @Bind("originalSql") String originalSql,
            @Bind("migrated") boolean migrated,
            @Bind("resultsMatch") boolean resultsMatch,
            @Bind("peregrineState") String peregrineState,
            @Bind("prestoState") String prestoState,
            @Bind("peregrineTimeMs") Long peregrineTimeMs,
            @Bind("prestoTimeMs") Long prestoTimeMs,
            @Bind("peregrineException") String peregrineException,
            @Bind("prestoException") String prestoException);

    @Transaction
    public boolean migrateReport(
            boolean migrate,
            long reportId,
            String originalSql,
            String updatedSql,
            boolean resultsMatch,
            String peregrineState,
            String prestoState,
            Long peregrineTimeMs,
            Long prestoTimeMs,
            String peregrineException,
            String prestoException)
    {
        boolean migrated = false;
        if (migrate) {
            int updated = updateReport(reportId, originalSql, updatedSql);
            if (updated > 1) {
                throw new RuntimeException("too many rows updated: " + updated);
            }
            migrated = (updated == 1);
        }
        insertMigration(
                reportId,
                originalSql,
                migrated,
                resultsMatch,
                peregrineState,
                prestoState,
                peregrineTimeMs,
                prestoTimeMs,
                peregrineException,
                prestoException);
        return migrated;
    }
}
