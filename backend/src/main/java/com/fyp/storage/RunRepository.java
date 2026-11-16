package com.fyp.storage;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import com.fyp.RunContext;

/**
 * Writes and reads the {@code runs} table, the record that makes a run id mean something.
 *
 * <p>{@code provenance_enabled} is written as false because this stage persists results only; the
 * TIN capture that would flip it is a later phase.
 */
public final class RunRepository {

    private static final String INSERT_RUN =
            "INSERT INTO runs (run_id, started_at, dataset, config_json, provenance_enabled)"
                    + " VALUES (?, ?, ?, ?, ?)";

    private static final String MARK_FINISHED = "UPDATE runs SET finished_at = ? WHERE run_id = ?";

    private static final String FIND_RUN =
            "SELECT run_id, started_at, dataset, config_json FROM runs WHERE run_id = ?";

    private final SqlExecutor sql;

    public RunRepository(SqlExecutor sql) {
        this.sql = sql;
    }

    public void recordStart(RunContext run) {
        sql.update(INSERT_RUN, List.of(run.runId(), toTimestamp(run.startedAt()), run.dataset(),
                run.configJson(), false));
    }

    public void recordFinish(String runId, Instant finishedAt) {
        sql.update(MARK_FINISHED, List.of(toTimestamp(finishedAt), runId));
    }

    /** {@code finished_at} is not read back: it is the caller's own clock, not part of a result. */
    public Optional<RunContext> findByRunId(String runId) {
        List<Row> rows = sql.query(FIND_RUN, List.of(runId));
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        Row row = rows.get(0);
        return Optional.of(new RunContext(row.getString(0), toInstant(row.getObject(1)), row.getString(2),
                row.getString(3)));
    }

    /** DuckDB TIMESTAMP carries no zone, so every value is stored as naive UTC. */
    static Timestamp toTimestamp(Instant instant) {
        return Timestamp.valueOf(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
    }

    static Instant toInstant(Object value) {
        if (value instanceof Timestamp timestamp) {
            return timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC);
        }
        throw new IllegalStateException("Expected a timestamp from the runs table but got: " + value);
    }
}
