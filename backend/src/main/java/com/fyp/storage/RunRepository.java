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
 * <p>Two reads, because two callers want different halves of a row: {@link #find} and
 * {@link #findAll} answer {@code GET /runs} with what happened, and {@link #findByRunId} rebuilds the
 * {@link RunContext} a job would have started from, config snapshot included.
 *
 * <p>{@code provenance_enabled} is written as true because a run records the operators it used and the
 * state each fired window left behind, which is what that flag has always meant.
 */
public final class RunRepository {

    /** Shared by both run reads so a row and {@link #toRun} cannot drift apart. */
    private static final String COLUMNS = "run_id, started_at, finished_at, dataset";

    private static final String INSERT_RUN =
            "INSERT INTO runs (run_id, started_at, dataset, config_json, provenance_enabled)"
                    + " VALUES (?, ?, ?, ?, ?)";

    private static final String MARK_FINISHED = "UPDATE runs SET finished_at = ? WHERE run_id = ?";

    private static final String FIND_RUN = "SELECT " + COLUMNS + " FROM runs WHERE run_id = ?";

    private static final String LIST_RUNS = "SELECT " + COLUMNS + " FROM runs"
            + " ORDER BY started_at DESC, run_id DESC";

    private static final String FIND_CONTEXT =
            "SELECT run_id, started_at, dataset, config_json FROM runs WHERE run_id = ?";

    private final SqlExecutor sql;

    public RunRepository(SqlExecutor sql) {
        this.sql = sql;
    }

    public void recordStart(RunContext run) {
        sql.update(INSERT_RUN, List.of(run.runId(), toTimestamp(run.startedAt()), run.dataset(),
                run.configJson(), true));
    }

    public void recordFinish(String runId, Instant finishedAt) {
        sql.update(MARK_FINISHED, List.of(toTimestamp(finishedAt), runId));
    }

    /**
     * The recorded row for one run, empty for an id that never ran.
     *
     * <p>{@code finishedAt} is null for a run whose job failed or is still going, which is the only
     * way a caller can tell the two apart from outside the process.
     */
    public Optional<Run> find(String runId) {
        return sql.query(FIND_RUN, List.of(runId)).stream().findFirst().map(RunRepository::toRun);
    }

    /** Every run this database holds, newest first. */
    public List<Run> findAll() {
        return sql.query(LIST_RUNS, List.of()).stream().map(RunRepository::toRun).toList();
    }

    /**
     * The run as its own job would have seen it, config snapshot and no finish time -- that is a
     * fact about the row, not about the run. {@link #find} is the read that reports one.
     */
    public Optional<RunContext> findByRunId(String runId) {
        List<Row> rows = sql.query(FIND_CONTEXT, List.of(runId));
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        Row row = rows.get(0);
        return Optional.of(new RunContext(row.getString(0), toInstant(row.getObject(1)), row.getString(2),
                row.getString(3)));
    }

    private static Run toRun(Row row) {
        Object finishedAt = row.getObject(2);
        return new Run(row.getString(0), toInstant(row.getObject(1)),
                finishedAt == null ? null : toInstant(finishedAt), row.getString(3));
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
