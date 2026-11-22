package com.fyp.storage;

import java.util.List;

import com.fyp.model.Attribution;
import com.fyp.model.WindowOutput;

/**
 * Writes window results and reads them back for one run.
 *
 * <p>The batch path exists because the Python side buffered window results and flushed every 100
 * records ({@code DatabaseBatchWriter}); the sink does the same and calls in here for the SQL.
 */
public final class WindowOutputRepository {

    private static final String INSERT_OUTPUT =
            "INSERT INTO window_outputs (run_id, vertex_id, category, window_start, window_end,"
                    + " output_count, attribution_json) VALUES (?, ?, ?, ?, ?, ?, ?)";

    private static final String FIND_BY_RUN =
            "SELECT window_start, window_end, category, output_count, attribution_json"
                    + " FROM window_outputs WHERE run_id = ? ORDER BY window_start, category";

    private static final String COUNT_BY_RUN = "SELECT COUNT(*) FROM window_outputs WHERE run_id = ?";

    private final SqlExecutor sql;

    public WindowOutputRepository(SqlExecutor sql) {
        this.sql = sql;
    }

    /**
     * @param vertexId the window operator that fired these, which is a {@code tin_vertices} id of the
     *        same run -- the sink is handed it rather than naming one, so the two cannot disagree
     */
    public int insertAll(String runId, String vertexId, List<WindowOutput> outputs) {
        if (outputs.isEmpty()) {
            return 0;
        }
        return sql.insertBatch(INSERT_OUTPUT,
                outputs.stream().map(output -> params(runId, vertexId, output)).toList());
    }

    public List<WindowOutput> findByRunId(String runId) {
        return sql.query(FIND_BY_RUN, List.of(runId)).stream().map(row -> new WindowOutput(
                row.getLong(0), row.getLong(1), row.getString(2), row.getLong(3),
                Attribution.fromJson(row.getString(4)))).toList();
    }

    public long countByRunId(String runId) {
        return sql.query(COUNT_BY_RUN, List.of(runId)).get(0).getLong(0);
    }

    /** Column order of {@link #INSERT_OUTPUT}, kept in one place so writes and tests agree. */
    static List<Object> params(String runId, String vertexId, WindowOutput output) {
        return List.of(runId, vertexId, output.category(), output.windowStart(),
                output.windowEnd(), Math.toIntExact(output.outputCount()),
                output.attribution().toJson());
    }
}
