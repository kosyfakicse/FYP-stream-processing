package com.fyp.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fyp.model.Attribution;
import com.fyp.model.WindowOutput;

class WindowOutputRepositoryTest {

    private final RecordingExecutor sql = new RecordingExecutor();
    private final WindowOutputRepository repository = new WindowOutputRepository(sql);

    private static final WindowOutput PYTHON = new WindowOutput(1_000L, 2_000L, "PYTHON", 3L,
            new Attribution(Map.of("user_1", 1L, "user_2", 2L)));
    private static final WindowOutput JAVA = new WindowOutput(1_000L, 2_000L, "JAVA", 1L,
            new Attribution(Map.of("user_3", 1L)));

    @Test
    void writesEveryOutputInOneBatchWithRunIdAndVertexAttached() {
        repository.insertAll("run-20261114-1836-a7f3", List.of(PYTHON, JAVA));

        assertThat(sql.batches).hasSize(1);
        RecordingExecutor.Call batch = sql.lastBatch();
        assertThat(batch.sql())
                .isEqualTo("INSERT INTO window_outputs (run_id, vertex_id, category, window_start,"
                        + " window_end, output_count, attribution_json) VALUES (?, ?, ?, ?, ?, ?, ?)");

        List<?> rows = batch.args();
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0)).isEqualTo(List.of("run-20261114-1836-a7f3", "W1", "PYTHON", 1_000L,
                2_000L, 3, "{\"user_1\":1,\"user_2\":2}"));
        assertThat(rows.get(1)).isEqualTo(List.of("run-20261114-1836-a7f3", "W1", "JAVA", 1_000L,
                2_000L, 1, "{\"user_3\":1}"));
    }

    @Test
    void issuesNothingForAnEmptyBatch() {
        assertThat(repository.insertAll("run-1", List.of())).isZero();
        assertThat(sql.batches).isEmpty();
    }

    @Test
    void readsRowsBackAsWindowOutputs() {
        sql.queueRows(new Row(List.of(1_000L, 2_000L, "PYTHON", 3, "{\"user_1\":1,\"user_2\":2}")));

        List<WindowOutput> outputs = repository.findByRunId("run-20261114-1836-a7f3");

        assertThat(outputs).containsExactly(PYTHON);
        assertThat(sql.queries.get(0).args()).isEqualTo(List.of("run-20261114-1836-a7f3"));
        assertThat(sql.queries.get(0).sql()).contains("ORDER BY window_start, category");
    }

    @Test
    void countsTheRowsOfOneRunOnly() {
        sql.queueRows(new Row(List.of(7L)));

        assertThat(repository.countByRunId("run-42")).isEqualTo(7L);
        assertThat(sql.queries.get(0).sql()).isEqualTo("SELECT COUNT(*) FROM window_outputs WHERE run_id = ?");
        assertThat(sql.queries.get(0).args()).isEqualTo(List.of("run-42"));
    }
}
