package com.fyp.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fyp.RunContext;
import com.fyp.model.Attribution;
import com.fyp.model.WindowOutput;

/**
 * The only storage test that touches a real database file. It is what proves the output_id sequence
 * in {@code schema.sql} fills itself and that a second run can coexist with the first.
 */
class StorageRoundTripTest {

    private static final Instant STARTED = Instant.parse("2026-11-14T10:20:30Z");
    private static final WindowOutput PYTHON = new WindowOutput(1_000L, 2_000L, "PYTHON", 3L,
            new Attribution(Map.of("user_1", 1L, "user_2", 2L)));
    private static final WindowOutput JAVA = new WindowOutput(1_000L, 2_000L, "JAVA", 1L,
            new Attribution(Map.of("user_3", 1L)));

    @TempDir
    Path tempDir;

    @Test
    void writesTwoRunsToOneFileAndReadsEachBackSeparately() {
        Path dbFile = tempDir.resolve("attribution.duckdb");

        try (DuckDbExecutor executor = new DuckDbExecutor(dbFile)) {
            SchemaInitializer.applyTo(executor);
            RunRepository runs = new RunRepository(executor);
            WindowOutputRepository outputs = new WindowOutputRepository(executor);

            runs.recordStart(new RunContext("run-a", STARTED, "tweet_hashtag", "{\"a\":1}"));
            int written = outputs.insertAll("run-a", List.of(PYTHON, JAVA));
            runs.recordFinish("run-a", STARTED.plusSeconds(20));

            runs.recordStart(new RunContext("run-b", STARTED.plusSeconds(60), "tweet_hashtag", "{\"b\":2}"));
            outputs.insertAll("run-b", List.of(PYTHON));

            assertThat(written).isEqualTo(2);
            assertThat(outputs.findByRunId("run-a")).containsExactly(JAVA, PYTHON);
            assertThat(outputs.countByRunId("run-a")).isEqualTo(2);
            assertThat(outputs.countByRunId("run-b")).isEqualTo(1);
            assertThat(runs.findByRunId("run-a")).map(RunContext::configJson).contains("{\"a\":1}");
            assertThat(runs.findByRunId("run-c")).isEmpty();
        }
    }

    /**
     * The upload path over a real file and a real database: the row, the bytes on disk and the id that
     * joins them have to agree, which is the one thing a fake executor cannot check.
     */
    @Test
    void storesAnUploadOnDiskReadsItBackAndCollectsItWhenItExpires() {
        byte[] csv = "user_id,text,timestamp\nuser_1,#AI,1000\nuser_2,#java,2000\n"
                .getBytes(StandardCharsets.UTF_8);
        Instant uploaded = Instant.parse("2026-11-16T09:00:00Z");

        try (DuckDbExecutor executor = new DuckDbExecutor(tempDir.resolve("datasets.duckdb"));
                DatasetRepository datasets = new DatasetRepository(executor, tempDir.resolve("uploads"),
                        Duration.ofDays(7))) {

            Dataset stored = datasets.store(csv, "my tweets.csv", uploaded);

            assertThat(datasets.findAll()).extracting(Dataset::datasetId).containsExactly(stored.datasetId());
            assertThat(datasets.find(stored.datasetId()))
                    .hasValue(new Dataset(stored.datasetId(), uploaded, uploaded.plus(Duration.ofDays(7)),
                            "my tweets.csv", csv.length, 2));
            assertThat(datasets.pathOf(stored.datasetId()))
                    .contains(tempDir.resolve("uploads").resolve(stored.datasetId() + ".csv"));
            assertThat(datasets.delete("ds-19990101-0000-0000")).isFalse();

            assertThat(datasets.deleteExpired(uploaded.plus(Duration.ofDays(8)))).isEqualTo(1);
            assertThat(datasets.findAll()).isEmpty();
            assertThat(datasets.pathOf(stored.datasetId())).isEmpty();
            assertThat(tempDir.resolve("uploads")).isEmptyDirectory();
        }
    }

    @Test
    void applyingTheSchemaTwiceChangesNothing() {
        try (DuckDbExecutor executor = DuckDbExecutor.inMemory()) {
            SchemaInitializer.applyTo(executor);
            SchemaInitializer.applyTo(executor);

            new WindowOutputRepository(executor).insertAll("run-a", List.of(PYTHON));

            assertThat(executor.query("SELECT COUNT(*) FROM window_outputs", List.of()).get(0).getLong(0))
                    .isEqualTo(1);
        }
    }

    @Test
    void storesEveryDocumentedTable() {
        try (DuckDbExecutor executor = DuckDbExecutor.inMemory()) {
            SchemaInitializer.applyTo(executor);

            for (String table : List.of("runs", "datasets", "tin_vertices", "tin_edges",
                    "tin_interactions", "tin_states", "window_outputs")) {
                assertThat(executor.query("SELECT COUNT(*) FROM " + table, List.of()).get(0).getLong(0))
                        .as("table %s", table)
                        .isZero();
            }
        }
    }
}
