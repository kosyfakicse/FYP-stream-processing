package com.fyp.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fyp.RunContext;
import com.fyp.config.AppConfig;
import com.fyp.config.DatasetConfig;
import com.fyp.config.DatasetEntry;
import com.fyp.config.FlinkConfig;
import com.fyp.config.PipelineConfig;
import com.fyp.model.Attribution;
import com.fyp.model.Interaction;
import com.fyp.model.State;
import com.fyp.model.Vertex;
import com.fyp.model.WindowOutput;
import com.fyp.service.AttributionService;
import com.fyp.storage.DuckDbExecutor;
import com.fyp.storage.ProvenanceRepository;
import com.fyp.storage.RunRepository;
import com.fyp.storage.RunResults;
import com.fyp.storage.WindowOutputRepository;

/**
 * CSV in, DuckDB file out, results read back by run id. This is the test that would notice a
 * regression anywhere between the loader and the sink.
 */
class AttributionPipelineEndToEndTest {

    private static final String CSV = "user_id,text,timestamp\n"
            + "user_1,Loving #python today,1000\n"
            + "user_2,#python is awesome,1200\n"
            + "user_3,Check out #java,1400\n"
            + "user_4,#python and #streaming,1600\n";

    @TempDir
    Path tempDir;

    private Path datasetCsv;
    private AppConfig config;
    private String jdbcUrl;
    private DuckDbExecutor executor;

    @BeforeEach
    void setUp() throws IOException {
        datasetCsv = tempDir.resolve("tweets.csv");
        Files.writeString(datasetCsv, CSV, StandardCharsets.UTF_8);
        Path dbFile = tempDir.resolve("attribution.duckdb");
        jdbcUrl = DuckDbExecutor.urlFor(dbFile);

        config = new AppConfig(
                new DatasetConfig("tweet_hashtag", tempDir.resolve("uploads").toString(), 0,
                        new DatasetEntry("test data", datasetCsv.toString()), null, null),
                new FlinkConfig(1),
                new PipelineConfig(1, 1, 1, 100, dbFile.toString()));

        executor = new DuckDbExecutor(jdbcUrl);
    }


    @Test
    void runReturnsAnIdThatRetrievesExactlyThatRunsResults() throws Exception {
        try (AttributionService service = new AttributionService(config, executor, jdbcUrl)) {
            String runId = service.run(datasetCsv);
            RunResults results = service.fetchAll(runId);

            assertThat(runId).matches("run-\\d{8}-\\d{4}-[0-9a-f]{4}");
            assertThat(results.runId()).isEqualTo(runId);
            assertThat(results.outputs())
                    .extracting(WindowOutput::category)
                    .containsExactly("JAVA", "PYTHON", "STREAMING");
            assertThat(results.totalCounted()).isEqualTo(5);

            WindowOutput python = results.outputs().stream()
                    .filter(output -> output.category().equals("PYTHON"))
                    .findFirst()
                    .orElseThrow();
            assertThat(python.windowStart()).isEqualTo(1_000L);
            assertThat(python.windowEnd()).isEqualTo(2_000L);
            assertThat(python.outputCount()).isEqualTo(3);
            assertThat(python.attribution())
                    .isEqualTo(new Attribution(Map.of("user_1", 1L, "user_2", 1L, "user_4", 1L)));
        }
    }

    /**
     * The run records which operators it used and what each of them held. Two of the state rows are the
     * boxes the source and the map carry -- tag-blind, cumulative, with no user map because neither
     * operator ever held one -- and three are the windows, which is what a firing reports about itself.
     */
    @Test
    void recordsTheOperatorsAndTheBoxOfEveryVertexThatHasOne() throws Exception {
        try (AttributionService service = new AttributionService(config, executor, jdbcUrl)) {
            String runId = service.run(datasetCsv);
            ProvenanceRepository provenance = new ProvenanceRepository(executor);

            assertThat(provenance.findVertices(runId)).extracting(Vertex::vertexType)
                    .containsExactlyInAnyOrder("source", "map", "window", "sink");
            assertThat(provenance.findVertices(runId)).extracting(Vertex::description)
                    .contains("csv dataset tweet_hashtag (4 rows)",
                            "duckdb sink, window_outputs, tin_interactions and tin_states");
            assertThat(provenance.findEdges(runId))
                    .extracting(edge -> edge.sourceVertex() + "->" + edge.targetVertex())
                    .containsExactly("K1->M1", "M1->W1", "W1->S1");

            List<State> windows = provenance.findStates(runId).stream()
                    .filter(state -> state.vertexId().equals("W1")).toList();
            assertThat(windows).extracting(State::category).containsExactly("JAVA", "PYTHON", "STREAMING");
            assertThat(windows).allSatisfy(state -> {
                assertThat(state.timeStart()).isEqualTo(1_000L);
                assertThat(state.timeEnd()).isEqualTo(2_000L);
                assertThat(state.firedAt()).isEqualTo(2_000L);
                assertThat(state.quantity()).isEqualTo(state.attribution().total());
            });

            // Invariant 4, and the one that used to be unprovable: the source's own box, not a count
            // inferred from the events that survived the extractor.
            assertThat(boxOf(provenance.findStates(runId), "K1")).satisfies(state -> {
                assertThat(state.category()).isNull();
                assertThat(state.quantity()).isEqualTo(4L);
                assertThat(state.attribution()).isEqualTo(Attribution.empty());
            });
            assertThat(boxOf(provenance.findStates(runId), "M1")).satisfies(state -> {
                assertThat(state.quantity()).isEqualTo(5L);
                assertThat(state.attribution()).isEqualTo(Attribution.empty());
            });
        }
    }

    /**
     * Each interaction is measured where it happens, so the units of the two edges stay distinct: four
     * tweets went over {@code K1 -> M1} and five hashtag events over {@code M1 -> W1}, because one of
     * those tweets names two hashtags. The first count used to be inferred from the events that survived
     * the extractor, which is why it could only ever equal the second.
     */
    @Test
    void recordsEachInteractionWhereItHappened() throws Exception {
        try (AttributionService service = new AttributionService(config, executor, jdbcUrl)) {
            String runId = service.run(datasetCsv);
            List<Interaction> interactions = new ProvenanceRepository(executor).findInteractions(runId);

            assertThat(interactions).hasSize(7);
            Interaction intoTheMap = interactionOn(interactions, "K1", "M1", null);
            assertThat(intoTheMap.quantity()).isEqualTo(4L);
            assertThat(intoTheMap.attribution()).isEqualTo(new Attribution(Map.of("K1", 4L)));

            assertThat(interactions).filteredOn(row -> row.sourceVertex().equals("M1"))
                    .extracting(Interaction::quantity)
                    .containsExactlyInAnyOrder(1L, 3L, 1L);
            assertThat(interactions).filteredOn(row -> row.sourceVertex().equals("M1"))
                    .extracting(Interaction::attribution)
                    .containsOnly(new Attribution(Map.of("M1", 1L)), new Attribution(Map.of("M1", 3L)));

            // The sink was handed exactly what the window counted, which is the pair a checker compares.
            assertThat(interactions).filteredOn(row -> row.sourceVertex().equals("W1"))
                    .extracting(Interaction::quantity)
                    .containsExactlyInAnyOrder(1L, 3L, 1L);

            assertThat(interactions).allSatisfy(row -> {
                assertThat(row.timestamp()).isEqualTo(2_000L);
                assertThat(row.quantity()).isEqualTo(row.attribution().total());
            });
        }
    }

    private static State boxOf(List<State> states, String vertexId) {
        return states.stream().filter(state -> state.vertexId().equals(vertexId))
                .findFirst().orElseThrow();
    }

    private static Interaction interactionOn(List<Interaction> interactions, String from, String to,
            String category) {
        return interactions.stream()
                .filter(row -> row.sourceVertex().equals(from)
                        && row.targetVertex().equals(to)
                        && (category == null ? row.category() == null : category.equals(row.category())))
                .findFirst().orElseThrow();
    }

    @Test
    void aSecondRunIntoTheSameFileKeepsItsOwnResultsAndItsOwnKeys() throws Exception {
        try (AttributionService service = new AttributionService(config, executor, jdbcUrl)) {
            String first = service.run(datasetCsv);
            String second = service.run(datasetCsv);

            assertThat(second).isNotEqualTo(first);
            assertThat(new WindowOutputRepository(executor).countByRunId(first)).isEqualTo(3);
            assertThat(new WindowOutputRepository(executor).countByRunId(second)).isEqualTo(3);
            assertThat(new WindowOutputRepository(executor).findByRunId(first))
                    .extracting(WindowOutput::category)
                    .containsExactly("JAVA", "PYTHON", "STREAMING");

            // Every run calls its window operator W1, so the graph is only storable at all because the
            // vertex ids are scoped to a run rather than to the database.
            ProvenanceRepository provenance = new ProvenanceRepository(executor);
            assertThat(provenance.findVertices(first)).extracting(Vertex::vertexId)
                    .containsExactlyInAnyOrder("K1", "M1", "W1", "S1");
            assertThat(provenance.findVertices(second)).extracting(Vertex::vertexId)
                    .containsExactlyInAnyOrder("K1", "M1", "W1", "S1");
            assertThat(provenance.findStates(first)).hasSize(5);
            assertThat(provenance.findStates(second)).hasSize(5);
            assertThat(provenance.findInteractions(first)).hasSize(7);
            assertThat(provenance.findInteractions(second)).hasSize(7);
        }
    }

    @Test
    void recordsTheRunRowWithItsDatasetAndConfigSnapshot() throws Exception {
        try (AttributionService service = new AttributionService(config, executor, jdbcUrl)) {
            String runId = service.run(datasetCsv);

            RunRepository runs = new RunRepository(executor);
            RunContext recorded = runs.findByRunId(runId).orElseThrow();

            assertThat(recorded.runId()).isEqualTo(runId);
            assertThat(recorded.dataset()).isEqualTo("tweet_hashtag");
            assertThat(recorded.configJson()).contains("\"target_dataset\":\"tweet_hashtag\"");
            assertThat(executor.query("SELECT finished_at FROM runs WHERE run_id = ?", List.of(runId))
                    .get(0).getObject(0)).isNotNull();
        }
    }
}
