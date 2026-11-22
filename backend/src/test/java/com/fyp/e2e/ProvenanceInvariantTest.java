package com.fyp.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fyp.config.AppConfig;
import com.fyp.config.DatasetConfig;
import com.fyp.config.DatasetEntry;
import com.fyp.config.FlinkConfig;
import com.fyp.config.PipelineConfig;
import com.fyp.model.Interaction;
import com.fyp.model.RunGraph;
import com.fyp.model.State;
import com.fyp.model.WindowOutput;
import com.fyp.service.AttributionService;
import com.fyp.storage.DuckDbExecutor;
import com.fyp.storage.WindowOutputRepository;

/**
 * Invariants 1 to 3 of {@code docs/TIN.md}, run against the rows a finished job stored rather than
 * against the records its operators emitted. This is the file that answers "is the graph checkable" --
 * the other end-to-end test proves the rows exist and say the right thing about one interval, and these
 * prove the two tables agree with each other across several.
 *
 * <p>The fixture therefore needs what the shipped one does not: <b>two state ticks inside one window</b>
 * (window 4s, both ticks 1s, and the intervals are epoch-anchored so all five tweets share the window
 * {@code [0, 4000)}). With one interval a box has no previous row to subtract, so invariant 1 collapses
 * into "the box equals the edge", which is the same number read twice. Here the box is 3 then 5 against
 * edges of 3 and 2, and only the subtraction makes them match -- and {@code PYTHON}'s count of 3 is the
 * first number in this suite that is a sum of two separately measured rows.
 *
 * <p>Each check is per unit, never per key: the interaction maps name vertices and the window state map
 * names users, so only quantities may be compared across the two tables.
 */
class ProvenanceInvariantTest {

    /** Five tweets, six events: {@code user_4} names two hashtags, which is the whole difficulty. */
    private static final String CSV = "user_id,text,timestamp\n"
            + "user_1,Loving #python today,1000\n"
            + "user_2,#python is awesome,1300\n"
            + "user_3,Check out #java,1800\n"
            + "user_4,#python and #streaming,2100\n"
            + "user_5,#java rules,2600\n";

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
                // The first run in the suite where the three intervals are not all the same size.
                new PipelineConfig(4, 1, 1, 100, dbFile.toString()));

        executor = new DuckDbExecutor(jdbcUrl);
    }

    /**
     * Invariant 1, on both boxes. Each box is checked against the edge leaving it, and that is a rule about
     * units rather than a neat pattern: {@code K1}'s box and {@code K1 -> M1} both count tweets, and
     * {@code M1}'s box and {@code M1 -> W1} both count events. Checking {@code M1}'s growth against the edge
     * <i>entering</i> it would compare 2 tweets with the 3 events that interval added, so the obvious reading
     * of the invariant fails on a run that is working -- the extractor is where the two units part.
     */
    @Test
    void eachBoxGrowsByExactlyWhatItsOperatorPassedOn() throws Exception {
        RunGraph graph = runOnce().graph();

        assertThat(growthOf(graph, "K1"))
                .containsExactly(entry(2_000L, 3L), entry(3_000L, 2L))
                .isEqualTo(carriedOver(graph, "K1", "M1"));
        assertThat(growthOf(graph, "M1"))
                .containsExactly(entry(2_000L, 3L), entry(3_000L, 3L))
                .isEqualTo(carriedOver(graph, "M1", "W1"));
    }

    /**
     * Invariant 2. The per-category interactions are now measured on the edge instead of copied out of the
     * window, so this sum is a real cross-check: {@code PYTHON} reaches its count of 3 only by adding the
     * 2 events of the first tick to the 1 of the second.
     */
    @Test
    void theInteractionsIntoAWindowSumToTheResultItReports() throws Exception {
        Stored stored = runOnce();
        assertThat(stored.outputs()).extracting(WindowOutput::category, WindowOutput::windowEnd,
                WindowOutput::outputCount)
                .containsExactly(tuple("JAVA", 4_000L, 2L), tuple("PYTHON", 4_000L, 3L),
                        tuple("STREAMING", 4_000L, 1L));

        assertThat(stored.outputs()).allSatisfy(output -> {
            long carried = stored.graph().interactions().stream()
                    .filter(row -> row.sourceVertex().equals("M1") && row.targetVertex().equals("W1"))
                    .filter(row -> output.category().equals(row.category()))
                    .filter(row -> row.timestamp() > output.windowStart()
                            && row.timestamp() <= output.windowEnd())
                    .mapToLong(Interaction::quantity)
                    .sum();
            assertThat(carried).isEqualTo(output.outputCount());
            assertThat(output.attribution().total()).isEqualTo(output.outputCount());
        });
    }

    /**
     * Invariant 3. The window reports itself twice, once as its own state and once as what it handed
     * downstream, and only the second of those passes through the sink's buffer -- so a writer that dropped
     * a record would part these two numbers while leaving every other row intact.
     */
    @Test
    void everyFiringReachesTheSinkWithTheQuantityItLeftTheWindowWith() throws Exception {
        RunGraph graph = runOnce().graph();

        Map<String, Long> held = new TreeMap<>();
        for (State row : graph.states()) {
            if (row.vertexId().equals("W1")) {
                held.put(row.category() + "@" + row.timeEnd(), row.quantity());
            }
        }
        Map<String, Long> passedOn = new TreeMap<>();
        for (Interaction row : graph.interactions()) {
            if (row.sourceVertex().equals("W1")) {
                passedOn.put(row.category() + "@" + row.timestamp(), row.quantity());
            }
        }

        assertThat(held).containsOnly(entry("JAVA@4000", 2L), entry("PYTHON@4000", 3L),
                entry("STREAMING@4000", 1L));
        assertThat(passedOn).isEqualTo(held);
    }

    private Stored runOnce() throws Exception {
        try (AttributionService service = new AttributionService(config, executor, jdbcUrl)) {
            String runId = service.run(datasetCsv);
            return new Stored(service.findGraph(runId),
                    new WindowOutputRepository(executor).findByRunId(runId));
        }
    }

    /** Everything one finished run left in the database, read back through the same calls the API uses. */
    private record Stored(RunGraph graph, List<WindowOutput> outputs) {
    }

    /** The box rows are cumulative, so an interval's traffic is the difference to the row before it. */
    private static Map<Long, Long> growthOf(RunGraph graph, String vertexId) {
        Map<Long, Long> growth = new TreeMap<>();
        long soFar = 0L;
        for (State row : graph.states().stream().filter(state -> state.vertexId().equals(vertexId))
                .sorted(Comparator.comparingLong(State::timeEnd)).toList()) {
            growth.put(row.timeEnd(), row.quantity() - soFar);
            soFar = row.quantity();
        }
        return growth;
    }

    /** What one edge carried per interval, summed because {@code M1 -> W1} writes a row per hashtag. */
    private static Map<Long, Long> carriedOver(RunGraph graph, String from, String to) {
        Map<Long, Long> quantity = new TreeMap<>();
        for (Interaction row : graph.interactions()) {
            if (row.sourceVertex().equals(from) && row.targetVertex().equals(to)) {
                quantity.merge(row.timestamp(), row.quantity(), Long::sum);
            }
        }
        return quantity;
    }
}
