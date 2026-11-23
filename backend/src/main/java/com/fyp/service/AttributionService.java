package com.fyp.service;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import com.fyp.RunContext;
import com.fyp.config.AppConfig;
import com.fyp.model.ProvenanceRecord;
import com.fyp.model.RunGraph;
import com.fyp.model.RunVertices;
import com.fyp.model.Tweet;
import com.fyp.model.UnifiedEvent;
import com.fyp.model.Vertex;
import com.fyp.model.WindowOutput;
import com.fyp.pipeline.flink.BoxRecorder;
import com.fyp.pipeline.flink.CategoryAttributionFunction;
import com.fyp.pipeline.flink.HashtagExtractor;
import com.fyp.pipeline.flink.InteractionRecorder;
import com.fyp.pipeline.flink.TweetTimestampAssigner;
import com.fyp.pipeline.graph.ProvenanceGraph;
import com.fyp.pipeline.graph.ProvenanceGraph.OperatorKind;
import com.fyp.pipeline.loader.CsvDatasetLoader;
import com.fyp.pipeline.sink.DuckDbSink;
import com.fyp.storage.DuckDbExecutor;
import com.fyp.storage.ProvenanceRepository;
import com.fyp.storage.Run;
import com.fyp.storage.RunRepository;
import com.fyp.storage.RunResults;
import com.fyp.storage.SchemaInitializer;
import com.fyp.storage.SqlExecutor;
import com.fyp.storage.WindowOutputRepository;

/**
 * The whole pipeline as two calls, which is the shape the project is built around:
 *
 * <pre>
 * String runId = service.run(csvPath);
 * RunResults results = service.fetchAll(runId);
 * </pre>
 *
 * <p>Replaces {@code main()} in {@code legacy/src/fyp_attribution/main.py}, where the topology was
 * wired inside a script that could only run against Kafka. The dataset is the source here, so the
 * job is bounded and finishes.
 */
public final class AttributionService implements AutoCloseable {

    /**
     * The key for the branches that measure a whole operator rather than one hashtag. {@code K1} counts
     * rows and {@code M1} counts events, so neither has a category to key by -- putting the hashtag split
     * on their rows would describe a tally the operator never held.
     */
    private static final String WHOLE_CHAIN = "all";

    private final AppConfig config;
    private final SqlExecutor executor;
    private final String jdbcUrl;

    public AttributionService(AppConfig config, SqlExecutor executor, String jdbcUrl) {
        this.config = config;
        this.executor = executor;
        this.jdbcUrl = jdbcUrl;
    }

    /** Opens the DuckDB file named in the config, eg {@code ../data/attribution.duckdb}. */
    public static AttributionService fromConfig(AppConfig config) {
        String url = DuckDbExecutor.urlFor(Path.of(config.pipeline().duckdbFile()));
        return new AttributionService(config, new DuckDbExecutor(url), url);
    }

    /**
     * Creates the tables from {@code schema.sql} on this service's database, if they are not there
     * already.
     *
     * <p>Every statement is {@code CREATE TABLE IF NOT EXISTS}, so this is a no-op on a database that
     * has been used before. The tables belong to the file rather than to this connection, so the one
     * call also covers the upload service's separate connection to the same file.
     */
    public void ensureSchema() {
        SchemaInitializer.applyTo(executor);
    }

    /**
     * Reads the dataset, attributes every hashtag over event-time windows, stores the results and the
     * network of operators that produced them under a freshly generated run id, and returns that id.
     *
     * <p>The command line route: {@code runs.dataset} records the configured dataset name, because
     * that is the only name this caller has.
     */
    public String run(Path datasetCsv) throws Exception {
        return run(datasetCsv, config.dataset().targetDataset());
    }

    /**
     * The same run over a file the caller can name.
     *
     * @param datasetLabel what {@code runs.dataset} records -- the {@code ds-...} id of a stored
     *        upload, so the run row points back at the file it read
     */
    public String run(Path datasetCsv, String datasetLabel) throws Exception {
        RunContext run = RunContext.start(config, datasetLabel);
        ensureSchema();
        RunRepository runs = new RunRepository(executor);
        runs.recordStart(run);

        List<Tweet> tweets = new CsvDatasetLoader(datasetCsv).readAll();
        int windowSizeSeconds = config.pipeline().windowSizeSeconds();

        // These four mints describe the chain under them in the same order: source, map, window, sink.
        // The graph is written here rather than from inside an operator because the
        // dataset is loaded before the job starts, so no operator knows its name or its row count.
        ProvenanceGraph graph = new ProvenanceGraph(run.runId());
        Vertex k1 = graph.mint(OperatorKind.SOURCE,
                "csv dataset " + datasetLabel + " (" + tweets.size() + " rows)");
        Vertex m1 = graph.mint(OperatorKind.MAP, "hashtag extractor, one event per hashtag");
        Vertex w1 = graph.mint(OperatorKind.WINDOW,
                windowSizeSeconds + "s tumbling count, keyed by category");
        Vertex s1 = graph.mint(OperatorKind.SINK,
                "duckdb sink, window_outputs, tin_interactions and tin_states");
        graph.connect(k1, m1);
        graph.connect(m1, w1);
        graph.connect(w1, s1);
        ProvenanceRepository provenance = new ProvenanceRepository(executor);
        provenance.insertVertices(graph.vertices());
        provenance.insertEdges(graph.edges());
        RunVertices chain = new RunVertices(run.runId(), k1.vertexId(), m1.vertexId(), w1.vertexId(),
                s1.vertexId());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.flink().parallelism());
        DataStream<Tweet> tweetsRead = env.fromData(tweets)
                // On the tweets, not on the events: a stage only knows which interval it is in if the
                // time was attached upstream of it, and the extractor is where tweets stop existing.
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tweet>forMonotonousTimestamps()
                        .withTimestampAssigner(new TweetTimestampAssigner()));
        DataStream<UnifiedEvent> eventsEmitted = tweetsRead.flatMap(new HashtagExtractor());

        Duration stateTick = Duration.ofSeconds(config.pipeline().stateTickSeconds());
        Duration interactionTick = Duration.ofSeconds(config.pipeline().interactionTickSeconds());

        // Four observers over the chain, one per thing the graph has to say about it: what each of K1 and
        // M1 holds, and the K1 -> M1 and M1 -> W1 interactions. They are separate branches rather than one
        // pass because keyBy(category) means no operator ever sees the whole chain, and because a tick's
        // interval is its own knob -- the box and the handoff on the same edge can be recorded at
        // different rates. Each counts what arrived at its own upstream operator, which is what the
        // interaction inferred from a closed window could not: a tweet naming no hashtag reached K1 anyway.
        DataStream<ProvenanceRecord> sourceBox = tweetsRead
                .keyBy((KeySelector<Tweet, String>) tweet -> WHOLE_CHAIN)
                .window(TumblingEventTimeWindows.of(stateTick))
                .process(new BoxRecorder<Tweet>(chain, k1.vertexId(), m1.vertexId()));
        DataStream<ProvenanceRecord> mapBox = eventsEmitted
                .keyBy((KeySelector<UnifiedEvent, String>) event -> WHOLE_CHAIN)
                .window(TumblingEventTimeWindows.of(stateTick))
                .process(new BoxRecorder<UnifiedEvent>(chain, m1.vertexId(), null));
        DataStream<ProvenanceRecord> mapInteractions = eventsEmitted
                .keyBy((KeySelector<UnifiedEvent, String>) UnifiedEvent::category)
                .window(TumblingEventTimeWindows.of(interactionTick))
                .process(new InteractionRecorder(chain, m1.vertexId(), w1.vertexId()));
        DataStream<ProvenanceRecord> firings = eventsEmitted
                .keyBy((KeySelector<UnifiedEvent, String>) UnifiedEvent::category)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSizeSeconds)))
                .process(new CategoryAttributionFunction())
                .map(ProvenanceRecord::ofFiring);

        // One stream into the sink, because the writer is the only thing in the job allowed to open the
        // database: DuckDB takes one read-write process per file, the API server is that process during a
        // run, and the at-least-once guard is one flush plus a natural unique key.
        sourceBox.union(mapBox, mapInteractions, firings)
                .sinkTo(new DuckDbSink(jdbcUrl, chain, config.pipeline().batchSize()));

        env.execute("hashtag attribution " + run.runId());

        runs.recordFinish(run.runId(), Instant.now());
        return run.runId();
    }

    /** Every window result one run produced, which is what the run id is for. */
    public RunResults fetchAll(String runId) {
        List<WindowOutput> outputs = new WindowOutputRepository(executor).findByRunId(runId);
        return new RunResults(runId, outputs);
    }

    /** The recorded row for one run, empty for an id that never ran. */
    public Optional<Run> findRun(String runId) {
        return new RunRepository(executor).find(runId);
    }

    /** Every run this database holds, newest first. */
    public List<Run> listRuns() {
        return new RunRepository(executor).findAll();
    }

    /**
     * The provenance one run left behind: its operators, the flows between them, and what each held and
     * passed on. An id that never ran gives four empty lists, like every other read here -- the run row
     * is what says whether an id is a run at all, which is why the route checks it first.
     */
    public RunGraph findGraph(String runId) {
        ProvenanceRepository provenance = new ProvenanceRepository(executor);
        return new RunGraph(provenance.findVertices(runId), provenance.findEdges(runId),
                provenance.findStates(runId), provenance.findInteractions(runId));
    }

    @Override
    public void close() {
        executor.close();
    }
}
