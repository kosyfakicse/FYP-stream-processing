package com.fyp.service;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import com.fyp.RunContext;
import com.fyp.config.AppConfig;
import com.fyp.model.Tweet;
import com.fyp.model.UnifiedEvent;
import com.fyp.model.WindowOutput;
import com.fyp.pipeline.flink.CategoryAttributionFunction;
import com.fyp.pipeline.flink.EventTimestampAssigner;
import com.fyp.pipeline.flink.HashtagExtractor;
import com.fyp.pipeline.loader.CsvDatasetLoader;
import com.fyp.pipeline.sink.DuckDbSink;
import com.fyp.storage.DuckDbExecutor;
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
     * Reads the dataset, attributes every hashtag over event-time windows, stores the results under
     * a freshly generated run id and returns that id.
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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.flink().parallelism());
        env.fromData(tweets)
                .flatMap(new HashtagExtractor())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UnifiedEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new EventTimestampAssigner()))
                .keyBy((KeySelector<UnifiedEvent, String>) UnifiedEvent::category)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSizeSeconds)))
                .process(new CategoryAttributionFunction())
                .sinkTo(new DuckDbSink(jdbcUrl, run.runId(), config.pipeline().batchSize()));

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

    @Override
    public void close() {
        executor.close();
    }
}
