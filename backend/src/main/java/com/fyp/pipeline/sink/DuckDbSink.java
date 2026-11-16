package com.fyp.pipeline.sink;

import java.io.IOException;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import com.fyp.model.WindowOutput;

/**
 * Persists window results straight into DuckDB from inside the job.
 *
 * <p>Replaces the Python approach in {@code legacy/src/fyp_attribution/main.py}, where
 * {@code write_category_to_db} was a map function holding a module-global batch writer: state that
 * only worked because PyFlink ran one worker, and that could not be closed on shutdown. Sink V2 is
 * the supported place for an output with a buffer and a flush.
 *
 * <p>The sink carries only the connection string, the run id to tag rows with and the batch size, so
 * it stays serialisable; the JDBC connection belongs to the writer.
 */
public final class DuckDbSink implements Sink<WindowOutput> {

    private static final long serialVersionUID = 1L;

    private final String jdbcUrl;
    private final String runId;
    private final int batchSize;

    public DuckDbSink(String jdbcUrl, String runId, int batchSize) {
        this.jdbcUrl = jdbcUrl;
        this.runId = runId;
        this.batchSize = batchSize;
    }

    @Override
    public SinkWriter<WindowOutput> createWriter(WriterInitContext context) throws IOException {
        return new DuckDbSinkWriter(jdbcUrl, runId, batchSize);
    }
}
