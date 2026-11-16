package com.fyp.pipeline.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.fyp.model.WindowOutput;
import com.fyp.storage.DuckDbExecutor;
import com.fyp.storage.SqlExecutor;
import com.fyp.storage.WindowOutputRepository;

/**
 * Buffers window results and hands each full batch to the repository, which is the Java answer to
 * Python's {@code DatabaseBatchWriter}: same buffer, same default of 100, same flush-on-close, but
 * the flush is no longer a side effect hidden in a map function.
 *
 * <p>The batching seam is {@link SqlExecutor}, so a batch can be inspected without a database.
 */
public final class DuckDbSinkWriter implements SinkWriter<WindowOutput> {

    private final SqlExecutor sql;
    private final WindowOutputRepository repository;
    private final String runId;
    private final int batchSize;
    private final List<WindowOutput> buffer = new ArrayList<>();

    private long written;

    public DuckDbSinkWriter(String jdbcUrl, String runId, int batchSize) {
        this(new DuckDbExecutor(jdbcUrl), runId, batchSize);
    }

    public DuckDbSinkWriter(SqlExecutor sql, String runId, int batchSize) {
        this.sql = sql;
        this.repository = new WindowOutputRepository(sql);
        this.runId = runId;
        this.batchSize = batchSize;
    }

    @Override
    public void write(WindowOutput output, Context context) throws IOException {
        buffer.add(output);
        if (buffer.size() >= batchSize) {
            flushBuffer();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        flushBuffer();
    }

    @Override
    public void close() throws IOException {
        flushBuffer();
        sql.close();
    }

    /** Rows written so far, which is what the tests assert on instead of a log line. */
    public long written() {
        return written;
    }

    private void flushBuffer() throws IOException {
        try {
            written += repository.insertAll(runId, buffer);
        } catch (RuntimeException e) {
            throw new IOException("Failed to write " + buffer.size() + " window outputs for run " + runId, e);
        }
        buffer.clear();
    }
}
