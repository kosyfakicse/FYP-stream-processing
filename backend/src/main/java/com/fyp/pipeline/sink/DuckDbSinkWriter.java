package com.fyp.pipeline.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.fyp.model.Interaction;
import com.fyp.model.ProvenanceRecord;
import com.fyp.model.RunVertices;
import com.fyp.model.State;
import com.fyp.model.WindowOutput;
import com.fyp.storage.DuckDbExecutor;
import com.fyp.storage.ProvenanceRepository;
import com.fyp.storage.SqlExecutor;
import com.fyp.storage.WindowOutputRepository;

/**
 * Buffers what the job observed and hands each full batch to the repositories, which is the Java answer
 * to Python's {@code DatabaseBatchWriter}: same buffer, same default of 100, same flush-on-close, but the
 * flush is no longer a side effect hidden in a map function.
 *
 * <p>Every flush writes three tables: the window results, the interactions and the states. A fired window
 * contributes one row to each table plus its own {@code W1 -> S1} interaction, and a tick record contributes
 * the one row it observed at its own vertex, so both reach storage through this one path and cannot
 * disagree about what a window counted.
 *
 * <p>The batching seam is {@link SqlExecutor}, so a batch can be inspected without a database.
 */
public final class DuckDbSinkWriter implements SinkWriter<ProvenanceRecord> {

    private final SqlExecutor sql;
    private final WindowOutputRepository windowOutputs;
    private final ProvenanceRepository provenance;
    private final RunVertices chain;
    private final int batchSize;
    private final List<ProvenanceRecord> buffer = new ArrayList<>();

    private long written;

    public DuckDbSinkWriter(String jdbcUrl, RunVertices chain, int batchSize) {
        this(new DuckDbExecutor(jdbcUrl), chain, batchSize);
    }

    public DuckDbSinkWriter(SqlExecutor sql, RunVertices chain, int batchSize) {
        this.sql = sql;
        this.windowOutputs = new WindowOutputRepository(sql);
        this.provenance = new ProvenanceRepository(sql);
        this.chain = chain;
        this.batchSize = batchSize;
    }

    @Override
    public void write(ProvenanceRecord record, Context context) throws IOException {
        buffer.add(record);
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

    /** Window results written so far, which is what the tests assert on instead of a log line. */
    public long written() {
        return written;
    }

    private void flushBuffer() throws IOException {
        List<WindowOutput> firings = new ArrayList<>();
        List<State> states = new ArrayList<>();
        List<Interaction> interactions = new ArrayList<>();
        for (ProvenanceRecord record : buffer) {
            switch (record.kind()) {
                case FIRING -> firings.add(record.firing());
                case STATE -> states.add(record.state());
                case INTERACTION -> interactions.add(record.interaction());
            }
        }
        states.addAll(statesOf(firings));
        interactions.addAll(interactionsOf(firings));

        try {
            written += windowOutputs.insertAll(chain.runId(), chain.windowVertex(), firings);
            provenance.insertInteractions(interactions);
            provenance.insertStates(states);
        } catch (RuntimeException e) {
            throw new IOException("Failed to write " + firings.size() + " window outputs, "
                    + interactions.size() + " interactions and " + states.size() + " states for run "
                    + chain.runId(), e);
        }
        buffer.clear();
    }

    /** What left the window for the sink. The other two edges of the chain report from their own operators. */
    private List<Interaction> interactionsOf(List<WindowOutput> batch) {
        return batch.stream().map(firing -> Interaction.ofFiring(chain, firing)).toList();
    }

    private List<State> statesOf(List<WindowOutput> batch) {
        return batch.stream().map(firing -> State.ofWindow(chain.runId(), chain.windowVertex(), firing))
                .toList();
    }
}
