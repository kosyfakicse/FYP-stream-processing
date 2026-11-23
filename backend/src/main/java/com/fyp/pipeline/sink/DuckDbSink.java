package com.fyp.pipeline.sink;

import java.io.IOException;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import com.fyp.model.ProvenanceRecord;
import com.fyp.model.RunVertices;

/**
 * Persists what the job observed straight into DuckDB from inside the job.
 *
 * <p>Replaces the Python approach in {@code legacy/src/fyp_attribution/main.py}, where
 * {@code write_category_to_db} was a map function holding a module-global batch writer: state that
 * only worked because PyFlink ran one worker, and that could not be closed on shutdown. Sink V2 is
 * the supported place for an output with a buffer and a flush.
 *
 * <p>The input type carries every shape a run stores -- see {@link ProvenanceRecord} for why they share
 * one stream. The sink itself carries only the connection string, the run id and the vertex ids the rows
 * are labelled with, so it stays serialisable; the JDBC connection belongs to the writer.
 */
public final class DuckDbSink implements Sink<ProvenanceRecord> {

    private static final long serialVersionUID = 1L;

    private final String jdbcUrl;
    private final RunVertices chain;
    private final int batchSize;

    public DuckDbSink(String jdbcUrl, RunVertices chain, int batchSize) {
        this.jdbcUrl = jdbcUrl;
        this.chain = chain;
        this.batchSize = batchSize;
    }

    @Override
    public SinkWriter<ProvenanceRecord> createWriter(WriterInitContext context) throws IOException {
        return new DuckDbSinkWriter(jdbcUrl, chain, batchSize);
    }
}
