package com.fyp.model;

import java.io.Serializable;

/**
 * The vertex ids a run minted for its own chain, handed to the sink so the rows it writes name the
 * operators they describe.
 *
 * <p>A record of ids rather than the {@code ProvenanceGraph} itself: the graph is a builder that mints
 * the next name on every call, and shipping that into a job would put mutable state on the wire.
 *
 * <p>Serialisable because it travels inside {@code DuckDbSink} to every task that runs it.
 */
public record RunVertices(String runId, String sourceVertex, String mapVertex, String windowVertex,
        String sinkVertex) implements Serializable {

    private static final long serialVersionUID = 1L;
}
