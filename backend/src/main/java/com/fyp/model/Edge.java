package com.fyp.model;

/**
 * One data flow connection of one run's network: a row of {@code tin_edges}.
 *
 * <p>Logical, not physical. Flink chains the map and the window functions into a single task, so no
 * process boundary sits between {@code M1} and {@code W1} -- the edge is still the truth, because that
 * is where the operator boundary is.
 *
 * @param runId the run whose topology this is
 * @param sourceVertex the upstream operator's id
 * @param targetVertex the downstream operator's id
 */
public record Edge(String runId, String sourceVertex, String targetVertex) {
}
