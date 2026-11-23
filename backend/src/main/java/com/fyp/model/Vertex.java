package com.fyp.model;

/**
 * One operator of one run's temporal interaction network: a row of {@code tin_vertices}.
 *
 * <p>Ids are minted per run by counting operator types, so every run names its window operator
 * {@code W1}. The id alone is therefore meaningless; {@code runId} travels with it, and every read
 * filters on both.
 *
 * @param vertexId the operator's id within the run: {@code K1}, {@code M1}, {@code W1}, {@code S1}
 * @param runId the run that built this operator
 * @param vertexType {@code source}, {@code map}, {@code filter}, {@code window} or {@code sink}
 * @param description what the operator actually did, for whoever reads the graph back
 */
public record Vertex(String vertexId, String runId, String vertexType, String description) {
}
