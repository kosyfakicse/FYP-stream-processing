package com.fyp.pipeline.graph;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import com.fyp.model.Edge;
import com.fyp.model.Vertex;

/**
 * The operators one run built, described as a temporal interaction network.
 *
 * <p>Written from outside the job on purpose. The topology never depends on data, and the dataset is
 * loaded before {@code env.execute()}, so no operator can know the file or the row count that its own
 * source vertex has to describe.
 *
 * <p>Ids come from a per-kind counter, so a second map becomes {@code M2} without anyone having
 * claimed the name first. That makes the order of {@link #mint} calls part of the contract: it follows
 * the order the topology is assembled in, and an unstable order would rename the vertices between two
 * runs and leave their rows silently incomparable.
 */
public final class ProvenanceGraph {

    /** The operator types of the reference schema, with the letter each one mints its ids under. */
    public enum OperatorKind {
        SOURCE("K", "source"),
        MAP("M", "map"),
        FILTER("F", "filter"),
        WINDOW("W", "window"),
        SINK("S", "sink");

        private final String idPrefix;
        private final String vertexType;

        OperatorKind(String idPrefix, String vertexType) {
            this.idPrefix = idPrefix;
            this.vertexType = vertexType;
        }
    }

    private final String runId;
    private final Map<OperatorKind, Integer> minted = new EnumMap<>(OperatorKind.class);
    private final List<Vertex> vertices = new ArrayList<>();
    private final List<Edge> edges = new ArrayList<>();

    public ProvenanceGraph(String runId) {
        this.runId = runId;
    }

    /** The next free operator of its kind, so the first window is {@code W1} and the second {@code W2}. */
    public Vertex mint(OperatorKind kind, String description) {
        int ordinal = minted.merge(kind, 1, Integer::sum);
        Vertex vertex = new Vertex(kind.idPrefix + ordinal, runId, kind.vertexType, description);
        vertices.add(vertex);
        return vertex;
    }

    public Edge connect(Vertex from, Vertex to) {
        Edge edge = new Edge(runId, from.vertexId(), to.vertexId());
        edges.add(edge);
        return edge;
    }

    public List<Vertex> vertices() {
        return List.copyOf(vertices);
    }

    public List<Edge> edges() {
        return List.copyOf(edges);
    }
}
