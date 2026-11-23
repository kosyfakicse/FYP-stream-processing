package com.fyp.storage;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import com.fyp.model.Attribution;
import com.fyp.model.Edge;
import com.fyp.model.Interaction;
import com.fyp.model.State;
import com.fyp.model.Vertex;

/**
 * Writes and reads the four tables one run's temporal interaction network is made of.
 *
 * <p>One repository over {@code tin_vertices}, {@code tin_edges}, {@code tin_interactions} and
 * {@code tin_states} rather than this project's one-table-each habit, because they are a single
 * artifact: the graph of one run, written in one order and read back as a whole.
 *
 * <p>The {@code *_id} columns are absent from every INSERT: DuckDB fills them from their sequences.
 */
public final class ProvenanceRepository {

    private static final String INSERT_VERTEX =
            "INSERT INTO tin_vertices (vertex_id, run_id, vertex_type, description) VALUES (?, ?, ?, ?)";

    private static final String INSERT_EDGE =
            "INSERT INTO tin_edges (source_vertex, target_vertex, run_id) VALUES (?, ?, ?)";

    private static final String INSERT_INTERACTION =
            "INSERT INTO tin_interactions (run_id, source_vertex, target_vertex, category, timestamp,"
                    + " quantity, attribution_json) VALUES (?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_STATE =
            "INSERT INTO tin_states (run_id, vertex_id, category, time_start, time_end, quantity,"
                    + " attribution_json, fired_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String FIND_VERTICES =
            "SELECT vertex_id, vertex_type, description FROM tin_vertices WHERE run_id = ?"
                    + " ORDER BY vertex_id";

    private static final String FIND_EDGES =
            "SELECT source_vertex, target_vertex FROM tin_edges WHERE run_id = ?"
                    + " ORDER BY source_vertex, target_vertex";

    private static final String FIND_INTERACTIONS =
            "SELECT source_vertex, target_vertex, category, timestamp, quantity, attribution_json"
                    + " FROM tin_interactions WHERE run_id = ?"
                    + " ORDER BY timestamp, source_vertex, target_vertex, category";

    private static final String FIND_STATES =
            "SELECT vertex_id, category, time_start, time_end, quantity, attribution_json, fired_at"
                    + " FROM tin_states WHERE run_id = ? ORDER BY time_start, vertex_id, category";

    private final SqlExecutor sql;

    public ProvenanceRepository(SqlExecutor sql) {
        this.sql = sql;
    }

    /** The topology, stored before any window has fired: what the run was going to do. */
    public int insertVertices(List<Vertex> vertices) {
        return batch(INSERT_VERTEX, vertices, ProvenanceRepository::vertexParams);
    }

    public int insertEdges(List<Edge> edges) {
        return batch(INSERT_EDGE, edges, ProvenanceRepository::edgeParams);
    }

    /** The interactions of one sink batch: the edge each fired window flowed out over, plus the tick rows. */
    public int insertInteractions(List<Interaction> interactions) {
        return batch(INSERT_INTERACTION, interactions, ProvenanceRepository::interactionParams);
    }

    /** The states of one sink batch, which is where a fired window becomes a row. */
    public int insertStates(List<State> states) {
        return batch(INSERT_STATE, states, ProvenanceRepository::stateParams);
    }

    public List<Vertex> findVertices(String runId) {
        return sql.query(FIND_VERTICES, List.of(runId)).stream()
                .map(row -> new Vertex(row.getString(0), runId, row.getString(1), row.getString(2)))
                .toList();
    }

    public List<Edge> findEdges(String runId) {
        return sql.query(FIND_EDGES, List.of(runId)).stream()
                .map(row -> new Edge(runId, row.getString(0), row.getString(1)))
                .toList();
    }

    public List<Interaction> findInteractions(String runId) {
        return sql.query(FIND_INTERACTIONS, List.of(runId)).stream()
                .map(row -> new Interaction(runId, row.getString(0), row.getString(1), row.getString(2),
                        row.getLong(3), row.getLong(4), Attribution.fromJson(row.getString(5))))
                .toList();
    }

    public List<State> findStates(String runId) {
        return sql.query(FIND_STATES, List.of(runId)).stream()
                .map(row -> new State(runId, row.getString(0), row.getString(1), row.getLong(2),
                        row.getLong(3), row.getLong(4), Attribution.fromJson(row.getString(5)),
                        row.getLong(6)))
                .toList();
    }

    private static List<Object> vertexParams(Vertex vertex) {
        return List.of(vertex.vertexId(), vertex.runId(), vertex.vertexType(), vertex.description());
    }

    private static List<Object> edgeParams(Edge edge) {
        return List.of(edge.sourceVertex(), edge.targetVertex(), edge.runId());
    }

    /**
     * {@code Arrays.asList}, not {@code List.of}: the tag-blind rows -- {@code K1}'s and {@code M1}'s
     * boxes, and the {@code K1 -> M1} interaction -- have no hashtag to name, and {@code List.of} rejects a
     * null instead of storing the absence.
     */
    private static List<Object> interactionParams(Interaction interaction) {
        return Arrays.asList(interaction.runId(), interaction.sourceVertex(), interaction.targetVertex(),
                interaction.category(), interaction.timestamp(),
                Math.toIntExact(interaction.quantity()), interaction.attribution().toJson());
    }

    private static List<Object> stateParams(State state) {
        return Arrays.asList(state.runId(), state.vertexId(), state.category(), state.timeStart(),
                state.timeEnd(), Math.toIntExact(state.quantity()), state.attribution().toJson(),
                state.firedAt());
    }

    private <T> int batch(String insert, List<T> rows, Function<T, List<Object>> params) {
        if (rows.isEmpty()) {
            return 0;
        }
        return sql.insertBatch(insert, rows.stream().map(params).toList());
    }
}
