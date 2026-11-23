package com.fyp.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fyp.model.Attribution;
import com.fyp.model.Edge;
import com.fyp.model.Interaction;
import com.fyp.model.State;
import com.fyp.model.Vertex;

/**
 * The {@code tin_*} tables against a real DuckDB, because the two claims worth proving about them are
 * storage claims: that the id sequences fill themselves, and that a second run can mint its own W1.
 */
class ProvenanceRepositoryTest {

    private static final Vertex K1 = new Vertex("K1", "run-a", "source", "csv dataset tweet_hashtag");
    private static final Vertex W1 = new Vertex("W1", "run-a", "window", "60s tumbling count");
    private static final Edge K1_TO_W1 = new Edge("run-a", "K1", "W1");
    private static final State PYTHON = new State("run-a", "W1", "PYTHON", 1_000L, 2_000L, 3L,
            new Attribution(Map.of("user_1", 1L, "user_2", 2L)), 2_000L);
    private static final State JAVA = new State("run-a", "W1", "JAVA", 1_000L, 2_000L, 1L,
            new Attribution(Map.of("user_3", 1L)), 2_000L);
    private static final Attribution PYTHON_TOTALS =
            new Attribution(Map.of("user_1", 1L, "user_2", 2L));
    private static final Interaction PYTHON_IN = new Interaction("run-a", "K1", "M1", "PYTHON",
            2_000L, 2L, new Attribution(Map.of("user_1", 1L, "user_2", 1L)));
    private static final Interaction PYTHON_OUT = new Interaction("run-a", "M1", "W1", "PYTHON",
            2_000L, 3L, PYTHON_TOTALS);

    private DuckDbExecutor executor;
    private ProvenanceRepository provenance;

    @BeforeEach
    void createDatabase() {
        executor = DuckDbExecutor.inMemory();
        SchemaInitializer.applyTo(executor);
        provenance = new ProvenanceRepository(executor);
    }

    @AfterEach
    void closeDatabase() {
        executor.close();
    }

    @Test
    void storesTheTopologyAndReadsBackOnlyTheRunAskedFor() {
        provenance.insertVertices(List.of(K1, W1));
        provenance.insertEdges(List.of(K1_TO_W1));
        Vertex otherRun = new Vertex("W1", "run-b", "window", "the same id, because ids are per run");
        provenance.insertVertices(List.of(otherRun));

        assertThat(provenance.findVertices("run-a")).containsExactly(K1, W1);
        assertThat(provenance.findEdges("run-a")).containsExactly(K1_TO_W1);
        assertThat(provenance.findVertices("run-b")).containsExactly(otherRun);
        assertThat(provenance.findEdges("run-b")).isEmpty();
    }

    @Test
    void fillsStateIdsFromItsSequenceAndKeepsOneRowPerCategoryAndWindow() {
        provenance.insertVertices(List.of(W1));

        assertThat(provenance.insertStates(List.of(PYTHON, JAVA))).isEqualTo(2);
        assertThat(executor.query("SELECT state_id FROM tin_states ORDER BY state_id", List.of()))
                .extracting(row -> row.getLong(0)).containsExactly(1L, 2L);
        assertThat(provenance.findStates("run-a")).containsExactly(JAVA, PYTHON);
    }

    @Test
    void numbersEachInteractionOfTheChainAndReadsThemBackInOrder() {
        assertThat(provenance.insertInteractions(List.of(PYTHON_OUT, PYTHON_IN))).isEqualTo(2);
        assertThat(executor.query("SELECT interaction_id FROM tin_interactions"
                + " ORDER BY interaction_id", List.of()))
                .extracting(row -> row.getLong(0)).containsExactly(1L, 2L);
        assertThat(provenance.findInteractions("run-a")).containsExactly(PYTHON_IN, PYTHON_OUT);
    }

    /**
     * The sink is at-least-once, so a retry has to collide with the window it is repeating rather than
     * land as a second row that makes the same window look like it fired twice.
     */
    @Test
    void refusesToStoreTheSameWindowStateTwice() {
        provenance.insertVertices(List.of(W1));
        provenance.insertStates(List.of(PYTHON));

        assertThatThrownBy(() -> provenance.insertStates(List.of(PYTHON)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tin_states");
        assertThat(provenance.findStates("run-a")).containsExactly(PYTHON);
    }

    /** The same retry rule on the interaction table: one edge, one category, one firing is one row. */
    @Test
    void refusesToStoreTheSameInteractionTwice() {
        provenance.insertInteractions(List.of(PYTHON_OUT));

        assertThatThrownBy(() -> provenance.insertInteractions(List.of(PYTHON_OUT)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tin_interactions");
        assertThat(provenance.findInteractions("run-a")).containsExactly(PYTHON_OUT);
    }

    /**
     * What a tag-blind row costs, measured rather than assumed. {@code K1}'s and {@code M1}'s boxes are
     * not about one hashtag, so their category is null -- and DuckDB treats two nulls as distinct, so the
     * unique key lets a repeated row through instead of colliding. The at-least-once guard therefore
     * covers the category-bearing rows only. This test asserts the hole so it fails loudly the day the
     * driver starts enforcing {@code NULLS NOT DISTINCT}, which it currently cannot even parse.
     */
    @Test
    void storesAnUnkeyedStateTwiceBecauseNullCategoriesNeverCollide() {
        State rowsRead = new State("run-a", "K1", null, 1_000L, 2_000L, 4L, new Attribution(Map.of()),
                2_000L);

        assertThat(provenance.insertStates(List.of(rowsRead))).isEqualTo(1);
        assertThat(provenance.insertStates(List.of(rowsRead))).isEqualTo(1);
        assertThat(provenance.findStates("run-a")).containsExactly(rowsRead, rowsRead);
    }

    @Test
    void storesAnUnkeyedInteractionTwiceForTheSameReason() {
        Interaction tweetsIn = new Interaction("run-a", "K1", "M1", null, 2_000L, 4L,
                new Attribution(Map.of("K1", 4L)));

        assertThat(provenance.insertInteractions(List.of(tweetsIn))).isEqualTo(1);
        assertThat(provenance.insertInteractions(List.of(tweetsIn))).isEqualTo(1);
        assertThat(provenance.findInteractions("run-a")).containsExactly(tweetsIn, tweetsIn);
    }

    @Test
    void issuesNoStatementForAnEmptyBatch() {
        RecordingExecutor sql = new RecordingExecutor();
        ProvenanceRepository repository = new ProvenanceRepository(sql);

        assertThat(repository.insertVertices(List.of())).isZero();
        assertThat(repository.insertEdges(List.of())).isZero();
        assertThat(repository.insertInteractions(List.of())).isZero();
        assertThat(repository.insertStates(List.of())).isZero();
        assertThat(sql.batches).isEmpty();
    }
}
