package com.fyp.pipeline.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.fyp.model.Attribution;
import com.fyp.model.Interaction;
import com.fyp.model.ProvenanceRecord;
import com.fyp.model.RunVertices;
import com.fyp.model.State;
import com.fyp.model.UnifiedEvent;
import com.fyp.model.WindowOutput;
import com.fyp.pipeline.flink.CategoryAttributionFunction;
import com.fyp.storage.RecordingExecutor;

class DuckDbSinkWriterTest {

    private static final String WINDOW_STATEMENTS = "INSERT INTO window_outputs ";
    private static final String INTERACTION_STATEMENTS = "INSERT INTO tin_interactions ";
    private static final String STATE_STATEMENTS = "INSERT INTO tin_states ";

    private final RecordingExecutor sql = new RecordingExecutor();

    @Test
    void writesAWholeBatchTheMomentItFills() throws Exception {
        try (DuckDbSinkWriter writer = new DuckDbSinkWriter(sql, chain("run-20261114-1836-a7f3"),
                100)) {
            IntStream.range(0, 250).forEach(i -> writeOne(writer, i));

            assertThat(batchesOf(WINDOW_STATEMENTS)).hasSize(2);
            assertThat(rowsOf(WINDOW_STATEMENTS)).hasSize(200);
            assertThat(writer.written()).isEqualTo(200);
        }
        assertThat(rowsOf(WINDOW_STATEMENTS)).hasSize(250);
        assertThat(sql.closed).isTrue();
    }

    @Test
    void flushEndsTheCurrentBatchAndLeavesNothingBehind() throws Exception {
        try (DuckDbSinkWriter writer = new DuckDbSinkWriter(sql, chain("run-1"), 100)) {
            IntStream.range(0, 3).forEach(i -> writeOne(writer, i));
            writer.flush(true);

            assertThat(batchesOf(WINDOW_STATEMENTS)).hasSize(1);
            assertThat(rowsOf(WINDOW_STATEMENTS)).hasSize(3);

            writer.flush(false);
            assertThat(batchesOf(WINDOW_STATEMENTS)).hasSize(1);
            assertThat(batchesOf(STATE_STATEMENTS)).hasSize(1);
            assertThat(batchesOf(INTERACTION_STATEMENTS)).hasSize(1);
        }
    }

    @Test
    void tagsEveryRowWithTheRunIdItWasCreatedFor() throws Exception {
        try (DuckDbSinkWriter writer = new DuckDbSinkWriter(sql, chain("run-42"), 2)) {
            writeOne(writer, 0);
            writeOne(writer, 1);

            List<?> rows = batchesOf(WINDOW_STATEMENTS).get(0).args();
            assertThat(rows).hasSize(2);
            assertThat(((List<?>) rows.get(0)).subList(0, 3)).isEqualTo(List.of("run-42", "W1", "AI"));
        }
    }

    /** One flush is one event for every table, so a result, its interactions and its state cannot disagree. */
    @Test
    void writesTheWindowOperatorStateForEveryResultItStores() throws Exception {
        try (DuckDbSinkWriter writer = new DuckDbSinkWriter(sql, chain("run-42"), 1)) {
            writeOne(writer, 7);

            assertThat(rowsOf(STATE_STATEMENTS)).containsExactly(
                    List.of("run-42", "W1", "AI", 7L, 8L, 1, "{\"user_1\":1}", 8L));
        }
    }

    /**
     * The firing describes one edge only -- what left the window for the sink -- because that is the only
     * interaction an operator can observe about itself. The other two come in as tick records from their own
     * vertices, so the writer must not invent them from the window's element list again.
     */
    @Test
    void writesTheSinkInteractionOfEveryResultWithTheVertexThatPassedIt() throws Exception {
        try (DuckDbSinkWriter writer = new DuckDbSinkWriter(sql, chain("run-42"), 1)) {
            writer.write(ProvenanceRecord.ofFiring(firingOf(List.of(
                    UnifiedEvent.of("user_1", "AI", 7_000L),
                    UnifiedEvent.of("user_1", "AI", 7_000L),
                    UnifiedEvent.of("user_2", "AI", 7_500L)))), null);

            assertThat(rowsOf(INTERACTION_STATEMENTS)).containsExactly(
                    List.of("run-42", "W1", "S1", "AI", 8_000L, 3, "{\"W1\":3}"));
        }
    }

    /**
     * A tick record is already a row, so it is stored as it arrived and drags no result behind it. This
     * is the path the per-vertex counts take once they are being taken.
     */
    @Test
    void storesATickRecordAsTheRowItDescribesAndWritesNothingElse() throws Exception {
        try (DuckDbSinkWriter writer = new DuckDbSinkWriter(sql, chain("run-42"), 100)) {
            writer.write(ProvenanceRecord.ofState(new State("run-42", "K1", null, 1_000L, 2_000L,
                    4L, new Attribution(Map.of()), 2_000L)), null);
            writer.write(ProvenanceRecord.ofInteraction(new Interaction("run-42", "K1", "M1", null,
                    2_000L, 4L, new Attribution(Map.of("K1", 4L)))), null);
            writer.flush(true);

            assertThat(rowsOf(WINDOW_STATEMENTS)).isEmpty();
            assertThat(rowsOf(STATE_STATEMENTS)).containsExactly(
                    Arrays.asList("run-42", "K1", null, 1_000L, 2_000L, 4, "{}", 2_000L));
            assertThat(rowsOf(INTERACTION_STATEMENTS)).containsExactly(
                    Arrays.asList("run-42", "K1", "M1", null, 2_000L, 4, "{\"K1\":4}"));
        }
    }

    private static RunVertices chain(String runId) {
        return new RunVertices(runId, "K1", "M1", "W1", "S1");
    }

    private static WindowOutput firingOf(List<UnifiedEvent> events) {
        return CategoryAttributionFunction.aggregate("AI", 7_000L, 8_000L, events);
    }

    /** One tweet, one event, so the two counts agree and only the ids are under test. */
    private static WindowOutput oneEventIn(long windowStart) {
        return new WindowOutput(windowStart, windowStart + 1, "AI", 1L,
                new Attribution(Map.of("user_1", 1L)));
    }

    private void writeOne(DuckDbSinkWriter writer, int index) {
        try {
            writer.write(ProvenanceRecord.ofFiring(oneEventIn(index)), null);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** One flush sends one batch per table, so a test has to name the statement it means. */
    private List<RecordingExecutor.Call> batchesOf(String insertPrefix) {
        return sql.batches.stream().filter(call -> call.sql().startsWith(insertPrefix)).toList();
    }

    private List<List<?>> rowsOf(String insertPrefix) {
        List<List<?>> rows = new ArrayList<>();
        batchesOf(insertPrefix).forEach(call -> call.args().forEach(row -> rows.add((List<?>) row)));
        return rows;
    }
}
