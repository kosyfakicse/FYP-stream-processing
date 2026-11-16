package com.fyp.pipeline.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.fyp.model.Attribution;
import com.fyp.model.WindowOutput;
import com.fyp.storage.RecordingExecutor;

class DuckDbSinkWriterTest {

    private final RecordingExecutor sql = new RecordingExecutor();

    @Test
    void writesAWholeBatchTheMomentItFills() throws Exception {
        try (DuckDbSinkWriter writer = new DuckDbSinkWriter(sql, "run-20261114-1836-a7f3", 100)) {
            IntStream.range(0, 250).forEach(i -> writeOne(writer, i));

            assertThat(sql.batches).hasSize(2);
            assertThat(sql.allBatchedRows()).hasSize(200);
            assertThat(writer.written()).isEqualTo(200);
        }
        assertThat(sql.allBatchedRows()).hasSize(250);
        assertThat(sql.closed).isTrue();
    }

    @Test
    void flushEndsTheCurrentBatchAndLeavesNothingBehind() throws Exception {
        try (DuckDbSinkWriter writer = new DuckDbSinkWriter(sql, "run-1", 100)) {
            IntStream.range(0, 3).forEach(i -> writeOne(writer, i));
            writer.flush(true);

            assertThat(sql.batches).hasSize(1);
            assertThat(sql.allBatchedRows()).hasSize(3);

            writer.flush(false);
            assertThat(sql.batches).hasSize(1);
        }
    }

    @Test
    void tagsEveryRowWithTheRunIdItWasCreatedFor() throws Exception {
        try (DuckDbSinkWriter writer = new DuckDbSinkWriter(sql, "run-42", 2)) {
            writeOne(writer, 0);
            writeOne(writer, 1);

            List<?> rows = sql.lastBatch().args();
            assertThat(rows).hasSize(2);
            assertThat(((List<?>) rows.get(0)).subList(0, 3)).isEqualTo(List.of("run-42", "W1", "AI"));
        }
    }

    private static void writeOne(DuckDbSinkWriter writer, int index) {
        try {
            writer.write(new WindowOutput(index, index + 1, "AI", 1L,
                    new Attribution(Map.of("user_1", 1L))), null);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
