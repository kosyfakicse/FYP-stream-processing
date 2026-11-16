package com.fyp.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.fyp.RunContext;

class RunRepositoryTest {

    private static final Instant STARTED = Instant.parse("2026-11-14T10:20:30Z");
    private static final RunContext RUN =
            new RunContext("run-20261114-1020-a7f3", STARTED, "tweet_hashtag", "{\"pipeline\":{}}");

    private final RecordingExecutor sql = new RecordingExecutor();
    private final RunRepository repository = new RunRepository(sql);

    @Test
    void recordsTheRunBeforeTheJobStarts() {
        repository.recordStart(RUN);

        RecordingExecutor.Call call = sql.lastUpdate();
        assertThat(call.sql()).isEqualTo("INSERT INTO runs (run_id, started_at, dataset, config_json,"
                + " provenance_enabled) VALUES (?, ?, ?, ?, ?)");
        assertThat(call.args()).isEqualTo(List.of("run-20261114-1020-a7f3",
                RunRepository.toTimestamp(STARTED), "tweet_hashtag", "{\"pipeline\":{}}", false));
    }

    @Test
    void stampsFinishedAtAfterTheJobCompletes() {
        Instant finished = STARTED.plusSeconds(45);

        repository.recordFinish(RUN.runId(), finished);

        assertThat(sql.lastUpdate().sql())
                .isEqualTo("UPDATE runs SET finished_at = ? WHERE run_id = ?");
        assertThat(sql.lastUpdate().args()).isEqualTo(List.of(RunRepository.toTimestamp(finished),
                "run-20261114-1020-a7f3"));
    }

    @Test
    void readsTheRunBackAsAContext() {
        sql.queueRows(new Row(List.of("run-20261114-1020-a7f3", RunRepository.toTimestamp(STARTED),
                "tweet_hashtag", "{\"pipeline\":{}}")));

        Optional<RunContext> found = repository.findByRunId("run-20261114-1020-a7f3");

        assertThat(found).contains(RUN);
    }

    @Test
    void reportsAnUnknownRunIdAsAbsent() {
        assertThat(repository.findByRunId("run-19990101-0000-0000")).isEmpty();
    }

    @Test
    void rejectsATimestampColumnThatIsNotATimestamp() {
        sql.queueRows(new Row(List.of("run-1", "not a timestamp", "tweet_hashtag", "{}")));

        assertThat(org.assertj.core.api.Assertions.catchThrowableOfType(
                () -> repository.findByRunId("run-1"), IllegalStateException.class))
                .hasMessageContaining("Expected a timestamp");
    }
}
