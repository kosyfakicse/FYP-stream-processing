package com.fyp.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.time.Instant;
import java.util.Arrays;
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
    void findsARecordedRunWithItsFinishTime() {
        Instant finished = STARTED.plusSeconds(2);
        sql.queueRows(new Row(List.of("run-20261114-1020-a7f3", RunRepository.toTimestamp(STARTED),
                RunRepository.toTimestamp(finished), "ds-20261116-0931-4e11")));

        assertThat(repository.find("run-20261114-1020-a7f3")).contains(
                new Run("run-20261114-1020-a7f3", STARTED, finished, "ds-20261116-0931-4e11"));
        assertThat(sql.lastQuery().sql())
                .isEqualTo("SELECT run_id, started_at, finished_at, dataset FROM runs WHERE run_id = ?");
    }

    /** The null is the whole difference between a run that finished and one that died halfway. */
    @Test
    void anUnfinishedRunReadsWithoutAFinishTime() {
        sql.queueRows(new Row(Arrays.asList("run-20261114-1020-a7f3",
                RunRepository.toTimestamp(STARTED), null, "ds-20261116-0931-4e11")));

        Run recorded = repository.find("run-20261114-1020-a7f3").orElseThrow();

        assertThat(recorded.finishedAt()).isNull();
        assertThat(recorded.startedAt()).isEqualTo(STARTED);
    }

    @Test
    void listsEveryRunTheTableHolds() {
        sql.queueRows(new Row(List.of("run-20261116-0000-0002", RunRepository.toTimestamp(STARTED),
                        RunRepository.toTimestamp(STARTED.plusSeconds(1)), "ds-20261116-0931-4e11")),
                new Row(List.of("run-20261116-0000-0001", RunRepository.toTimestamp(STARTED),
                        RunRepository.toTimestamp(STARTED.plusSeconds(1)), "tweet_hashtag")));

        assertThat(repository.findAll()).extracting(Run::runId)
                .containsExactly("run-20261116-0000-0002", "run-20261116-0000-0001");
        assertThat(sql.lastQuery().sql()).isEqualTo("SELECT run_id, started_at, finished_at, dataset"
                + " FROM runs ORDER BY started_at DESC, run_id DESC");
    }

    @Test
    void anEmptyTableListsNothing() {
        assertThat(repository.find("run-19990101-0000-0000")).isEmpty();
        assertThat(repository.findAll()).isEmpty();
    }

    @Test
    void rejectsATimestampColumnThatIsNotATimestamp() {
        sql.queueRows(new Row(List.of("run-1", "not a timestamp", "tweet_hashtag", "{}")));

        assertThat(org.assertj.core.api.Assertions.catchThrowableOfType(
                () -> repository.findByRunId("run-1"), IllegalStateException.class))
                .hasMessageContaining("Expected a timestamp");
    }
}
