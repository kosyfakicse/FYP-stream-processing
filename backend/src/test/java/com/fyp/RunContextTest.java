package com.fyp;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.fyp.config.ConfigLoader;

class RunContextTest {

    private static final Instant WHEN = Instant.parse("2026-11-14T10:20:30Z");

    @Test
    void formatsTheRunIdFromUtcTimePlusAFourDigitSuffix() {
        assertThat(RunContext.newRunId(WHEN)).matches("run-20261114-1020-[0-9a-f]{4}");
    }

    @Test
    void staysSortableByEyeAcrossDays() {
        assertThat(RunContext.newRunId(Instant.parse("2026-01-05T08:01:00Z")))
                .startsWith("run-20260105-0801-");
    }

    @Test
    void doesNotReuseAnIdWithinTheSameMinute() {
        long distinct = IntStream.range(0, 200)
                .mapToObj(i -> RunContext.newRunId(WHEN))
                .distinct()
                .count();

        assertThat(distinct).isGreaterThan(150);
    }

    @Test
    void snapshotsTheConfigThatProducedTheRun() {
        RunContext run = RunContext.start(ConfigLoader.load());

        assertThat(run.runId()).startsWith("run-");
        assertThat(run.startedAt()).isBeforeOrEqualTo(Instant.now());
        assertThat(run.dataset()).isEqualTo("tweet_hashtag");
        assertThat(run.configJson()).contains("\"target_dataset\":\"tweet_hashtag\"");
    }

    /**
     * A run over HTTP names the upload it read, and that id is what {@code runs.dataset} holds. The
     * config snapshot still carries {@code target_dataset}: it records the settings, not the input.
     */
    @Test
    void recordsTheDatasetItWasHandedInsteadOfTheConfiguredName() {
        RunContext run = RunContext.start(ConfigLoader.load(), "ds-20261116-0931-4e11");

        assertThat(run.dataset()).isEqualTo("ds-20261116-0931-4e11");
        assertThat(run.configJson()).contains("\"target_dataset\":\"tweet_hashtag\"");
    }
}
