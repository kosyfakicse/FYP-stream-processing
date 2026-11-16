package com.fyp;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

import com.fyp.config.AppConfig;
import com.fyp.config.ConfigLoader;

/**
 * Identifies one execution of the pipeline. Python read a static {@code run_id: "run-001"} out of
 * config.yaml; here the id is generated so two runs never share a key, and the same run tags every
 * row it writes.
 *
 * @param runId the generated identifier, eg {@code run-20261114-2258-a7f3}
 * @param startedAt when the run began
 * @param dataset what the run read: a {@code ds-...} upload id over HTTP, else the configured
 *        {@code target_dataset} name
 * @param configJson the config that produced it, stored verbatim in {@code runs.config_json}
 */
public record RunContext(String runId, Instant startedAt, String dataset, String configJson) {

    /** Minute resolution, so the id stays readable; the random suffix separates runs in one minute. */
    private static final DateTimeFormatter STAMP =
            DateTimeFormatter.ofPattern("'run-'uuuuMMdd-HHmm");

    /** The run the command line job makes: the dataset named in {@code config.yaml}. */
    public static RunContext start(AppConfig config) {
        return start(config, config.dataset().targetDataset());
    }

    /**
     * @param dataset what to record in {@code runs.dataset}, so a stored upload's run says which
     *        upload it read instead of naming a config entry that has nothing to do with it
     */
    public static RunContext start(AppConfig config, String dataset) {
        Instant now = Instant.now();
        return new RunContext(newRunId(now), now, dataset, ConfigLoader.toJson(config));
    }

    /**
     * Sortable by eye and unique enough to retry: the stamp is UTC so ids and stored timestamps
     * agree regardless of where the job ran.
     */
    public static String newRunId(Instant when) {
        String suffix = Integer.toHexString(ThreadLocalRandom.current().nextInt(0x1000, 0x10000));
        return STAMP.format(LocalDateTime.ofInstant(when, ZoneOffset.UTC)) + "-" + suffix;
    }
}
