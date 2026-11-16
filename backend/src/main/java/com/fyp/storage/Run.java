package com.fyp.storage;

import java.time.Instant;

/**
 * A run as the {@code runs} table recorded it.
 *
 * <p>The read counterpart of {@link com.fyp.RunContext}. That one is what a job is about to do and
 * carries the config snapshot; this one is what a caller asks about afterwards, so it carries
 * {@code finished_at} and not the config.
 *
 * @param runId the id every result row is stamped with
 * @param startedAt when the run row was written, before the dataset was read
 * @param finishedAt when the job finished, null while it has not -- a failed or in-flight run
 * @param dataset the {@code ds-...} upload id an HTTP run was given, else the configured name
 */
public record Run(String runId, Instant startedAt, Instant finishedAt, String dataset) {
}
