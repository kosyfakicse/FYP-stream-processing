package com.fyp.config;

/**
 * Flink job settings.
 *
 * <p>Python also carried {@code mode} and {@code checkpoint_interval}; neither was read by the
 * pipeline, so they are dropped rather than carried as decoration.
 *
 * @param parallelism job-wide parallelism
 */
public record FlinkConfig(int parallelism) {
}
