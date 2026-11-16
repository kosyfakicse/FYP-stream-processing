package com.fyp.config;

/**
 * Root of the YAML tree loaded by {@link ConfigLoader}.
 *
 * @param dataset which input dataset to read
 * @param flink Flink job settings
 * @param pipeline attribution and storage settings
 */
public record AppConfig(DatasetConfig dataset, FlinkConfig flink, PipelineConfig pipeline) {
}
