package com.fyp.config;

/**
 * Attribution and storage settings.
 *
 * @param windowSizeSeconds width of each tumbling window
 * @param batchSize window outputs buffered per DuckDB batch write
 * @param duckdbFile path to the DuckDB file, relative to the directory the job runs from
 */
public record PipelineConfig(int windowSizeSeconds, int batchSize, String duckdbFile) {
}
