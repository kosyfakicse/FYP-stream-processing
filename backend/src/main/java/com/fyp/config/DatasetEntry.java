package com.fyp.config;

/**
 * One entry of the dataset map.
 *
 * @param description human-readable note from the YAML
 * @param file path to the CSV, relative to the directory the job runs from
 */
public record DatasetEntry(String description, String file) {
}
