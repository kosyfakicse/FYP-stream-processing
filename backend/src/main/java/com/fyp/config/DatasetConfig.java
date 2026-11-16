package com.fyp.config;

import java.util.Locale;

/**
 * The dataset block: one target name plus one entry per supported dataset.
 *
 * <p>Python resolved the target with {@code getattr(self, target_dataset)}; a record has no such
 * escape hatch, so the mapping is written out.
 *
 * @param targetDataset which of the entries below to use
 * @param tweetHashtag generated tweets dataset
 * @param nycTaxi reserved, no file configured yet
 * @param nexmark reserved, no file configured yet
 */
public record DatasetConfig(String targetDataset, DatasetEntry tweetHashtag, DatasetEntry nycTaxi,
        DatasetEntry nexmark) {

    public DatasetEntry resolveTarget() {
        String key = targetDataset == null ? "" : targetDataset.replace('-', '_').toLowerCase(Locale.ROOT);
        DatasetEntry entry = switch (key) {
            case "tweet_hashtag" -> tweetHashtag;
            case "nyc_taxi" -> nycTaxi;
            case "nexmark" -> nexmark;
            default -> null;
        };
        if (entry == null) {
            throw new IllegalStateException("Dataset config not found: " + targetDataset);
        }
        if (entry.file() == null || entry.file().isBlank()) {
            throw new IllegalStateException("Dataset file path not set: " + targetDataset);
        }
        return entry;
    }
}
