package com.fyp.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ConfigLoaderTest {

    @Test
    void loadsTheWholeTreeFromTheClasspath() {
        AppConfig config = ConfigLoader.load("config-test.yaml");

        assertThat(config.dataset().targetDataset()).isEqualTo("nexmark");
        assertThat(config.dataset().uploadsDir()).isEqualTo("data/uploads");
        assertThat(config.dataset().uploadTtlDays()).isEqualTo(14);
        assertThat(config.flink().parallelism()).isEqualTo(3);
        assertThat(config.pipeline().windowSizeSeconds()).isEqualTo(15);
        assertThat(config.pipeline().batchSize()).isEqualTo(7);
        assertThat(config.pipeline().duckdbFile()).isEqualTo("data/test.duckdb");
    }

    @Test
    void ignoresKeysItDoesNotModelYet() {
        assertThat(ConfigLoader.load("config-test.yaml").pipeline().batchSize()).isEqualTo(7);
    }

    @Test
    void resolvesTheTargetDatasetToItsFile() {
        AppConfig config = ConfigLoader.load();

        DatasetEntry entry = config.dataset().resolveTarget();

        assertThat(entry.file()).endsWith("tweet_hashtag.csv");
        assertThat(entry.description()).isNotBlank();
    }

    /**
     * The keys {@code ApiApplication} needs at startup, checked against the shipped file rather than
     * the test one: a missing {@code uploads_dir} binds to null and the context dies in
     * {@code Path.of(null)}, which is not an obvious failure to read from the other end.
     */
    @Test
    void shipsAnUploadsDirectoryAndKeepsUploadsForever() {
        DatasetConfig config = ConfigLoader.load().dataset();

        assertThat(config.uploadsDir()).isNotBlank();
        assertThat(config.uploadTtlDays()).isZero();
    }

    @Test
    void acceptsHyphenatedTargetNamesLikePythonDid() {
        DatasetConfig config = new DatasetConfig("tweet-hashtag", "data/uploads", 0,
                new DatasetEntry("tweets", "data/raw/tweet_hashtag.csv"), null, null);

        assertThat(config.resolveTarget().file()).isEqualTo("data/raw/tweet_hashtag.csv");
    }

    @Test
    void failsWhenTheTargetDatasetHasNoFile() {
        DatasetConfig config = new DatasetConfig("nexmark", "data/uploads", 0, null, null,
                new DatasetEntry("Nexmark benchmark (future)", null));

        assertThatThrownBy(config::resolveTarget)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Dataset file path not set: nexmark");
    }

    @Test
    void failsWhenTheTargetDatasetIsUnknown() {
        DatasetConfig config = new DatasetConfig("stock-prices", "data/uploads", 0, null, null, null);

        assertThatThrownBy(config::resolveTarget)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Dataset config not found: stock-prices");
    }

    @Test
    void failsLoudlyWhenTheResourceIsMissing() {
        assertThatThrownBy(() -> ConfigLoader.load("no-such-config.yaml"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no-such-config.yaml");
    }

    @Test
    void serialisesTheConfigSnapshotWithSnakeCaseKeys() {
        String json = ConfigLoader.toJson(ConfigLoader.load());

        assertThat(json)
                .contains("\"target_dataset\":\"tweet_hashtag\"")
                .contains("\"window_size_seconds\"")
                .contains("\"batch_size\"")
                .doesNotContain("targetDataset");
    }
}
