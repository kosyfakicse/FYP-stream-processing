package com.fyp.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ConfigLoaderTest {

    @Test
    void loadsTheWholeTreeFromTheClasspath() {
        AppConfig config = ConfigLoader.load("config-test.yaml");

        assertThat(config.dataset().targetDataset()).isEqualTo("nexmark");
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

    @Test
    void acceptsHyphenatedTargetNamesLikePythonDid() {
        DatasetConfig config = new DatasetConfig("tweet-hashtag",
                new DatasetEntry("tweets", "data/raw/tweet_hashtag.csv"), null, null);

        assertThat(config.resolveTarget().file()).isEqualTo("data/raw/tweet_hashtag.csv");
    }

    @Test
    void failsWhenTheTargetDatasetHasNoFile() {
        DatasetConfig config = new DatasetConfig("nexmark", null, null,
                new DatasetEntry("Nexmark benchmark (future)", null));

        assertThatThrownBy(config::resolveTarget)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Dataset file path not set: nexmark");
    }

    @Test
    void failsWhenTheTargetDatasetIsUnknown() {
        DatasetConfig config = new DatasetConfig("stock-prices", null, null, null);

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
