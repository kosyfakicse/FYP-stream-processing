package com.fyp.pipeline.loader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fyp.model.Tweet;

class CsvDatasetLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    void readsRowsAndSkipsTheHeader() throws IOException {
        Path csv = write("user_id,text,timestamp\n"
                + "user_1,#UST #DATA,1793844000000\n"
                + "user_2,Who else uses #AI?,1793844002400\n");

        List<Tweet> tweets = new CsvDatasetLoader(csv).readAll();

        assertThat(tweets).containsExactly(
                new Tweet("user_1", "#UST #DATA", 1_793_844_000_000L),
                new Tweet("user_2", "Who else uses #AI?", 1_793_844_002_400L));
    }

    @Test
    void ignoresBlankLines() throws IOException {
        Path csv = write("user_id,text,timestamp\n\nuser_1,#AI,1000\n\n");

        assertThat(new CsvDatasetLoader(csv).readAll())
                .containsExactly(new Tweet("user_1", "#AI", 1_000L));
    }

    @Test
    void failsLoudlyWhenTheFileIsMissing() {
        Path missing = tempDir.resolve("nope.csv");

        assertThatThrownBy(() -> new CsvDatasetLoader(missing).readAll())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Dataset not found");
    }

    @Test
    void failsLoudlyOnAnUnexpectedHeader() throws IOException {
        Path csv = write("a,b,c\nuser_1,#AI,1000\n");

        assertThatThrownBy(() -> new CsvDatasetLoader(csv).readAll())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Expected header user_id,text,timestamp");
    }

    @Test
    void reportsTheLineNumberOfARowItCannotParse() throws IOException {
        Path csv = write("user_id,text,timestamp\nuser_1,#AI,not-a-number\n");

        assertThatThrownBy(() -> new CsvDatasetLoader(csv).readAll())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("line 2");
    }

    private Path write(String content) throws IOException {
        Path csv = tempDir.resolve("tweets.csv");
        Files.writeString(csv, content, StandardCharsets.UTF_8);
        return csv;
    }
}
