package com.fyp.pipeline.loader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.fyp.model.Tweet;

/**
 * Reads the tweets CSV into {@link Tweet} rows.
 *
 * <p>Ports {@code DatasetLoader.read_csv} from
 * {@code legacy/src/fyp_attribution/pipeline/dataset_loader.py}, which returned a lazy generator;
 * the Java pipeline feeds a bounded Flink source, so the rows are returned as a list.
 */
public final class CsvDatasetLoader {

    private final Path csvPath;

    public CsvDatasetLoader(Path csvPath) {
        this.csvPath = csvPath;
    }

    public List<Tweet> readAll() {
        if (!Files.isRegularFile(csvPath)) {
            throw new IllegalStateException("Dataset not found: " + csvPath.toAbsolutePath().normalize());
        }
        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                throw new DatasetFormatException("Dataset is empty: " + csvPath);
            }
            requireHeader(header);

            List<Tweet> tweets = new ArrayList<>();
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                tweets.add(parseLine(line, lineNumber));
            }
            return tweets;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read dataset: " + csvPath, e);
        }
    }

    private static Tweet parseLine(String line, int lineNumber) {
        String[] fields = line.split(",", 3);
        if (fields.length < 3) {
            throw new DatasetFormatException(
                    "Expected user_id,text,timestamp at line " + lineNumber + " but got: " + line);
        }
        try {
            return new Tweet(fields[0], fields[1], Long.parseLong(fields[2].trim()));
        } catch (NumberFormatException e) {
            throw new DatasetFormatException(
                    "Timestamp is not a long at line " + lineNumber + ": " + line, e);
        }
    }

    /** Shared with {@link DatasetScan}, so an upload is accepted under exactly the rule the run enforces. */
    static void requireHeader(String header) {
        String[] fields = header.split(",");
        if (fields.length < 3 || !fields[0].equals("user_id") || !fields[1].equals("text")
                || !fields[2].equals("timestamp")) {
            throw new DatasetFormatException(
                    "Expected header user_id,text,timestamp but found: " + header);
        }
    }
}
