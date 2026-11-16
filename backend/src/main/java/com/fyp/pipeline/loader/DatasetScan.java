package com.fyp.pipeline.loader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * One pass over an upload that builds no rows: is it a tweets csv, and how many records does it hold.
 *
 * <p>Separate from {@link CsvDatasetLoader} because an upload is checked and then put away unread.
 * Materialising a million {@link com.fyp.model.Tweet} objects to answer "is this file usable" is
 * exactly the cost this avoids.
 */
public final class DatasetScan {

    private DatasetScan() {
    }

    /**
     * Counts the data rows, rejecting a file with no header, no rows, or a malformed line.
     *
     * <p>The ordering rule is not a nicety. The pipeline assigns watermarks with
     * {@code forMonotonousTimestamps()}, which treats every event older than the last one seen as late
     * and drops it without a word -- measured at 73,613 events counted from a million tweets. Refusing
     * here turns that silent loss into a message naming the line.
     *
     * @param csv the bytes as uploaded
     * @return the number of data rows, always at least one
     * @throws DatasetFormatException if the bytes are not a usable, ordered tweets csv
     */
    public static long rowCount(byte[] csv) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new ByteArrayInputStream(csv), StandardCharsets.UTF_8))) {
            String header = reader.readLine();
            if (header == null) {
                throw new DatasetFormatException("Dataset is empty");
            }
            CsvDatasetLoader.requireHeader(header);

            long rows = 0;
            long previous = Long.MIN_VALUE;
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                rows++;
                long timestamp = timestampAt(line, lineNumber);
                if (timestamp < previous) {
                    throw new DatasetFormatException("Dataset is not sorted by timestamp at line "
                            + lineNumber + ": " + timestamp + " comes after " + previous);
                }
                previous = timestamp;
            }
            if (rows == 0) {
                throw new DatasetFormatException("Dataset has no rows, only the header");
            }
            return rows;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read the uploaded dataset", e);
        }
    }

    /** Third field to end of line, which is the same slice {@code split(",", 3)} would give. */
    private static long timestampAt(String line, int lineNumber) {
        int firstComma = line.indexOf(',');
        int secondComma = firstComma < 0 ? -1 : line.indexOf(',', firstComma + 1);
        if (secondComma < 0) {
            throw new DatasetFormatException(
                    "Expected user_id,text,timestamp at line " + lineNumber + " but got: " + line);
        }
        try {
            return Long.parseLong(line.substring(secondComma + 1).trim());
        } catch (NumberFormatException e) {
            throw new DatasetFormatException(
                    "Timestamp is not a long at line " + lineNumber + ": " + line, e);
        }
    }
}
