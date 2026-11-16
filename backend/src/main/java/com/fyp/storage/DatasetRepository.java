package com.fyp.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fyp.config.AppConfig;
import com.fyp.pipeline.loader.DatasetScan;

/**
 * Owns the uploads directory and the {@code datasets} rows that describe it.
 *
 * <p>The dataset counterpart of {@link RunRepository}, with one duty it does not have: an upload is
 * two artifacts, a file and a row, so this class keeps them in step and no caller has to learn where
 * the bytes live. A caller that runs a stored dataset asks {@link #pathOf} and hands the result to
 * {@code AttributionService.run}, which is what {@code POST /runs} does with the id in its body.
 *
 * <p>It opens its own connection to the same DuckDB file rather than borrowing the pipeline's, which
 * is what {@code DuckDbSink} already does. That is what makes one lock here enough: the connection it
 * guards belongs to this object alone.
 */
public final class DatasetRepository implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetRepository.class);

    /** Shared by every select so a row and {@link #toDataset} cannot drift apart. */
    private static final String COLUMNS = "dataset_id, uploaded_at, expires_at, original_name,"
            + " size_bytes, row_count";

    private static final String INSERT_DATASET =
            "INSERT INTO datasets (dataset_id, uploaded_at, expires_at, original_name, size_bytes,"
                    + " row_count) VALUES (?, ?, ?, ?, ?, ?)";

    private static final String FIND_DATASET =
            "SELECT " + COLUMNS + " FROM datasets WHERE dataset_id = ?";

    private static final String LIST_DATASETS =
            "SELECT " + COLUMNS + " FROM datasets ORDER BY uploaded_at DESC";

    private static final String FIND_EXPIRED = "SELECT dataset_id FROM datasets"
            + " WHERE expires_at IS NOT NULL AND expires_at <= ?";

    private static final String DELETE_DATASET = "DELETE FROM datasets WHERE dataset_id = ?";

    /** The id doubles as a file name, so the alphabet is lowercase, digits and dashes only. */
    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("'ds-'uuuuMMdd-HHmm");

    private static final String ID_PATTERN = "ds-\\d{8}-\\d{4}-[0-9a-f]{4}";

    private final SqlExecutor sql;
    private final Path uploadDir;
    private final Duration retention;

    private final Object connectionLock = new Object();

    public DatasetRepository(SqlExecutor sql, Path uploadDir, Duration retention) {
        this.sql = sql;
        this.uploadDir = uploadDir;
        this.retention = retention;
    }

    /** The DuckDB file and uploads directory named in the config, with the configured retention. */
    public static DatasetRepository fromConfig(AppConfig config) {
        String url = DuckDbExecutor.urlFor(Path.of(config.pipeline().duckdbFile()));
        return new DatasetRepository(new DuckDbExecutor(url), Path.of(config.dataset().uploadsDir()),
                Duration.ofDays(config.dataset().uploadTtlDays()));
    }

    /**
     * Validates the bytes, writes them under a fresh id and records the row.
     *
     * <p>The scan happens before anything is written, so a rejected upload leaves nothing behind. A
     * failure after that point removes the file again, because a dataset no row names would occupy
     * the directory forever.
     *
     * @param csvBytes the upload, stored verbatim
     * @param clientName what the uploader called it; may be null, and is never used as a path
     * @param uploadedAt starts the retention period
     * @throws com.fyp.pipeline.loader.DatasetFormatException if the bytes are not a usable tweets csv
     */
    public Dataset store(byte[] csvBytes, String clientName, Instant uploadedAt) {
        long rowCount = DatasetScan.rowCount(csvBytes);
        String datasetId = newDatasetId(uploadedAt);
        Instant expiresAt = retention.isZero() || retention.isNegative() ? null : uploadedAt.plus(retention);
        Dataset dataset = new Dataset(datasetId, uploadedAt, expiresAt, displayName(clientName, datasetId),
                csvBytes.length, rowCount);

        synchronized (connectionLock) {
            SchemaInitializer.applyTo(sql);
            writeFile(fileFor(datasetId), csvBytes);
            try {
                sql.update(INSERT_DATASET, Arrays.asList(datasetId, RunRepository.toTimestamp(uploadedAt),
                        expiresAt == null ? null : RunRepository.toTimestamp(expiresAt),
                        dataset.originalName(), (long) csvBytes.length, rowCount));
            } catch (RuntimeException storageFailure) {
                deleteQuietly(fileFor(datasetId));
                throw storageFailure;
            }
        }
        return dataset;
    }

    public Optional<Dataset> find(String datasetId) {
        synchronized (connectionLock) {
            return sql.query(FIND_DATASET, List.of(datasetId)).stream().findFirst().map(DatasetRepository::toDataset);
        }
    }

    /** Newest first, which is the order an upload picker wants. */
    public List<Dataset> findAll() {
        synchronized (connectionLock) {
            return sql.query(LIST_DATASETS, List.of()).stream().map(DatasetRepository::toDataset).toList();
        }
    }

    /**
     * Where the bytes are, for a caller about to run the pipeline over them.
     *
     * <p>Empty for an unknown id and for a row whose file has gone, which is the one way the file and
     * the row can disagree: a hand-cleaned directory.
     */
    public Optional<Path> pathOf(String datasetId) {
        return find(datasetId).map(dataset -> fileFor(dataset.datasetId())).filter(Files::isRegularFile);
    }

    /** Forget an upload now instead of waiting for its expiry. False means no such id. */
    public boolean delete(String datasetId) {
        synchronized (connectionLock) {
            Path file = fileFor(datasetId);
            int removed = sql.update(DELETE_DATASET, List.of(datasetId));
            deleteQuietly(file);
            return removed > 0;
        }
    }

    /**
     * Deletes every upload whose retention has run out and returns how many went.
     *
     * <p>Nothing calls this on a schedule yet, which is why it takes {@code now} instead of reading
     * the clock: a cron entry, a scheduled task and a test can all drive the same method without one
     * of them waiting a month. Rows with a null {@code expires_at} are kept forever, and the filter
     * skips them in SQL rather than here.
     */
    public int deleteExpired(Instant now) {
        synchronized (connectionLock) {
            List<String> expired = sql.query(FIND_EXPIRED, List.of(RunRepository.toTimestamp(now))).stream()
                    .map(row -> row.getString(0)).toList();
            for (String datasetId : expired) {
                sql.update(DELETE_DATASET, List.of(datasetId));
                deleteQuietly(fileFor(datasetId));
            }
            if (!expired.isEmpty()) {
                LOG.info("collected {} expired dataset(s) before {}", expired.size(), now);
            }
            return expired.size();
        }
    }

    @Override
    public void close() {
        sql.close();
    }

    /** Ids are generated here and read back from our own table, so anything else is not one. */
    private Path fileFor(String datasetId) {
        if (datasetId == null || !datasetId.matches(ID_PATTERN)) {
            throw new IllegalArgumentException("Not a dataset id: " + datasetId);
        }
        return uploadDir.resolve(datasetId + ".csv");
    }

    private void writeFile(Path target, byte[] csvBytes) {
        try {
            Files.createDirectories(uploadDir);
            Files.write(target, csvBytes);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write the upload to " + target, e);
        }
    }

    private static void deleteQuietly(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            LOG.warn("upload left behind: {}", file);
        }
    }

    /** Strips any directory the client sent, so {@code ../../etc/passwd} is stored as {@code passwd}. */
    private static String displayName(String clientName, String datasetId) {
        if (clientName == null || clientName.isBlank()) {
            return datasetId + ".csv";
        }
        int slash = Math.max(clientName.lastIndexOf('/'), clientName.lastIndexOf('\\'));
        String leaf = slash < 0 ? clientName : clientName.substring(slash + 1);
        return leaf.isBlank() ? datasetId + ".csv" : leaf;
    }

    /** Minute resolution keeps ids readable; the suffix separates two uploads inside one minute. */
    static String newDatasetId(Instant when) {
        String suffix = Integer.toHexString(ThreadLocalRandom.current().nextInt(0x1000, 0x10000));
        return STAMP.format(LocalDateTime.ofInstant(when, ZoneOffset.UTC)) + "-" + suffix;
    }

    private static Dataset toDataset(Row row) {
        Object expiresAt = row.getObject(2);
        return new Dataset(row.getString(0), RunRepository.toInstant(row.getObject(1)),
                expiresAt == null ? null : RunRepository.toInstant(expiresAt), row.getString(3),
                row.getLong(4), row.getLong(5));
    }
}
