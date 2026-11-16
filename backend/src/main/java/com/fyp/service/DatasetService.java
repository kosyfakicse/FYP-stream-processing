package com.fyp.service;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.fyp.config.AppConfig;
import com.fyp.storage.Dataset;
import com.fyp.storage.DatasetRepository;

/**
 * Everything the API is allowed to ask about uploads.
 *
 * <p>The layer above the repository exists for two reasons, not for decoration. It owns the clock:
 * {@code store} decides that an upload is accepted now, and {@link #collectExpired()} decides that
 * cleanup runs now, which is why {@link DatasetRepository} takes an {@link Instant} on every method
 * and can be tested without waiting a month. It is also where dataset policy will live -- refusing to
 * delete a dataset a run points at, or to run one that has expired -- before it spreads into a
 * controller.
 */
public final class DatasetService implements AutoCloseable {

    private final DatasetRepository datasets;

    public DatasetService(DatasetRepository datasets) {
        this.datasets = datasets;
    }

    /** The configured database and uploads directory, with the configured retention. */
    public static DatasetService fromConfig(AppConfig config) {
        return new DatasetService(DatasetRepository.fromConfig(config));
    }

    /**
     * Validates and keeps an upload.
     *
     * @param csvBytes the file as sent
     * @param clientName what the uploader called it, may be null, never used as a path
     * @throws com.fyp.pipeline.loader.DatasetFormatException if the bytes are not a usable tweets csv
     */
    public Dataset store(byte[] csvBytes, String clientName) {
        return datasets.store(csvBytes, clientName, Instant.now());
    }

    /** Newest first, which is the order an upload picker wants. */
    public List<Dataset> list() {
        return datasets.findAll();
    }

    public Optional<Dataset> find(String datasetId) {
        return datasets.find(datasetId);
    }

    /** The stored file, for a caller about to run the pipeline over it. Empty if it is not there. */
    public Optional<Path> pathOf(String datasetId) {
        return datasets.pathOf(datasetId);
    }

    public boolean delete(String datasetId) {
        return datasets.delete(datasetId);
    }

    /**
     * Collects uploads past their retention and returns how many went.
     *
     * <p>Nothing schedules this yet, and with {@code upload_ttl_days: 0} every upload is permanent so
     * it deletes nothing whatever the schedule says.
     */
    public int collectExpired() {
        return datasets.deleteExpired(Instant.now());
    }

    @Override
    public void close() {
        datasets.close();
    }
}
