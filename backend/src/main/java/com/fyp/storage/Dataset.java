package com.fyp.storage;

import java.time.Instant;

/**
 * One uploaded csv: the file in the uploads directory plus the row that remembers it.
 *
 * @param datasetId the generated id, which is also the file name stem
 * @param uploadedAt when the bytes arrived
 * @param expiresAt when cleanup may delete the upload, null meaning keep it forever
 * @param originalName what the uploader called the file, display text and never a path
 * @param sizeBytes length of the stored file
 * @param rowCount data rows in it, counted when it was accepted
 */
public record Dataset(String datasetId, Instant uploadedAt, Instant expiresAt, String originalName,
        long sizeBytes, long rowCount) {
}
