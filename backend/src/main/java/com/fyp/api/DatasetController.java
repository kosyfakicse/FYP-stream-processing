package com.fyp.api;

import java.net.URI;
import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fyp.pipeline.loader.DatasetFormatException;
import com.fyp.service.DatasetService;
import com.fyp.storage.Dataset;


@RestController
public class DatasetController {
    // Constants
    private static final Logger LOG = LoggerFactory.getLogger(DatasetController.class);

    // The upload endpoint takes the file itself: raw bytes, never a form.
    private static final String SEND_BYTES_MESSAGE =
            "send the raw csv bytes with Content-Type: text/csv or application/octet-stream";

    // Data member
    public record DatasetResponse(String datasetId, String originalName, Instant uploadedAt,
            Instant expiresAt, long sizeBytes, long rowCount){}

    public record ErrorResponse(String error){}

    // Singleton Service
    private final DatasetService service;

    // Constructor
    public DatasetController(DatasetService service){
        this.service = service;
    }

    // API Endpoints
    // 1. Post for submit a dataset
    @PostMapping(value = "/datasets", consumes = MediaType.ALL_VALUE)
    public ResponseEntity<?> submitDataset(
        @RequestHeader(value = HttpHeaders.CONTENT_TYPE, required = false)
        String contentType,
        @RequestParam(required = false) String name,
        @RequestBody(required = false) byte[] dataset
    ){
        if (containerParsedTheBodyAsForm(contentType)) {
            return ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
                    .body(new ErrorResponse(SEND_BYTES_MESSAGE));
        }

        if (dataset == null || dataset.length == 0) {
            return ResponseEntity.badRequest()
                    .body(new ErrorResponse("empty body: POST the dataset csv bytes"));
        }

        try {
            Dataset stored = service.store(dataset, name);
            LOG.info("stored dataset {} with {} rows", stored.datasetId(), stored.rowCount());
            return ResponseEntity.created(URI.create("/datasets/" + stored.datasetId()))
                    .body(view(stored));
        } catch (DatasetFormatException badData) {
            LOG.warn("rejected upload: {}", badData.getMessage());
            return ResponseEntity.badRequest().body(new ErrorResponse(badData.getMessage()));
        } catch (RuntimeException storageFailure) {
            // The message goes to the client, the stack trace stays in the log.
            LOG.error("upload failed", storageFailure);
            return ResponseEntity.internalServerError()
                    .body(new ErrorResponse(describe(storageFailure)));
        }
    }

    // 2. Get for list every stored dataset, newest first
    @GetMapping("/datasets")
    public List<DatasetResponse> listDatasets(){
        return service.list().stream().map(DatasetController::view).toList();
    }

    // 3. Get one dataset by id
    @GetMapping("/datasets/{datasetId}")
    public ResponseEntity<?> getDataset(@PathVariable String datasetId){
        var found = service.find(datasetId);
        if (found.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new ErrorResponse("no dataset with id " + datasetId));
        }
        return ResponseEntity.ok(view(found.get()));
    }

    // 4. Delete for forget an upload now instead of waiting for its expiry
    @DeleteMapping("/datasets/{datasetId}")
    public ResponseEntity<?> deleteDataset(@PathVariable String datasetId){
        boolean removed;
        try {
            removed = service.delete(datasetId);
        } catch (IllegalArgumentException notAnId) {
            // An id is a file name here, so the repository refuses shapes it never generated. To a
            // caller that is indistinguishable from an id that was never stored.
            removed = false;
        }
        if (!removed) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new ErrorResponse("no dataset with id " + datasetId));
        }
        return ResponseEntity.noContent().build();
    }

    // Helper
    private static DatasetResponse view(Dataset dataset){
        return new DatasetResponse(dataset.datasetId(), dataset.originalName(), dataset.uploadedAt(),
                dataset.expiresAt(), dataset.sizeBytes(), dataset.rowCount());
    }

    private static boolean containerParsedTheBodyAsForm(String contentType) {
        if (contentType == null) {
            return false;
        }
        try {
            MediaType sent = MediaType.valueOf(contentType);
            return sent.isCompatibleWith(MediaType.APPLICATION_FORM_URLENCODED)
                    || sent.isCompatibleWith(MediaType.MULTIPART_FORM_DATA);
        } catch (IllegalArgumentException malformedHeader) {
            return false;
        }
    }

    private static String describe(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
