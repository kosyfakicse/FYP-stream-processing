package com.fyp.api;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fyp.model.WindowOutput;
import com.fyp.pipeline.loader.DatasetFormatException;
import com.fyp.service.AttributionService;
import com.fyp.service.DatasetService;
import com.fyp.storage.Run;

/**
 * {@code POST /runs} takes a dataset id and returns a run id; {@code GET /runs} and
 * {@code GET /runs/{runId}} read back what those runs stored.
 *
 * <p>The class owns the noun {@code run} and the path is the collection, because POST creates one
 * member of it. The reads hang off the same noun so a run id stays a key a client can come back with
 * instead of a number it has to remember: the list is what a run picker renders, the detail is the
 * run's own row plus every window it fired.
 *
 * <p>The body of a POST names an upload rather than carrying one, because an upload that got here
 * through {@code POST /datasets} was already scanned -- and a scan is what proves the csv is sorted by
 * timestamp. {@code AttributionService} reads event time with monotonic watermarks, so running
 * unsorted bytes is a run that answers 200 and silently drops every event that looks late. Requiring
 * an id makes that input impossible to send.
 *
 * <p>POST rather than PUT because the same dataset twice is two runs with two ids, not one resource
 * written twice.
 *
 * <p>Responses are synchronous: 200 on a POST means the job finished and every window was stored. That
 * is honest at 51 rows and under two seconds; at larger inputs the same run id supports an async pair
 * of {@code 202 Accepted} plus a status endpoint, since {@code runs.started_at} and
 * {@code finished_at} are already written and {@code GET /runs/{runId}} already reads them back.
 */
@RestController
public class RunController {

    private static final Logger LOG = LoggerFactory.getLogger(RunController.class);

    public record RunRequest(String datasetId) {
    }

    public record RunResponse(String runId) {
    }

    public record RunSummaryResponse(String runId, Instant startedAt, Instant finishedAt,
            String dataset) {
    }

    public record WindowResponse(long windowStart, long windowEnd, String category, long outputCount,
            Map<String, Long> attribution) {
    }

    public record RunDetailResponse(String runId, Instant startedAt, Instant finishedAt,
            String dataset, List<WindowResponse> windows) {
    }

    public record ErrorResponse(String error) {
    }

    private final AttributionService service;
    private final DatasetService datasets;

    /**
     * One service means one DuckDB connection, and DuckDB allows a single writer per file, so two
     * overlapping requests would interleave their batches on that connection. Serialising runs is
     * the fix at this scale; queueing properly is the fix if the API ever serves more than one user.
     *
     * <p>The reads take the same lock. They only SELECT, but they share the connection with a job
     * that is INSERTing, and one connection is not safe for two statements at once.
     */
    private final Object runLock = new Object();

    public RunController(AttributionService service, DatasetService datasets) {
        this.service = service;
        this.datasets = datasets;
    }

    @PostMapping(value = "/runs", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> createRun(@RequestBody(required = false) RunRequest request) {
        // Optional so a body-less or id-less request answers with the message below rather than
        // Spring's generic 400, which does not use the {"error": ...} shape.
        if (request == null || request.datasetId() == null || request.datasetId().isBlank()) {
            return ResponseEntity.badRequest()
                    .body(new ErrorResponse("expected {\"datasetId\": \"ds-...\"} from POST /datasets"));
        }
        String datasetId = request.datasetId();

        // Empty for an id that was never stored and for a row whose file has gone; both mean this
        // run cannot happen, and neither is a run row waiting on a result.
        Optional<Path> csv = datasets.pathOf(datasetId);
        if (csv.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new ErrorResponse("no dataset with id " + datasetId));
        }

        try {
            String runId;
            synchronized (runLock) {
                runId = service.run(csv.get(), datasetId);
            }
            LOG.info("stored run {} for dataset {}", runId, datasetId);
            return ResponseEntity.ok(new RunResponse(runId));

        } catch (DatasetFormatException badData) {
            // The upload passed DatasetScan, so this is a file edited on disk after it landed.
            LOG.warn("rejected dataset {}: {}", datasetId, badData.getMessage());
            return ResponseEntity.badRequest().body(new ErrorResponse(badData.getMessage()));
        } catch (Exception storageOrJobFailure) {
            LOG.error("run failed", storageOrJobFailure);
            return failed(storageOrJobFailure);
        }
    }

    /** Every run this database holds, newest first. */
    @GetMapping("/runs")
    public ResponseEntity<?> listRuns() {
        try {
            List<RunSummaryResponse> runs;
            synchronized (runLock) {
                runs = service.listRuns().stream().map(RunController::summary).toList();
            }
            return ResponseEntity.ok(runs);
        } catch (RuntimeException storageFailure) {
            LOG.error("listing runs failed", storageFailure);
            return failed(storageFailure);
        }
    }

    /** One run's own row plus every window it fired. */
    @GetMapping("/runs/{runId}")
    public ResponseEntity<?> getRun(@PathVariable String runId) {
        try {
            Run recorded;
            List<WindowOutput> windows;
            synchronized (runLock) {
                Optional<Run> found = service.findRun(runId);
                if (found.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .body(new ErrorResponse("no run with id " + runId));
                }
                recorded = found.get();
                windows = service.fetchAll(runId).outputs();
            }
            return ResponseEntity.ok(detail(recorded, windows));
        } catch (RuntimeException storageFailure) {
            LOG.error("reading run {} failed", runId, storageFailure);
            return failed(storageFailure);
        }
    }

    private static RunSummaryResponse summary(Run run) {
        return new RunSummaryResponse(run.runId(), run.startedAt(), run.finishedAt(), run.dataset());
    }

    private static RunDetailResponse detail(Run run, List<WindowOutput> windows) {
        return new RunDetailResponse(run.runId(), run.startedAt(), run.finishedAt(), run.dataset(),
                windows.stream().map(RunController::window).toList());
    }

    private static WindowResponse window(WindowOutput output) {
        return new WindowResponse(output.windowStart(), output.windowEnd(), output.category(),
                output.outputCount(), output.attribution().sourceQuantities());
    }

    private static ResponseEntity<ErrorResponse> failed(Exception storageOrJobFailure) {
        // The message goes to the client, the stack trace stays in the log.
        return ResponseEntity.internalServerError()
                .body(new ErrorResponse(describe(storageOrJobFailure)));
    }

    private static String describe(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
