package com.fyp.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.fyp.model.Attribution;
import com.fyp.model.WindowOutput;
import com.fyp.pipeline.loader.DatasetFormatException;
import com.fyp.service.AttributionService;
import com.fyp.service.DatasetService;
import com.fyp.storage.Run;
import com.fyp.storage.RunResults;

/**
 * The HTTP contract of {@code POST /runs} on its own, with the pipeline and the uploads directory
 * stubbed out.
 *
 * <p>{@code standaloneSetup} rather than a Spring context: what is under test is the mapping, the
 * status codes and the body shape, and none of those depend on how the controller was wired.
 * {@link ApiRoundTripTest} covers the wiring.
 */
class RunControllerTest {

    private static final String DATASET_ID = "ds-20261116-0931-4e11";
    private static final String RUN_ID = "run-20261116-1200-abcd";
    private static final Instant STARTED = Instant.parse("2026-11-16T12:00:00Z");
    private static final Instant FINISHED = Instant.parse("2026-11-16T12:00:02Z");
    private static final Run FINISHED_RUN = new Run(RUN_ID, STARTED, FINISHED, DATASET_ID);

    private AttributionService service;
    private DatasetService datasets;
    private Path storedCsv;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        service = mock(AttributionService.class);
        datasets = mock(DatasetService.class);
        storedCsv = Path.of("..", "data", "uploads", DATASET_ID + ".csv");
        mvc = MockMvcBuilders.standaloneSetup(new RunController(service, datasets)).build();
    }

    @Test
    void aStoredDatasetIsRunFromItsOwnFileAndTheIdIsRecordedOnTheRun() throws Exception {
        when(datasets.pathOf(DATASET_ID)).thenReturn(Optional.of(storedCsv));
        when(service.run(storedCsv, DATASET_ID)).thenReturn("run-20261116-1200-abcd");

        mvc.perform(run(DATASET_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.runId").value("run-20261116-1200-abcd"));

        verify(service).run(storedCsv, DATASET_ID);
    }

    /**
     * The id is the whole request, so the two ways a caller can leave it out get one message that
     * names the endpoint that mints them.
     */
    @Test
    void aRequestWithoutADatasetIdIsRejected() throws Exception {
        mvc.perform(post("/runs").contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error")
                        .value("expected {\"datasetId\": \"ds-...\"} from POST /datasets"));

        mvc.perform(run("   "))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").exists());

        verifyNoInteractions(service);
    }

    @Test
    void anIdNothingWasUploadedUnderIsNotFoundAndNotRun() throws Exception {
        when(datasets.pathOf(DATASET_ID)).thenReturn(Optional.empty());

        mvc.perform(run(DATASET_ID))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("no dataset with id " + DATASET_ID));

        verifyNoInteractions(service);
    }

    /**
     * A stored file passed {@code DatasetScan} on the way in, so the only way the loader still
     * complains is a hand-edit after the upload -- which is a bad dataset, not a bad request path.
     */
    @Test
    void aDatasetTheLoaderRejectsStillAnswersWithTheLoadersComplaint() throws Exception {
        when(datasets.pathOf(DATASET_ID)).thenReturn(Optional.of(storedCsv));
        when(service.run(storedCsv, DATASET_ID)).thenThrow(new DatasetFormatException(
                "Expected header user_id,text,timestamp but found: a,b,c"));

        mvc.perform(run(DATASET_ID))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error")
                        .value("Expected header user_id,text,timestamp but found: a,b,c"));
    }

    @Test
    void aStorageFailureIsAServerErrorAndNotTheClientsFault() throws Exception {
        when(datasets.pathOf(DATASET_ID)).thenReturn(Optional.of(storedCsv));
        when(service.run(storedCsv, DATASET_ID))
                .thenThrow(new IllegalStateException("Failed to update: INSERT INTO windows"));

        mvc.perform(run(DATASET_ID))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error")
                        .value("IllegalStateException: Failed to update: INSERT INTO windows"));
    }

    /**
     * The csv upload route is gone, so sending bytes here is a client still on the old contract. The
     * status is Spring's own mapping and its body is not the {@code {"error": ...}} shape, which is
     * all this asserts: {@code POST /datasets} is where bytes belong.
     */
    @Test
    void csvBytesAreRefusedAsTheWrongContentType() throws Exception {
        mvc.perform(post("/runs").contentType(MediaType.parseMediaType("text/csv"))
                        .content("user_id,text,timestamp\nuser_1,#AI,1000\n"))
                .andExpect(status().isUnsupportedMediaType());

        verifyNoInteractions(service, datasets);
    }

    /**
     * The read a run id exists for: the run's own row plus every window it fired. Timestamps are left
     * unasserted here because {@code standaloneSetup} is not Boot's converter chain;
     * {@link ApiRoundTripTest} is where the ISO-8601 shape is pinned.
     */
    @Test
    void aRunReadsBackWithEveryWindowItFired() throws Exception {
        when(service.findRun(RUN_ID)).thenReturn(Optional.of(FINISHED_RUN));
        when(service.fetchAll(RUN_ID)).thenReturn(new RunResults(RUN_ID, List.of(
                new WindowOutput(1000L, 2000L, "AI", 2L, new Attribution(Map.of("user_1", 2L))),
                new WindowOutput(1000L, 2000L, "DATA", 1L, new Attribution(Map.of("user_2", 1L))))));

        mvc.perform(get("/runs/" + RUN_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.runId").value(RUN_ID))
                .andExpect(jsonPath("$.dataset").value(DATASET_ID))
                .andExpect(jsonPath("$.windows.length()").value(2))
                .andExpect(jsonPath("$.windows[0].category").value("AI"))
                .andExpect(jsonPath("$.windows[0].windowStart").value(1000))
                .andExpect(jsonPath("$.windows[0].windowEnd").value(2000))
                .andExpect(jsonPath("$.windows[0].attribution.user_1").value(2));
    }

    @Test
    void aRunIdThatNeverRanIsNotFound() throws Exception {
        when(service.findRun(RUN_ID)).thenReturn(Optional.empty());

        mvc.perform(get("/runs/" + RUN_ID))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("no run with id " + RUN_ID));

        verify(service, never()).fetchAll(RUN_ID);
    }

    @Test
    void everyRunInTheTableIsListedWithItsOwnRow() throws Exception {
        when(service.listRuns()).thenReturn(List.of(FINISHED_RUN,
                new Run("run-20261116-1159-abcd", STARTED, null, "tweet_hashtag")));

        mvc.perform(get("/runs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].runId").value(RUN_ID))
                .andExpect(jsonPath("$[0].dataset").value(DATASET_ID))
                .andExpect(jsonPath("$[1].runId").value("run-20261116-1159-abcd"));
    }

    /** A read that trips over the connection is our failure, not the caller's, so it is a 500. */
    @Test
    void aFailedReadIsAServerErrorAndNotTheClientsFault() throws Exception {
        when(service.listRuns()).thenThrow(new IllegalStateException("Connection is closed"));

        mvc.perform(get("/runs"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("IllegalStateException: Connection is closed"));
    }

    private static org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder run(
            String datasetId) {
        return post("/runs").contentType(MediaType.APPLICATION_JSON)
                .content("{\"datasetId\":\"" + datasetId + "\"}");
    }
}
