package com.fyp.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.fyp.pipeline.loader.DatasetFormatException;
import com.fyp.service.DatasetService;
import com.fyp.storage.Dataset;

/**
 * The HTTP contract of the four /datasets calls on their own, with the service stubbed out.
 *
 * <p>{@code standaloneSetup} rather than a Spring context, the same reason as {@link RunControllerTest}:
 * the mapping, the status codes and the body shape do not depend on how the controller was wired.
 * {@link ApiRoundTripTest} covers the wiring.
 */
class DatasetControllerTest {

    private static final String VALID_CSV = "user_id,text,timestamp\nuser_1,#AI,1000\n";
    private static final Instant UPLOADED = Instant.parse("2026-11-16T09:00:00Z");
    private static final Dataset STORED = new Dataset("ds-20261116-0900-abcd", UPLOADED,
            UPLOADED.plus(Duration.ofDays(7)), "my tweets.csv", 38L, 1L);

    private DatasetService service;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        service = mock(DatasetService.class);
        mvc = MockMvcBuilders.standaloneSetup(new DatasetController(service)).build();
    }

    @Test
    void anUploadIsCreatedAndPointsAtItsOwnAddress() throws Exception {
        when(service.store(ArgumentMatchers.<byte[]>any(), any())).thenReturn(STORED);

        mvc.perform(post("/datasets").content(VALID_CSV.getBytes(UTF_8)))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/datasets/ds-20261116-0900-abcd"))
                .andExpect(jsonPath("$.datasetId").value("ds-20261116-0900-abcd"))
                .andExpect(jsonPath("$.originalName").value("my tweets.csv"))
                .andExpect(jsonPath("$.rowCount").value(1))
                .andExpect(jsonPath("$.uploadedAt").value("2026-11-16T09:00:00Z"));
    }

    /** The name is the one thing a raw-bytes upload cannot carry in its body, so it rides in the query. */
    @Test
    void theNameQueryParamReachesTheService() throws Exception {
        when(service.store(ArgumentMatchers.<byte[]>any(), eq("october.csv"))).thenReturn(STORED);

        mvc.perform(post("/datasets").param("name", "october.csv")
                        .content(VALID_CSV.getBytes(UTF_8)))
                .andExpect(status().isCreated());

        verify(service).store(ArgumentMatchers.<byte[]>any(), eq("october.csv"));
    }

    @Test
    void anEmptyBodyIsRejectedBeforeAnythingIsStored() throws Exception {
        mvc.perform(post("/datasets").content(new byte[0]))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("empty body: POST the dataset csv bytes"));

        verifyNoInteractions(service);
    }

    @Test
    void aFormEncodedBodyIsRejectedAsTheWrongContentType() throws Exception {
        mvc.perform(post("/datasets")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                        .content(VALID_CSV.getBytes(UTF_8)))
                .andExpect(status().isUnsupportedMediaType())
                .andExpect(jsonPath("$.error")
                        .value("send the raw csv bytes with Content-Type: text/csv or application/octet-stream"));

        verifyNoInteractions(service);
    }

    /**
     * A rejected file is the client's mistake and says so, which is the difference between this and
     * the 500 below: one names a line of their csv, the other names our own failure.
     */
    @Test
    void badBytesAnswerWithTheScannersComplaint() throws Exception {
        when(service.store(ArgumentMatchers.<byte[]>any(), any()))
                .thenThrow(new DatasetFormatException("Dataset is not sorted by timestamp at line 3"));

        mvc.perform(post("/datasets").content(VALID_CSV.getBytes(UTF_8)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error")
                        .value("Dataset is not sorted by timestamp at line 3"));
    }

    @Test
    void aStorageFailureIsAServerErrorAndNotTheClientsFault() throws Exception {
        when(service.store(ArgumentMatchers.<byte[]>any(), any()))
                .thenThrow(new IllegalStateException("Failed to update: INSERT INTO datasets"));

        mvc.perform(post("/datasets").content(VALID_CSV.getBytes(UTF_8)))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error")
                        .value("IllegalStateException: Failed to update: INSERT INTO datasets"));
    }

    @Test
    void theListKeepsTheOrderTheRepositoryGaveIt() throws Exception {
        when(service.list()).thenReturn(List.of(STORED, new Dataset("ds-20261115-0900-ffee", UPLOADED,
                null, "older.csv", 20L, 1L)));

        mvc.perform(get("/datasets"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].datasetId").value("ds-20261116-0900-abcd"))
                .andExpect(jsonPath("$[1].datasetId").value("ds-20261115-0900-ffee"))
                .andExpect(jsonPath("$[1].expiresAt").value(org.hamcrest.Matchers.nullValue()));
    }

    @Test
    void anUploadWithoutExpirySaysSoRatherThanInventingADate() throws Exception {
        when(service.find("ds-20261116-0900-abcd")).thenReturn(Optional.of(
                new Dataset("ds-20261116-0900-abcd", UPLOADED, null, "kept.csv", 38L, 1L)));

        mvc.perform(get("/datasets/ds-20261116-0900-abcd"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.expiresAt").value(org.hamcrest.Matchers.nullValue()));
    }

    @Test
    void anUnknownIdIsNotFoundRatherThanAnEmptyBody() throws Exception {
        when(service.find("ds-19990101-0000-0000")).thenReturn(Optional.empty());

        mvc.perform(get("/datasets/ds-19990101-0000-0000"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("no dataset with id ds-19990101-0000-0000"));
    }

    @Test
    void aDeletedUploadAnswersWithNoContent() throws Exception {
        when(service.delete("ds-20261116-0900-abcd")).thenReturn(true);

        mvc.perform(delete("/datasets/ds-20261116-0900-abcd")).andExpect(status().isNoContent());
    }

    /**
     * The repository refuses an id it never generated, because an id is a file name here. A caller
     * probing the filesystem gets the same answer as one guessing wrong, which is the point.
     */
    @Test
    void aShapeThatIsNotAnIdIsNotFoundAndNotAFiftyHundred() throws Exception {
        when(service.delete("two-dots-and-a-slash"))
                .thenThrow(new IllegalArgumentException("Not a dataset id: two-dots-and-a-slash"));

        mvc.perform(delete("/datasets/two-dots-and-a-slash"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("no dataset with id two-dots-and-a-slash"));
    }

    @Test
    void deletingAnUnknownIdIsNotFound() throws Exception {
        when(service.delete("ds-19990101-0000-0000")).thenReturn(false);

        mvc.perform(delete("/datasets/ds-19990101-0000-0000")).andExpect(status().isNotFound());
    }
}
