package com.fyp.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.fyp.storage.Dataset;
import com.fyp.storage.DatasetRepository;

/**
 * That the service adds is the clock and nothing else, so both are tested here: every call reaches
 * the repository, and the {@link Instant} it is handed is the one from now.
 *
 * <p>{@code DatasetRepositoryTest} owns the storage behaviour, so the repository is stubbed out and
 * no database or uploads directory is opened.
 */
class DatasetServiceTest {

    private static final byte[] CSV = "user_id,text,timestamp\nu,#AI,1000\n"
            .getBytes(StandardCharsets.UTF_8);
    private static final Instant UPLOADED = Instant.parse("2026-11-16T09:00:00Z");
    private static final Dataset STORED = new Dataset("ds-20261116-0900-abcd", UPLOADED,
            UPLOADED.plus(Duration.ofDays(14)), "tweets.csv", CSV.length, 1L);

    private DatasetRepository datasets;
    private DatasetService service;

    @BeforeEach
    void setUp() {
        datasets = mock(DatasetRepository.class);
        service = new DatasetService(datasets);
    }

    @Test
    void anUploadIsStoredAsOfNow() {
        when(datasets.store(any(byte[].class), any(), any())).thenReturn(STORED);

        Instant before = Instant.now();
        Dataset stored = service.store(CSV, "tweets.csv");
        Instant after = Instant.now();

        assertThat(stored).isEqualTo(STORED);
        ArgumentCaptor<Instant> stamp = ArgumentCaptor.forClass(Instant.class);
        verify(datasets).store(eq(CSV), eq("tweets.csv"), stamp.capture());
        assertThat(stamp.getValue()).isBetween(before, after);
    }

    /** A caller who never names their file is not sent a null through to the display name. */
    @Test
    void anAbsentNameStillReachesTheRepository() {
        service.store(CSV, null);

        verify(datasets).store(eq(CSV), eq(null), any());
    }

    @Test
    void readsAreTheRepositoriesOwn() {
        when(datasets.findAll()).thenReturn(List.of(STORED));
        when(datasets.find("ds-20261116-0900-abcd")).thenReturn(Optional.of(STORED));
        when(datasets.pathOf("ds-20261116-0900-abcd"))
                .thenReturn(Optional.of(Path.of("data", "uploads", "ds-20261116-0900-abcd.csv")));

        assertThat(service.list()).containsExactly(STORED);
        assertThat(service.find("ds-20261116-0900-abcd")).hasValue(STORED);
        assertThat(service.pathOf("ds-20261116-0900-abcd"))
                .hasValue(Path.of("data", "uploads", "ds-20261116-0900-abcd.csv"));
    }

    @Test
    void aDeleteReportsWhetherTheIdWasThere() {
        when(datasets.delete("ds-20261116-0900-abcd")).thenReturn(true);
        when(datasets.delete("ds-19990101-0000-0000")).thenReturn(false);

        assertThat(service.delete("ds-20261116-0900-abcd")).isTrue();
        assertThat(service.delete("ds-19990101-0000-0000")).isFalse();
    }

    /** The whole point of taking {@code now}: a cron entry and a test drive the same sweep. */
    @Test
    void collectingSweepsAsOfNow() {
        when(datasets.deleteExpired(any())).thenReturn(2);

        Instant before = Instant.now();
        int collected = service.collectExpired();
        Instant after = Instant.now();

        assertThat(collected).isEqualTo(2);
        ArgumentCaptor<Instant> cutoff = ArgumentCaptor.forClass(Instant.class);
        verify(datasets).deleteExpired(cutoff.capture());
        assertThat(cutoff.getValue()).isBetween(before, after);
    }

    @Test
    void closingTheServiceReleasesTheConnection() {
        service.close();

        verify(datasets).close();
    }
}
