package com.fyp.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fyp.pipeline.loader.DatasetFormatException;

class DatasetRepositoryTest {

    private static final Instant UPLOADED = Instant.parse("2026-11-16T09:00:00Z");
    private static final byte[] CSV =
            "user_id,text,timestamp\nuser_1,#AI,1000\nuser_2,#java,2000\n".getBytes(UTF_8);

    private final RecordingExecutor sql = new RecordingExecutor();

    @TempDir
    Path uploads;

    private DatasetRepository repository(Duration retention) {
        return new DatasetRepository(sql, uploads, retention);
    }

    @Test
    void writesTheFileNamedAfterTheIdItHandsBack() {
        Dataset stored = repository(Duration.ofDays(7)).store(CSV, "my tweets.csv", UPLOADED);

        assertThat(stored.datasetId()).matches("ds-\\d{8}-\\d{4}-[0-9a-f]{4}");
        assertThat(uploads.resolve(stored.datasetId() + ".csv")).exists();
        assertThat(stored.originalName()).isEqualTo("my tweets.csv");
        assertThat(stored.sizeBytes()).isEqualTo(CSV.length);
        assertThat(stored.rowCount()).isEqualTo(2);
    }

    @Test
    void recordsExpiryAsTheUploadTimePlusRetention() {
        Dataset stored = repository(Duration.ofDays(7)).store(CSV, "tweets.csv", UPLOADED);

        assertThat(sql.lastUpdate().sql()).startsWith("INSERT INTO datasets");
        assertThat(sql.lastUpdate().args()).isEqualTo(Arrays.asList(stored.datasetId(),
                RunRepository.toTimestamp(UPLOADED),
                RunRepository.toTimestamp(UPLOADED.plus(Duration.ofDays(7))), "tweets.csv",
                (long) CSV.length, 2L));
    }

    /** upload_ttl_days: 0 means keep until deleted by hand, which is a null and not a date. */
    @Test
    void storesNoExpiryWhenRetentionIsZero() {
        Dataset stored = repository(Duration.ZERO).store(CSV, "tweets.csv", UPLOADED);

        assertThat(stored.expiresAt()).isNull();
        assertThat(sql.lastUpdate().args().get(2)).isNull();
    }

    @Test
    void rejectsABadUploadWithoutTouchingTheDirectory() {
        byte[] bad = "a,b,c\n1,2,3\n".getBytes(UTF_8);

        assertThatThrownBy(() -> repository(Duration.ofDays(1)).store(bad, "bad.csv", UPLOADED))
                .isInstanceOf(DatasetFormatException.class);

        assertThat(uploads).isEmptyDirectory();
        assertThat(sql.updates).isEmpty();
    }

    /** The name is display text. Storing the client's directories would put them in the UI. */
    @Test
    void stripsDirectoriesFromTheClientName() {
        Dataset stored = repository(Duration.ofDays(1))
                .store(CSV, "..\\..\\desktop\\tweets.csv", UPLOADED);

        assertThat(stored.originalName()).isEqualTo("tweets.csv");
    }

    @Test
    void fallsBackToTheGeneratedNameWhenTheClientSendsNone() {
        Dataset stored = repository(Duration.ofDays(1)).store(CSV, "   ", UPLOADED);

        assertThat(stored.originalName()).isEqualTo(stored.datasetId() + ".csv");
    }

    @Test
    void deleteExpiredRemovesTheRowsItSelectedAndTheirFiles() {
        Dataset stored = repository(Duration.ofDays(7)).store(CSV, "tweets.csv", UPLOADED);
        Path file = uploads.resolve(stored.datasetId() + ".csv");
        sql.queueRows(new Row(List.of(stored.datasetId())));

        int removed = repository(Duration.ofDays(7)).deleteExpired(UPLOADED.plus(Duration.ofDays(30)));

        assertThat(removed).isEqualTo(1);
        assertThat(sql.lastUpdate().sql()).isEqualTo("DELETE FROM datasets WHERE dataset_id = ?");
        assertThat(sql.lastUpdate().args()).isEqualTo(List.of(stored.datasetId()));
        assertThat(file).doesNotExist();
    }

    @Test
    void deleteExpiredReportsNothingWhenNothingHasRunOut() {
        assertThat(repository(Duration.ofDays(7)).deleteExpired(UPLOADED)).isZero();
        assertThat(sql.updates).isEmpty();
    }

    /**
     * The id becomes a file name, so a string that is not one of ours must never reach resolve(). The
     * row below is deliberately impossible to create through store().
     */
    @Test
    void refusesToBuildAPathFromSomethingThatIsNotAnId() {
        sql.queueRows(new Row(Arrays.asList("../../etc/passwd", RunRepository.toTimestamp(UPLOADED), null,
                "passwd", 1L, 1L)));

        assertThatThrownBy(() -> repository(Duration.ofDays(1)).pathOf("../../etc/passwd"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Not a dataset id");
    }

    @Test
    void reportsAnUnknownIdAsAbsent() {
        assertThat(repository(Duration.ofDays(1)).pathOf("ds-19990101-0000-0000")).isEmpty();

        RecordingExecutor.Call query = sql.queries.get(sql.queries.size() - 1);
        assertThat(query.sql()).startsWith("SELECT dataset_id, uploaded_at, expires_at");
        assertThat(query.args()).isEqualTo(List.of("ds-19990101-0000-0000"));
    }

    /** The suffix is what separates two uploads in the same minute, so it must not repeat often. */
    @Test
    void generatesDistinctIdsInsideOneMinute() {
        List<String> ids = IntStream.range(0, 200).mapToObj(i -> DatasetRepository.newDatasetId(UPLOADED))
                .distinct().toList();

        assertThat(ids).hasSizeGreaterThan(150);
    }
}
