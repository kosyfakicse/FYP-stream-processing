package com.fyp.pipeline.loader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class DatasetScanTest {

    private static long count(String... lines) {
        return DatasetScan.rowCount(String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void countsDataRowsOfAnOrderedFile() {
        assertThat(count("user_id,text,timestamp", "user_1,#AI,1000", "user_2,#java,2000")).isEqualTo(2);
    }

    /** Equal timestamps are not backwards, and a window boundary landing on two events is normal. */
    @Test
    void acceptsRepeatedTimestamps() {
        assertThat(count("user_id,text,timestamp", "user_1,#AI,1000", "user_2,#AI,1000")).isEqualTo(2);
    }

    @Test
    void ignoresBlankLinesLikeTheLoaderDoes() {
        assertThat(count("user_id,text,timestamp", "user_1,#AI,1000", "", "user_2,#AI,2000", ""))
                .isEqualTo(2);
    }

    @Test
    void rejectsAWrongHeader() {
        assertThatThrownBy(() -> count("a,b,c", "1,2,3"))
                .isInstanceOf(DatasetFormatException.class)
                .hasMessageContaining("Expected header user_id,text,timestamp");
    }

    /**
     * The point of the whole scan. Without it this file loads, every out-of-order event is dropped as
     * late, and the run reports a confident small number.
     */
    @Test
    void rejectsATimestampGoingBackwardsAndNamesTheLine() {
        assertThatThrownBy(() -> count("user_id,text,timestamp", "user_1,#AI,2000", "user_2,#AI,1000"))
                .isInstanceOf(DatasetFormatException.class)
                .hasMessageContaining("not sorted by timestamp at line 3")
                .hasMessageContaining("1000 comes after 2000");
    }

    @Test
    void rejectsAFileWithOnlyAHeader() {
        assertThatThrownBy(() -> count("user_id,text,timestamp"))
                .isInstanceOf(DatasetFormatException.class)
                .hasMessageContaining("no rows");
    }

    @Test
    void rejectsANonNumericTimestamp() {
        assertThatThrownBy(() -> count("user_id,text,timestamp", "user_1,#AI,soon"))
                .isInstanceOf(DatasetFormatException.class)
                .hasMessageContaining("Timestamp is not a long at line 2");
    }

    @Test
    void rejectsALineWithTooFewFields() {
        assertThatThrownBy(() -> count("user_id,text,timestamp", "user_1,#AI"))
                .isInstanceOf(DatasetFormatException.class)
                .hasMessageContaining("Expected user_id,text,timestamp at line 2");
    }

    @Test
    void rejectsNoBytesAtAll() {
        assertThatThrownBy(() -> DatasetScan.rowCount(new byte[0]))
                .isInstanceOf(DatasetFormatException.class)
                .hasMessageContaining("Dataset is empty");
    }
}
