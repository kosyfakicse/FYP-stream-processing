package com.fyp.pipeline.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fyp.model.Attribution;
import com.fyp.model.UnifiedEvent;
import com.fyp.model.WindowOutput;

class CategoryAttributionFunctionTest {

    @Test
    void sumsEachSourceSeparatelyAndCountsEveryEvent() {
        List<UnifiedEvent> events = List.of(
                UnifiedEvent.of("user_1", "PYTHON", 1_000L),
                UnifiedEvent.of("user_2", "PYTHON", 1_200L),
                UnifiedEvent.of("user_2", "PYTHON", 1_400L));

        WindowOutput output = CategoryAttributionFunction.aggregate("PYTHON", 1_000L, 2_000L, events);

        assertThat(output.category()).isEqualTo("PYTHON");
        assertThat(output.windowStart()).isEqualTo(1_000L);
        assertThat(output.windowEnd()).isEqualTo(2_000L);
        assertThat(output.outputCount()).isEqualTo(3);
        assertThat(output.attribution().sourceQuantities()).containsExactly(
                Map.entry("user_1", 1L), Map.entry("user_2", 2L));
        assertThat(output.attribution().total()).isEqualTo(3);
    }

    @Test
    void addsQuantitiesRatherThanCountingRows() {
        List<UnifiedEvent> events = List.of(
                new UnifiedEvent("e1", "user_1", 7L, "PYTHON", 1_000L),
                new UnifiedEvent("e2", "user_1", 3L, "PYTHON", 1_200L));

        WindowOutput output = CategoryAttributionFunction.aggregate("PYTHON", 1_000L, 2_000L, events);

        assertThat(output.outputCount()).isEqualTo(2);
        assertThat(output.attribution().quantityOf("user_1")).isEqualTo(10L);
    }

    @Test
    void anEmptyWindowStillReportsZeroSources() {
        WindowOutput output = CategoryAttributionFunction.aggregate("PYTHON", 1_000L, 2_000L, List.of());

        assertThat(output.outputCount()).isZero();
        assertThat(output.attribution().sourceQuantities()).isEmpty();
    }

    @Test
    void writesAttributionAsSortedJsonAndReadsItBack() {
        Attribution attribution = new Attribution(Map.of("user_2", 1L, "user_1", 3L));

        assertThat(attribution.toJson()).isEqualTo("{\"user_1\":3,\"user_2\":1}");
        assertThat(Attribution.fromJson(attribution.toJson())).isEqualTo(attribution);
    }

    @Test
    void rejectsAttributionJsonThatIsNotAMap() {
        assertThat(org.assertj.core.api.Assertions.catchThrowableOfType(
                () -> Attribution.fromJson("[1,2]"), IllegalStateException.class))
                .hasMessageContaining("source->quantity map");
    }
}
