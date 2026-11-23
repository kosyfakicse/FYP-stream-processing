package com.fyp.pipeline.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.junit.jupiter.api.Test;

import com.fyp.model.Attribution;
import com.fyp.model.Interaction;
import com.fyp.model.ProvenanceRecord;
import com.fyp.model.RunVertices;
import com.fyp.model.UnifiedEvent;

/**
 * One row per hashtag per interval, with the key the stream was partitioned on written into it -- the
 * measurement the graph is checked against, so it is worth proving separately from the job that uses it.
 */
class InteractionRecorderTest {

    private static final RunVertices CHAIN = new RunVertices("run-1", "K1", "M1", "W1", "S1");

    private static final List<UnifiedEvent> EVENTS = List.of(
            UnifiedEvent.of("user_1", "PYTHON", 1_000L),
            UnifiedEvent.of("user_2", "PYTHON", 1_500L),
            UnifiedEvent.of("user_3", "JAVA", 1_800L),
            UnifiedEvent.of("user_4", "PYTHON", 2_500L));

    @Test
    void writesOneRowPerCategoryPerIntervalAndNothingForAHashtagThatWasSilent() throws Exception {
        List<ProvenanceRecord> rows = run(EVENTS, 1);

        // JAVA arrived once, in the first second, and says nothing about the second one. A row per
        // category per tick regardless would have to be invented from a count of zero, and a sum over
        // the ticks of one category would then disagree with the window it is meant to explain.
        assertThat(rowsOf(rows)).containsExactlyInAnyOrder(
                new Interaction("run-1", "M1", "W1", "JAVA", 2_000L, 1L,
                        new Attribution(Map.of("M1", 1L))),
                new Interaction("run-1", "M1", "W1", "PYTHON", 2_000L, 2L,
                        new Attribution(Map.of("M1", 2L))),
                new Interaction("run-1", "M1", "W1", "PYTHON", 3_000L, 1L,
                        new Attribution(Map.of("M1", 1L))));
    }

    /**
     * The interval is the row's, not the window's: a five-second tick over a sixty-second result has to
     * be able to say which fifth the events arrived in, and its timestamp is the instant that claim ends.
     */
    @Test
    void stampsEachRowWithTheEndOfTheIntervalItCounted() throws Exception {
        List<ProvenanceRecord> rows = run(EVENTS, 2);

        assertThat(rowsOf(rows)).containsExactlyInAnyOrder(
                new Interaction("run-1", "M1", "W1", "JAVA", 2_000L, 1L,
                        new Attribution(Map.of("M1", 1L))),
                new Interaction("run-1", "M1", "W1", "PYTHON", 2_000L, 2L,
                        new Attribution(Map.of("M1", 2L))),
                new Interaction("run-1", "M1", "W1", "PYTHON", 4_000L, 1L,
                        new Attribution(Map.of("M1", 1L))));
    }

    private static List<Interaction> rowsOf(List<ProvenanceRecord> records) {
        return records.stream().map(ProvenanceRecord::interaction).filter(Objects::nonNull).toList();
    }

    private List<ProvenanceRecord> run(List<UnifiedEvent> events, int tickSeconds) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ProvenanceRecord> recorded = env
                .fromData(events)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UnifiedEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((event, stamp) -> event.timestamp()))
                .keyBy((KeySelector<UnifiedEvent, String>) UnifiedEvent::category)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(tickSeconds)))
                .process(new InteractionRecorder(CHAIN, "M1", "W1"));

        List<ProvenanceRecord> rows = new ArrayList<>();
        try (var iterator = recorded.executeAndCollect()) {
            while (iterator.hasNext()) {
                rows.add(iterator.next());
            }
        }
        return rows;
    }
}
