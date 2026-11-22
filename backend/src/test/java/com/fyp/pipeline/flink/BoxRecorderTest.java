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
import com.fyp.model.State;
import com.fyp.model.Tweet;

/**
 * The box recorder over a real MiniCluster, for the one property a single-interval fixture cannot show:
 * the state row accumulates across intervals while the interaction row beside it reports only its own.
 * Getting that backwards -- window state instead of global state -- leaves every row individually
 * plausible and every box wrong.
 */
class BoxRecorderTest {

    private static final RunVertices CHAIN = new RunVertices("run-1", "K1", "M1", "W1", "S1");

    /** Four tweets in the first second, one in the third, and nothing at all in the second. */
    private static final List<Tweet> TWEETS = List.of(
            new Tweet("user_1", "#python", 1_000L),
            new Tweet("user_2", "#python", 1_200L),
            new Tweet("user_3", "#java", 1_400L),
            new Tweet("user_4", "#java", 1_600L),
            new Tweet("user_5", "#java", 3_000L));

    @Test
    void addsEachIntervalToTheBoxWhileItsInteractionStaysWithinItsOwn() throws Exception {
        List<ProvenanceRecord> rows = run(TWEETS, "M1");

        // Two rows for three intervals: the empty second wrote nothing, because Flink does not call a
        // window function for an interval with no elements. That is what keeps an idle minute over a
        // day of tweets off the table.
        assertThat(rows.stream().map(ProvenanceRecord::state).filter(Objects::nonNull).toList())
                .containsExactly(
                        new State("run-1", "K1", null, 1_000L, 2_000L, 4L, Attribution.empty(), 2_000L),
                        new State("run-1", "K1", null, 3_000L, 4_000L, 5L, Attribution.empty(), 4_000L));
        assertThat(rows.stream().map(ProvenanceRecord::interaction).filter(Objects::nonNull).toList())
                .containsExactly(
                        new Interaction("run-1", "K1", "M1", null, 2_000L, 4L,
                                new Attribution(Map.of("K1", 4L))),
                        new Interaction("run-1", "K1", "M1", null, 4_000L, 1L,
                                new Attribution(Map.of("K1", 1L))));
    }

    /**
     * A box whose interaction is measured by the operator on the other side -- {@code M1}, in the real
     * topology -- emits the state row and nothing else, so the two cannot be counted twice.
     */
    @Test
    void recordsOnlyTheBoxOfAVertexThatWasNotAskedForItsInteraction() throws Exception {
        List<ProvenanceRecord> rows = run(TWEETS, null);

        assertThat(rows).allSatisfy(row -> assertThat(row.interaction()).isNull());
        assertThat(rows).hasSize(2);
        assertThat(rows.get(1).state().quantity()).isEqualTo(5L);
    }

    private List<ProvenanceRecord> run(List<Tweet> tweets, String downstreamVertexId) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ProvenanceRecord> recorded = env
                .fromData(tweets)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tweet>forMonotonousTimestamps()
                        .withTimestampAssigner(new TweetTimestampAssigner()))
                .keyBy((KeySelector<Tweet, String>) tweet -> "all")
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
                .process(new BoxRecorder<>(CHAIN, "K1", downstreamVertexId));

        List<ProvenanceRecord> rows = new ArrayList<>();
        try (var iterator = recorded.executeAndCollect()) {
            while (iterator.hasNext()) {
                rows.add(iterator.next());
            }
        }
        return rows;
    }
}
