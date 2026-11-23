package com.fyp.pipeline.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.junit.jupiter.api.Test;

import com.fyp.model.Tweet;
import com.fyp.model.UnifiedEvent;
import com.fyp.model.WindowOutput;

/**
 * Runs the real topology over a bounded source on the in-process MiniCluster. This is the test that
 * proves event-time windows actually fire, which is the failure mode that produces silently empty
 * output rather than an error.
 */
class AttributionWindowJobTest {

    private static final List<Tweet> TWEETS = List.of(
            new Tweet("user_1", "Loving #python today", 1_000L),
            new Tweet("user_2", "#python is awesome", 1_200L),
            new Tweet("user_3", "Check out #java", 1_400L),
            new Tweet("user_4", "#python and #streaming", 1_600L),
            new Tweet("user_5", "#python again", 3_000L));

    @Test
    void attributesEachSourceWithinItsOwnHashtagWindow() throws Exception {
        List<WindowOutput> outputs = run(TWEETS, 1);

        WindowOutput pythonFirst = only(outputs, "PYTHON", 1_000L);
        assertThat(pythonFirst.outputCount()).isEqualTo(3);
        assertThat(pythonFirst.windowEnd()).isEqualTo(2_000L);
        assertThat(pythonFirst.attribution().total()).isEqualTo(3);
        assertThat(pythonFirst.attribution().quantityOf("user_1")).isEqualTo(1L);
        assertThat(pythonFirst.attribution().quantityOf("user_2")).isEqualTo(1L);
        assertThat(pythonFirst.attribution().quantityOf("user_4")).isEqualTo(1L);

        assertThat(only(outputs, "JAVA", 1_000L).attribution().quantityOf("user_3")).isEqualTo(1L);
        assertThat(only(outputs, "STREAMING", 1_000L).attribution().quantityOf("user_4")).isEqualTo(1L);
    }

    @Test
    void firesOneWindowPerIntervalAndFlushesTheLastOne() throws Exception {
        List<WindowOutput> outputs = run(TWEETS, 1);

        List<Long> pythonWindows = outputs.stream()
                .filter(output -> output.category().equals("PYTHON"))
                .map(WindowOutput::windowStart)
                .toList();

        assertThat(pythonWindows).containsExactlyInAnyOrder(1_000L, 3_000L);
        assertThat(only(outputs, "PYTHON", 3_000L).attribution().quantityOf("user_5")).isEqualTo(1L);
    }

    /**
     * Two tweets naming a tag three times fire one window whose count is 3, and nothing inside the window
     * can say how many tweets that was. The firing no longer tries: the tweet total is measured upstream by
     * {@code BoxRecorder}, on the stream that still carries whole tweets, which is why {@code K1 -> M1} and
     * {@code M1 -> W1} are allowed to disagree -- see {@code AttributionPipelineEndToEndTest}.
     */
    @Test
    void countsThreeEventsForTwoTweetsWithoutKnowingTheyWereTwo() throws Exception {
        List<WindowOutput> outputs = run(List.of(
                new Tweet("user_1", "#python and again #python", 1_000L),
                new Tweet("user_2", "#python", 1_200L)), 1);

        assertThat(only(outputs, "PYTHON", 1_000L).outputCount()).isEqualTo(3);
    }

    private static WindowOutput only(List<WindowOutput> outputs, String category, long windowStart) {
        List<WindowOutput> matches = outputs.stream()
                .filter(output -> output.category().equals(category))
                .filter(output -> output.windowStart() == windowStart)
                .toList();
        assertThat(matches).hasSize(1);
        return matches.get(0);
    }

    private List<WindowOutput> run(List<Tweet> tweets, int windowSizeSeconds) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<WindowOutput> outputs = env
                .fromData(tweets)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tweet>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new TweetTimestampAssigner()))
                .flatMap(new HashtagExtractor())
                .keyBy((KeySelector<UnifiedEvent, String>) UnifiedEvent::category)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSizeSeconds)))
                .process(new CategoryAttributionFunction());

        List<WindowOutput> results = new ArrayList<>();
        try (var iterator = outputs.executeAndCollect()) {
            while (iterator.hasNext()) {
                results.add(iterator.next());
            }
        }
        return results;
    }
}
