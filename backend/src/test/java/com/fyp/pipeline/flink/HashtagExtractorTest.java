package com.fyp.pipeline.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fyp.model.Tweet;
import com.fyp.model.UnifiedEvent;

class HashtagExtractorTest {

    private final HashtagExtractor extractor = new HashtagExtractor();

    @Test
    void extractsEachTagInOrderOfAppearance() {
        assertThat(HashtagExtractor.extractCategories("Loving #python today")).containsExactly("PYTHON");
        assertThat(HashtagExtractor.extractCategories("#python and #streaming"))
                .containsExactly("PYTHON", "STREAMING");
        assertThat(HashtagExtractor.extractCategories("#UST #DATA")).containsExactly("UST", "DATA");
    }

    @Test
    void stripsPunctuationAndLeadingHash() {
        assertThat(HashtagExtractor.extractCategories("Who else uses #AI?")).containsExactly("AI");
        assertThat(HashtagExtractor.extractCategories("Loving #AI!")).containsExactly("AI");
        assertThat(HashtagExtractor.extractCategories("#python3 is fine")).containsExactly("PYTHON3");
    }

    @Test
    void emitsNothingWhenThereIsNoTag() {
        assertThat(HashtagExtractor.extractCategories("no tags here")).isEmpty();
        assertThat(HashtagExtractor.extractCategories("#")).isEmpty();
        assertThat(HashtagExtractor.extractCategories("#-not-a-tag")).isEmpty();
        assertThat(HashtagExtractor.extractCategories(null)).isEmpty();
    }

    @Test
    void keepsRepeatedTagsAsSeparateContributions() {
        assertThat(HashtagExtractor.extractCategories("#ai and #ai again")).containsExactly("AI", "AI");
    }

    @Test
    void oneTweetBecomesOneEventPerHashtag() throws Exception {
        List<UnifiedEvent> events = flatten(new Tweet("user_4", "#python and #streaming", 1_600L));

        assertThat(events).containsExactly(
                new UnifiedEvent("user_4@1600#PYTHON", "user_4", 1L, "PYTHON", 1_600L),
                new UnifiedEvent("user_4@1600#STREAMING", "user_4", 1L, "STREAMING", 1_600L));
    }

    @Test
    void aTaglessTweetEmitsNoEvents() throws Exception {
        assertThat(flatten(new Tweet("user_1", "just text", 1_000L))).isEmpty();
    }

    private List<UnifiedEvent> flatten(Tweet tweet) throws Exception {
        RecordingCollector<UnifiedEvent> collector = new RecordingCollector<>();
        extractor.flatMap(tweet, collector);
        return collector.records();
    }
}
