package com.fyp.pipeline.flink;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import com.fyp.model.Tweet;
import com.fyp.model.UnifiedEvent;

/**
 * Turns one tweet into one event per hashtag.
 *
 * <p>Ports the {@code extract_hashtags} plus {@code flatten_hashtags} pair from
 * {@code legacy/src/fyp_attribution/main.py}, which needed a JSON string in between only because
 * PyFlink could not serialise a list. The intermediate serialisation is gone.
 */
public final class HashtagExtractor implements FlatMapFunction<Tweet, UnifiedEvent> {

    private static final long serialVersionUID = 1L;

    private static final Pattern HASHTAG = Pattern.compile("#(\\w+)", Pattern.UNICODE_CHARACTER_CLASS);

    @Override
    public void flatMap(Tweet tweet, Collector<UnifiedEvent> out) {
        for (String category : extractCategories(tweet.text())) {
            out.collect(UnifiedEvent.of(tweet.userId(), category, tweet.timestamp()));
        }
    }

    /**
     * The hashtags in a tweet body, upper-cased, in order of appearance. Repeated tags stay
     * repeated: two mentions of the same tag in one tweet are two contributions.
     */
    public static List<String> extractCategories(String text) {
        List<String> categories = new ArrayList<>();
        if (text == null) {
            return categories;
        }
        Matcher matcher = HASHTAG.matcher(text);
        while (matcher.find()) {
            categories.add(matcher.group(1).toUpperCase(Locale.ROOT));
        }
        return categories;
    }
}
