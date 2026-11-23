package com.fyp.pipeline.flink;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import com.fyp.model.Tweet;

/**
 * Reads the event time off a {@link Tweet} so windows are keyed to when the tweet was posted, not to
 * when the job happened to process it.
 *
 * <p>Assigned on the source stream, before {@link HashtagExtractor}: every stage downstream then knows
 * which interval its elements belong to, including the extractor, which cannot bucket anything once the
 * tweets have become events. The extractor's output inherits the record timestamp, so the events carry
 * the same value without a second assigner.
 *
 * <p>Ports {@code TweetTimestampAssigner}, which pulled the same value out of tuple index 2.
 */
public final class TweetTimestampAssigner implements SerializableTimestampAssigner<Tweet> {

    private static final long serialVersionUID = 1L;

    @Override
    public long extractTimestamp(Tweet tweet, long recordTimestamp) {
        return tweet.timestamp();
    }
}
