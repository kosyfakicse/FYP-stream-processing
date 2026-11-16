package com.fyp.pipeline.flink;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import com.fyp.model.UnifiedEvent;

/**
 * Reads the event time off a {@link UnifiedEvent} so windows are keyed to when the tweet was
 * posted, not to when the job happened to process it.
 *
 * <p>Ports {@code TweetTimestampAssigner}, which pulled the same value out of tuple index 2.
 */
public final class EventTimestampAssigner implements SerializableTimestampAssigner<UnifiedEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public long extractTimestamp(UnifiedEvent event, long recordTimestamp) {
        return event.timestamp();
    }
}
