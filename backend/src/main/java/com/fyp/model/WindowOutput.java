package com.fyp.model;

/**
 * One fired window: what was counted, over what interval, and who contributed it.
 *
 * <p>Ports the tuple returned by {@code CategoryAttributionProcessFunction} in
 * {@code legacy/src/fyp_attribution/pipeline/flink.py}, which had to hand the attribution over as a
 * JSON string because PyFlink serialised the stream element. The run id and operator id are not
 * here: they are attached when the row is written.
 *
 * @param windowStart inclusive window start, epoch milliseconds
 * @param windowEnd exclusive window end, epoch milliseconds
 * @param category the hashtag this window was keyed by
 * @param outputCount number of events in the window
 * @param attribution quantity contributed per source
 */
public record WindowOutput(long windowStart, long windowEnd, String category, long outputCount,
        Attribution attribution) {
}
