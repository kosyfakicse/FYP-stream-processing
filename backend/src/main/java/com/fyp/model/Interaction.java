package com.fyp.model;

import java.util.Map;

/**
 * How much one operator passed to the next, for one category in one interval.
 *
 * <p>The row is an interval, not an element: the reference model counts one interaction per element, but
 * this job aggregates, so one row carries the total that reached the far operator in that interval. That is
 * what makes {@code (run_id, source_vertex, target_vertex, category, timestamp)} a key rather than a hint.
 *
 * <p>{@code timestamp} is the interval's own end, the same instant {@code tin_states.fired_at} records for
 * the box that produced it, so a state row and its interaction rows line up without joining on the interval.
 *
 * <p>Its {@code attribution} names the vertex on the other side of the edge, never a user. The two key
 * domains are compared on quantity only -- see {@code docs/TIN.md}, "keys follow the box".
 *
 * @param quantity total passed over the edge, the sum of {@code attribution}
 */
public record Interaction(String runId, String sourceVertex, String targetVertex, String category,
        long timestamp, long quantity, Attribution attribution) {

    /**
     * What the sink was handed for one fired window: the {@code W1 -> S1} interaction.
     *
     * <p>This is the only interaction the firing can describe. The other two edges of the chain are
     * measured at their own upstream operators now, because the window's element list knows nothing about a
     * tweet that named no hashtag -- inferring {@code K1 -> M1} from it was the defect that counted one
     * observation four times.
     */
    public static Interaction ofFiring(RunVertices chain, WindowOutput output) {
        long events = output.outputCount();
        return new Interaction(chain.runId(), chain.windowVertex(), chain.sinkVertex(),
                output.category(), output.windowEnd(), events,
                new Attribution(Map.of(chain.windowVertex(), events)));
    }
}
