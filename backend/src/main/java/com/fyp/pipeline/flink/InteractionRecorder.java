package com.fyp.pipeline.flink;

import java.util.Map;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.fyp.model.Attribution;
import com.fyp.model.Interaction;
import com.fyp.model.ProvenanceRecord;
import com.fyp.model.RunVertices;
import com.fyp.model.UnifiedEvent;

/**
 * Records how much one operator passed to the next, per hashtag per interval, counted where it happened.
 *
 * <p>The stream is keyed by category, so the key is the {@code category} column and the elements in an
 * interval are exactly the events that moved over this edge in it. That is what makes the row a
 * measurement rather than a copy: the same number used to be derived from the window's own element list
 * after the fact, which left the graph unable to tell a filtered event from a dropped one.
 *
 * <p>{@code attribution} names the vertex on the other side of the edge, not a user, because that is the
 * only origin this operator can honestly claim. Per-contributor splits live on the window's state row and
 * on {@code window_outputs}. See {@code docs/TIN.md}, "keys follow the box".
 *
 * <p>An interval with no events never fires, so a hashtag that said nothing in a tick says nothing here.
 */
public final class InteractionRecorder
        extends ProcessWindowFunction<UnifiedEvent, ProvenanceRecord, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final RunVertices chain;
    private final String fromVertex;
    private final String toVertex;

    public InteractionRecorder(RunVertices chain, String fromVertex, String toVertex) {
        this.chain = chain;
        this.fromVertex = fromVertex;
        this.toVertex = toVertex;
    }

    @Override
    public void process(String category, Context context, Iterable<UnifiedEvent> passed,
            Collector<ProvenanceRecord> out) {
        long quantity = 0;
        for (UnifiedEvent ignored : passed) {
            quantity++;
        }
        out.collect(ProvenanceRecord.ofInteraction(new Interaction(chain.runId(), fromVertex, toVertex,
                category, context.window().getEnd(), quantity,
                new Attribution(Map.of(fromVertex, quantity)))));
    }
}
