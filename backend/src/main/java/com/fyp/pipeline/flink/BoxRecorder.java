package com.fyp.pipeline.flink;

import java.util.Map;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.fyp.model.Attribution;
import com.fyp.model.Interaction;
import com.fyp.model.ProvenanceRecord;
import com.fyp.model.RunVertices;
import com.fyp.model.State;

/**
 * Records what one vertex's box holds, and what it passed on, once per interval.
 *
 * <p>This is the measurement the graph was missing: until now every interaction was inferred from what the
 * window happened to hold, so a tweet that named no hashtag reached no downstream operator at all and
 * appeared nowhere. Counting on the stream that feeds the operator measures the traffic instead of the
 * residue -- here the tweets {@code K1} read, which is also the {@code K1 -> M1} quantity.
 *
 * <p>A state row is the box, so it is <b>cumulative</b> and lives in global state, while the interaction
 * row is the delta of the same interval. Keeping both in one operator is what makes them agree: two
 * separate counts of one stream would drift the moment a branch filtered.
 *
 * <p>Only intervals in which the box changed get a row. Flink does not call a window function for an
 * interval with no elements, so that rule costs no code here -- and it is why an idle minute over a
 * day-long dataset writes nothing instead of a row per vertex per second.
 *
 * <p>The box is deliberately tag-blind and its attribution is empty: {@code K1} counts rows and {@code M1}
 * counts events, neither holds a per-user tally, so a user map on their row would be data the pipeline
 * never had. See {@code docs/TIN.md}, "keys follow the box".
 */
public final class BoxRecorder<T> extends ProcessWindowFunction<T, ProvenanceRecord, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private static final ValueStateDescriptor<Long> CONTENTS =
            new ValueStateDescriptor<>("boxContentsSoFar", Long.class);

    private final RunVertices chain;
    private final String vertexId;

    /** The operator this box feeds, or null when the interaction out of it is written elsewhere. */
    private final String downstreamVertexId;

    public BoxRecorder(RunVertices chain, String vertexId, String downstreamVertexId) {
        this.chain = chain;
        this.vertexId = vertexId;
        this.downstreamVertexId = downstreamVertexId;
    }

    @Override
    public void process(String key, Context context, Iterable<T> arrived, Collector<ProvenanceRecord> out)
            throws Exception {
        long intervalStart = context.window().getStart();
        long intervalEnd = context.window().getEnd();
        long passed = count(arrived);
        // Global, not window, state: the box is what the vertex holds so far, which survives the
        // interval the row describes. A window-state copy would reset every tick and report a delta.
        ValueState<Long> contents = context.globalState().getState(CONTENTS);
        long soFar = passed + valueOrZero(contents.value());
        contents.update(soFar);

        out.collect(ProvenanceRecord.ofState(new State(chain.runId(), vertexId, null, intervalStart,
                intervalEnd, soFar, Attribution.empty(), intervalEnd)));
        if (downstreamVertexId != null) {
            out.collect(ProvenanceRecord.ofInteraction(new Interaction(chain.runId(), vertexId,
                    downstreamVertexId, null, intervalEnd, passed,
                    new Attribution(Map.of(vertexId, passed)))));
        }
    }

    private static long count(Iterable<?> elements) {
        long total = 0;
        for (Object ignored : elements) {
            total++;
        }
        return total;
    }

    private static long valueOrZero(Long value) {
        return value == null ? 0L : value;
    }
}
