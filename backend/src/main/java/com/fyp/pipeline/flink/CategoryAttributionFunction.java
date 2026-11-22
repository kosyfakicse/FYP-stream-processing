package com.fyp.pipeline.flink;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.fyp.model.Attribution;
import com.fyp.model.UnifiedEvent;
import com.fyp.model.WindowOutput;

/**
 * Fires one {@link WindowOutput} per hashtag per window: the total counted, and how much each
 * contributor added to it.
 *
 * <p>The total counts events, not tweets -- the stream is one element per hashtag, so a contributor that
 * named the tag twice in one tweet is two here. The tweet behind those events is deliberately not
 * reconstructed from this list: it is measured upstream by {@code BoxRecorder}, on the stream that still
 * carries whole tweets. Counting it here could only ever report the tweets that mentioned this tag, which
 * is what made the two edges of the chain look like the same measurement.
 *
 * <p>Ports {@code CategoryAttributionProcessFunction} from
 * {@code legacy/src/fyp_attribution/pipeline/flink.py}. The input stream is already keyed by category,
 * so the key is the hashtag.
 */
public final class CategoryAttributionFunction
        extends ProcessWindowFunction<UnifiedEvent, WindowOutput, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String category, Context context, Iterable<UnifiedEvent> events,
            Collector<WindowOutput> out) {
        out.collect(aggregate(category, context.window().getStart(), context.window().getEnd(), events));
    }

    /**
     * Sums each contributor's quantity over the window. Kept separate from {@link #process} so the
     * attribution can be tested without a running job.
     */
    public static WindowOutput aggregate(String category, long windowStart, long windowEnd,
            Iterable<UnifiedEvent> events) {
        Map<String, Long> quantities = new HashMap<>();
        long outputCount = 0;
        for (UnifiedEvent event : events) {
            quantities.merge(event.contributor(), event.quantity(), Long::sum);
            outputCount++;
        }
        return new WindowOutput(windowStart, windowEnd, category, outputCount,
                new Attribution(quantities));
    }
}
