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
 * source contributed to it.
 *
 * <p>Ports {@code CategoryAttributionProcessFunction} from
 * {@code legacy/src/fyp_attribution/pipeline/flink.py}. The input stream is already keyed by
 * category, so the key is the hashtag.
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
     * Sums each source's quantity over the window. Kept separate from {@link #process} so the
     * attribution can be tested without a running job.
     */
    public static WindowOutput aggregate(String category, long windowStart, long windowEnd,
            Iterable<UnifiedEvent> events) {
        Map<String, Long> quantities = new HashMap<>();
        long outputCount = 0;
        for (UnifiedEvent event : events) {
            quantities.merge(event.source(), event.quantity(), Long::sum);
            outputCount++;
        }
        return new WindowOutput(windowStart, windowEnd, category, outputCount, new Attribution(quantities));
    }
}
