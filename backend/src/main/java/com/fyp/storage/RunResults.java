package com.fyp.storage;

import java.util.List;

import com.fyp.model.WindowOutput;

/**
 * Everything one run produced, which is what the caller asks for with a run id.
 *
 * @param runId the run these results belong to
 * @param outputs window results, ordered by window start then hashtag
 */
public record RunResults(String runId, List<WindowOutput> outputs) {

    public RunResults {
        outputs = List.copyOf(outputs);
    }

    /** Total events counted across every window and hashtag of the run. */
    public long totalCounted() {
        return outputs.stream().mapToLong(WindowOutput::outputCount).sum();
    }
}
