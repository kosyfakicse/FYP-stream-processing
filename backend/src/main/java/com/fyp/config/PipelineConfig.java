package com.fyp.config;

/**
 * Attribution and storage settings.
 *
 * @param windowSizeSeconds width of each tumbling window
 * @param stateTickSeconds how often each vertex writes a row about what its box holds
 * @param interactionTickSeconds how often each edge writes a row about what it passed on
 * @param batchSize provenance records buffered per DuckDB batch write
 * @param duckdbFile path to the DuckDB file, relative to the directory the job runs from
 */
public record PipelineConfig(int windowSizeSeconds, int stateTickSeconds, int interactionTickSeconds,
        int batchSize, String duckdbFile) {

    /**
     * A tick that does not divide the window cannot be summed into it.
     *
     * <p>Tumbling intervals are anchored to the epoch, so a 7-second tick straddles two 60-second
     * windows and the invariant "the interactions into a box add up to what it reports" fails on the
     * boundary tick while every other row looks correct. Nothing in the stored data shows which
     * configuration produced that, so the check runs before the job is built rather than after.
     *
     * @throws IllegalStateException naming both values, because a bare "invalid config" cannot be acted on
     */
    public void validateTicks() {
        requireDivides("state_tick_seconds", stateTickSeconds);
        requireDivides("interaction_tick_seconds", interactionTickSeconds);
    }

    private void requireDivides(String key, int tickSeconds) {
        if (tickSeconds <= 0) {
            throw new IllegalStateException(key + " must be positive, is " + tickSeconds
                    + " (window_size_seconds is " + windowSizeSeconds + ")");
        }
        if (windowSizeSeconds % tickSeconds != 0) {
            throw new IllegalStateException(key + " = " + tickSeconds + " does not divide"
                    + " window_size_seconds = " + windowSizeSeconds + ", so the tick rows of one window"
                    + " would straddle two of them");
        }
    }
}
