package com.fyp.model;

/**
 * What one vertex held over one interval: a row of {@code tin_states}, the S of the TIN tuple.
 *
 * <p>Stored per {@code category} as well as per vertex and window, because {@code W1} is keyed by
 * category and a single quantity mixing two hashtags could never be attributed back to either.
 *
 * @param runId the run the state belongs to
 * @param vertexId the operator holding it
 * @param category the key this state is about, upper-cased and without the leading hash
 * @param timeStart inclusive start of the interval, epoch milliseconds
 * @param timeEnd exclusive end of the interval, epoch milliseconds
 * @param quantity how much the vertex counted over the interval
 * @param attribution how much each source contributed to that quantity
 * @param firedAt when the state was taken, epoch milliseconds
 */
public record State(String runId, String vertexId, String category, long timeStart, long timeEnd,
        long quantity, Attribution attribution, long firedAt) {

    /**
     * The state a fired window leaves behind at the window operator, which is the only vertex that
     * reports a state so far.
     *
     * <p>{@code firedAt} is the window's own end: the event-time close. The job does not record the
     * wall-clock instant a window fired, so this claims nothing about when it happened.
     */
    public static State ofWindow(String runId, String vertexId, WindowOutput output) {
        return new State(runId, vertexId, output.category(), output.windowStart(), output.windowEnd(),
                output.outputCount(), output.attribution(), output.windowEnd());
    }
}
