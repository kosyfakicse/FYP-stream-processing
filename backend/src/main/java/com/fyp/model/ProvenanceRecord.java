package com.fyp.model;

/**
 * One thing the sink has been told to store: a fired window, what a vertex held over an interval, or
 * how much an edge carried.
 *
 * <p>The three shapes share no fields, and they still have to arrive over one stream, because
 * {@code DuckDbSinkWriter} is the only component in the job that opens the database. DuckDB takes one
 * read-write process per file and the API server is that process during a run, and the protection
 * against a retried batch storing a duplicate is one flush plus a natural unique key -- both of which
 * stop being true the moment an operator reaches for a connection of its own. So a count taken at any
 * vertex travels downstream as one of these and is written where every other row is written.
 *
 * <p>Exactly one of the three payloads is set, chosen by {@link #kind()}, and the factories are the way
 * to build one.
 *
 * @param kind which payload is set
 * @param firing the window result as the firing reported it; set for {@link Kind#FIRING}
 * @param state what one vertex held over one interval; set for {@link Kind#STATE}
 * @param interaction one edge's transfer at one instant; set for {@link Kind#INTERACTION}
 */
public record ProvenanceRecord(Kind kind, WindowOutput firing, State state, Interaction interaction) {

    public enum Kind {
        FIRING,
        STATE,
        INTERACTION
    }

    public static ProvenanceRecord ofFiring(WindowOutput firing) {
        return new ProvenanceRecord(Kind.FIRING, firing, null, null);
    }

    public static ProvenanceRecord ofState(State state) {
        return new ProvenanceRecord(Kind.STATE, null, state, null);
    }

    public static ProvenanceRecord ofInteraction(Interaction interaction) {
        return new ProvenanceRecord(Kind.INTERACTION, null, null, interaction);
    }
}
