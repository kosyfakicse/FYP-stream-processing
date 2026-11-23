package com.fyp.model;

/**
 * One hashtag contribution by one contributor at one instant: the unit the window aggregates over.
 *
 * <p>A tweet carrying three hashtags becomes three events, so this is a data class in memory only,
 * not a table. It follows the Event Schema in docs/design/schema.md.
 *
 * <p>The field is called {@code contributor} rather than {@code source} on purpose. In the stored network
 * "source" belongs to an operator -- {@code tin_interactions.source_vertex} names the vertex that handed
 * data over -- while this holds a user id. One word carrying both senses is what made two contradictory
 * readings of {@code attribution_json} each look correct; see {@code docs/TIN.md}, "The word source means
 * two things in this schema".
 *
 * @param eventId stable identifier for this contribution
 * @param contributor the user this event is attributed to, which is the key of the attribution map
 * @param quantity how much this contributor added, always 1 at event level
 * @param category the upper-cased hashtag, without the leading hash
 * @param timestamp event time in epoch milliseconds
 */
public record UnifiedEvent(String eventId, String contributor, long quantity, String category,
        long timestamp) {

    public static UnifiedEvent of(String contributor, String category, long timestamp) {
        return new UnifiedEvent(contributor + "@" + timestamp + "#" + category, contributor, 1L, category,
                timestamp);
    }
}
