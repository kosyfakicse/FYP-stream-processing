package com.fyp.model;

/**
 * One hashtag contribution by one source at one instant: the unit the window aggregates over.
 *
 * <p>A tweet carrying three hashtags becomes three events, so this is a data class in memory only,
 * not a table. It follows the Event Schema in docs/design/schema.md.
 *
 * @param eventId stable identifier for this contribution
 * @param source where the event came from, the key of the attribution map
 * @param quantity how much this source contributed, always 1 at event level
 * @param category the upper-cased hashtag, without the leading hash
 * @param timestamp event time in epoch milliseconds
 */
public record UnifiedEvent(String eventId, String source, long quantity, String category, long timestamp) {

    public static UnifiedEvent of(String source, String category, long timestamp) {
        return new UnifiedEvent(source + "@" + timestamp + "#" + category, source, 1L, category, timestamp);
    }
}
