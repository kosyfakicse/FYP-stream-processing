package com.fyp.model;

/**
 * One row of the input dataset, as read from the CSV.
 *
 * @param userId who posted it, the provenance source
 * @param text tweet body, may contain several hashtags
 * @param timestamp event time in epoch milliseconds
 */
public record Tweet(String userId, String text, long timestamp) {
}
