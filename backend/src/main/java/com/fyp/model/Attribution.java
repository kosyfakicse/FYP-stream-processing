package com.fyp.model;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * How much each source contributed to one window output: the how-much half of provenance, stored in
 * {@code window_outputs.attribution_json} as {@code {"user_1": 3, "user_2": 1}}.
 *
 * <p>Keys are kept sorted so the JSON for the same attribution is byte-identical across runs, which
 * makes stored rows comparable and tests readable.
 *
 * @param sourceQuantities quantity contributed per source id
 */
public record Attribution(Map<String, Long> sourceQuantities) {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Long>> QUANTITY_MAP = new TypeReference<>() {
    };

    public Attribution {
        sourceQuantities = Collections.unmodifiableMap(new TreeMap<>(sourceQuantities));
    }

    public long total() {
        return sourceQuantities.values().stream().mapToLong(Long::longValue).sum();
    }

    public long quantityOf(String source) {
        return sourceQuantities.getOrDefault(source, 0L);
    }

    public String toJson() {
        try {
            return JSON.writeValueAsString(sourceQuantities);
        } catch (Exception e) {
            throw new IllegalStateException("Attribution is not serialisable: " + sourceQuantities, e);
        }
    }

    public static Attribution fromJson(String json) {
        try {
            return new Attribution(JSON.readValue(json, QUANTITY_MAP));
        } catch (Exception e) {
            throw new IllegalStateException("Attribution json is not a source->quantity map: " + json, e);
        }
    }

    public static Attribution empty() {
        return new Attribution(Map.of());
    }
}
