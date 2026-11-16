package com.fyp.pipeline.flink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.util.Collector;

/** Captures what an operator emits so functions can be tested without a running job. */
final class RecordingCollector<T> implements Collector<T> {

    private final List<T> records = new ArrayList<>();

    @Override
    public void collect(T record) {
        records.add(record);
    }

    @Override
    public void close() {
    }

    List<T> records() {
        return records;
    }
}
