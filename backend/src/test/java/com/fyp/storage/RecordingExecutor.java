package com.fyp.storage;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** Captures every statement so repositories and the sink can be tested without a database. */
public final class RecordingExecutor implements SqlExecutor {

    public record Call(String sql, List<?> args) {
    }

    public final List<String> executed = new ArrayList<>();
    public final List<Call> updates = new ArrayList<>();
    public final List<Call> queries = new ArrayList<>();
    public final List<Call> batches = new ArrayList<>();
    private final Deque<List<Row>> queuedResults = new ArrayDeque<>();
    public boolean closed;

    @Override
    public void execute(String sql) {
        executed.add(sql);
    }

    @Override
    public int update(String sql, List<Object> params) {
        updates.add(new Call(sql, params));
        return 1;
    }

    @Override
    public int insertBatch(String sql, List<List<Object>> rows) {
        batches.add(new Call(sql, rows));
        return rows.size();
    }

    @Override
    public List<Row> query(String sql, List<Object> params) {
        queries.add(new Call(sql, params));
        return queuedResults.isEmpty() ? List.of() : queuedResults.pop();
    }

    @Override
    public void close() {
        closed = true;
    }

    public void queueRows(Row... rows) {
        queuedResults.add(List.of(rows));
    }

    public Call lastUpdate() {
        return updates.get(updates.size() - 1);
    }

    public Call lastBatch() {
        return batches.get(batches.size() - 1);
    }

    /** Rows of every batch sent so far, flattened, so a test can count flushes by size. */
    public List<List<?>> allBatchedRows() {
        List<List<?>> rows = new ArrayList<>();
        batches.forEach(call -> call.args().forEach(row -> rows.add((List<?>) row)));
        return rows;
    }
}
