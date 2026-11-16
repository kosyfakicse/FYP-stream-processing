package com.fyp.storage;

import java.util.List;

/**
 * One row read back from storage, addressed by column position.
 *
 * <p>Deliberately narrow: it exists so {@link SqlExecutor} can hand rows to the repositories
 * without leaking JDBC types into the rest of the codebase.
 *
 * @param values column values in the order they were selected
 */
public record Row(List<Object> values) {

    public String getString(int index) {
        Object value = value(index);
        return value == null ? null : value.toString();
    }

    public long getLong(int index) {
        return asNumber(index).longValue();
    }

    public int getInt(int index) {
        return asNumber(index).intValue();
    }

    public boolean getBoolean(int index) {
        Object value = value(index);
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new IllegalStateException("Column " + index + " is not a boolean: " + value);
    }

    /** For column types with no accessor yet, such as TIMESTAMP. */
    public Object getObject(int index) {
        return value(index);
    }

    private Number asNumber(int index) {
        Object value = value(index);
        if (value instanceof Number number) {
            return number;
        }
        throw new IllegalStateException("Column " + index + " is not a number: " + value);
    }

    private Object value(int index) {
        if (index < 0 || index >= values.size()) {
            throw new IllegalStateException("Column " + index + " is out of range, row has "
                    + values.size() + " values: " + values);
        }
        return values.get(index);
    }
}
