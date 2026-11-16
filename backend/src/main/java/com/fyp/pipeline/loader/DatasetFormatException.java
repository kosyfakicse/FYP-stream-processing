package com.fyp.pipeline.loader;

/**
 * The dataset bytes are not a valid tweets CSV.
 *
 * <p>Exists so callers can tell "the data you gave me is wrong" apart from "my own storage failed",
 * because {@code DuckDbExecutor} throws plain {@link IllegalStateException} for every SQL problem
 * too. Subclasses IllegalStateException so the CLI paths keep working unchanged.
 */
public class DatasetFormatException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    public DatasetFormatException(String message) {
        super(message);
    }

    public DatasetFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}
