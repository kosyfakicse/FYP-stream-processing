package com.fyp.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Creates the tables from {@code schema.sql} on the classpath.
 *
 * <p>Every statement is {@code CREATE TABLE IF NOT EXISTS}, so applying the schema to a database
 * that already has it is a no-op and a run can be replayed against the same file.
 */
public final class SchemaInitializer {

    public static final String RESOURCE = "schema.sql";

    private SchemaInitializer() {
    }

    public static void applyTo(SqlExecutor sql) {
        for (String statement : statements(read())) {
            sql.execute(statement);
        }
    }

    /**
     * Splits the script so one bad table is reported as itself rather than as a giant statement.
     *
     * <p>Comment lines are dropped first, because a semicolon in prose would otherwise cut a
     * {@code CREATE TABLE} in half.
     */
    static List<String> statements(String script) {
        String withoutComments = script.lines()
                .filter(line -> !line.trim().startsWith("--"))
                .collect(Collectors.joining("\n"));
        return Arrays.stream(withoutComments.split(";"))
                .map(String::trim)
                .filter(statement -> !statement.isEmpty())
                .toList();
    }

    private static String read() {
        try (InputStream in = SchemaInitializer.class.getClassLoader().getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Schema resource not found on classpath: " + RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read " + RESOURCE, e);
        }
    }
}
