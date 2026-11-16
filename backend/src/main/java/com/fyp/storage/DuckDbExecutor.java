package com.fyp.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC against the {@code duckdb_jdbc} driver, which registers itself through
 * {@code META-INF/services}, so no {@code Class.forName} is needed.
 *
 * <p>DuckDB allows one read-write process per file; the connection is held for the lifetime of the
 * instance and closed by {@link #close()}.
 */
public final class DuckDbExecutor implements SqlExecutor {

    private final Connection connection;

    public DuckDbExecutor(String jdbcUrl) {
        try {
            this.connection = DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to open DuckDB at " + jdbcUrl, e);
        }
    }

    public DuckDbExecutor(Path dbFile) {
        this(urlFor(dbFile));
    }

    /** No file on disk, so tests do not fight over locks or leave artifacts behind. */
    public static DuckDbExecutor inMemory() {
        return new DuckDbExecutor("jdbc:duckdb:");
    }

    /** The URL form the driver accepts, and the same string the sink is handed so it can reconnect. */
    public static String urlFor(Path dbFile) {
        Path absolute = dbFile.toAbsolutePath().normalize();
        try {
            Files.createDirectories(absolute.getParent());
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create the directory for " + absolute, e);
        }
        return "jdbc:duckdb:" + absolute.toString().replace('\\', '/');
    }

    @Override
    public void execute(String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to execute: " + sql, e);
        }
    }

    @Override
    public int update(String sql, List<Object> params) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, params);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to update: " + sql + " with " + params, e);
        }
    }

    @Override
    public int insertBatch(String sql, List<List<Object>> rows) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (List<Object> row : rows) {
                bind(statement, row);
                statement.addBatch();
            }
            statement.executeBatch();
            // The driver reports SUCCESS_NO_INFO rather than a per-row count, so the number of
            // rows handed to the batch is the only figure that is not misleading.
            return rows.size();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to batch-insert " + rows.size() + " rows: " + sql, e);
        }
    }

    @Override
    public List<Row> query(String sql, List<Object> params) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, params);
            try (ResultSet resultSet = statement.executeQuery()) {
                return readAll(resultSet);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query: " + sql + " with " + params, e);
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to close the DuckDB connection", e);
        }
    }

    private static void bind(PreparedStatement statement, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            statement.setObject(i + 1, params.get(i));
        }
    }

    private static List<Row> readAll(ResultSet resultSet) throws SQLException {
        ResultSetMetaData meta = resultSet.getMetaData();
        int columns = meta.getColumnCount();
        List<Row> rows = new ArrayList<>();
        while (resultSet.next()) {
            List<Object> values = new ArrayList<>(columns);
            for (int column = 1; column <= columns; column++) {
                values.add(resultSet.getObject(column));
            }
            rows.add(new Row(values));
        }
        return rows;
    }
}
