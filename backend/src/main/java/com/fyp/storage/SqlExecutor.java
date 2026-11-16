package com.fyp.storage;

import java.util.List;

/**
 * The only way the rest of the pipeline talks to the database.
 *
 * <p>Ports the {@code IDatabase} interface from
 * {@code legacy/src/fyp_attribution/pipeline/db_writer.py}, which declared {@code write} and
 * {@code bulk_write} but only ever had a printing implementation: the Python
 * {@code class DuckDB(IDatabase)} body was empty, so all of this is new.
 */
public interface SqlExecutor extends AutoCloseable {

    /** Runs a statement that returns nothing, such as DDL. */
    void execute(String sql);

    /** Runs one INSERT or UPDATE with positional parameters. Returns the rows affected. */
    int update(String sql, List<Object> params);

    /** Runs one INSERT once per row, as a single JDBC batch. Returns the rows written. */
    int insertBatch(String sql, List<List<Object>> rows);

    /** Runs a SELECT and returns every row. */
    List<Row> query(String sql, List<Object> params);

    @Override
    void close();
}
