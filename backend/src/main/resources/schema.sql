-- DuckDB schema, transcribed from docs/design/reference_schema.md.
--
-- The four tin_* tables are documented and created here, but no Java code reads or writes them at
-- this stage: provenance capture is a later phase. They exist so the database and the reference
-- schema stay in step.
--
-- Two deliberate deviations from the reference document, both in window_outputs:
--   1. output_id draws from a sequence. The document declares a plain INTEGER PRIMARY KEY, which
--      DuckDB does not fill in for an omitted column, and a second run starting again at 1 would
--      collide with the first. (DuckDB 2.0 accepts neither SERIAL nor GENERATED AS IDENTITY here,
--      so the sequence is named and referenced explicitly.)
--   2. category holds the hashtag the window was keyed by. The document predates hashtag keying
--      (see commit "new window output, keyby hashtag instead of user id") and has no column for it,
--      so without this the results of two hashtags in the same window are indistinguishable.

CREATE TABLE IF NOT EXISTS runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    dataset VARCHAR NOT NULL,
    config_json VARCHAR,
    provenance_enabled BOOLEAN NOT NULL
);

-- Uploaded csvs are kept as files in the uploads directory, not as blobs: dataset_id names the file,
-- so the row is what makes an id mean something again after a restart.
--
-- expires_at is NULL when upload_ttl_days is zero, meaning keep until deleted by hand. Only
-- DatasetRepository.deleteExpired reads it, and nothing schedules that call yet.
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id VARCHAR PRIMARY KEY,
    uploaded_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP,
    original_name VARCHAR NOT NULL,
    size_bytes BIGINT NOT NULL,
    row_count BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS tin_vertices (
    vertex_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    vertex_type VARCHAR NOT NULL,
    description VARCHAR
);

CREATE TABLE IF NOT EXISTS tin_edges (
    source_vertex VARCHAR NOT NULL,
    target_vertex VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    PRIMARY KEY (source_vertex, target_vertex, run_id)
);

CREATE TABLE IF NOT EXISTS tin_interactions (
    interaction_id INTEGER PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    source_vertex VARCHAR NOT NULL,
    target_vertex VARCHAR NOT NULL,
    timestamp BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    attribution_json VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS tin_states (
    state_id INTEGER PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    vertex_id VARCHAR NOT NULL,
    time_start BIGINT NOT NULL,
    time_end BIGINT,
    quantity INTEGER NOT NULL,
    attribution_json VARCHAR NOT NULL,
    fired_at BIGINT
);

CREATE SEQUENCE IF NOT EXISTS window_outputs_output_id_seq START 1;

CREATE TABLE IF NOT EXISTS window_outputs (
    output_id INTEGER PRIMARY KEY DEFAULT nextval('window_outputs_output_id_seq'),
    run_id VARCHAR NOT NULL,
    vertex_id VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    window_start BIGINT NOT NULL,
    window_end BIGINT NOT NULL,
    output_count INTEGER NOT NULL,
    attribution_json VARCHAR NOT NULL
);
