-- DuckDB schema, transcribed from the TIN reference schema and adjusted for this job.
--
-- All four tin_* tables are written by a run: the job records the operators it built, what each vertex's
-- box held per interval, and how much each edge carried per interval. A row is per interval rather than per
-- element, so one row summarizes a whole tick: K1 -> M1 counts tweets, M1 -> W1 counts hashtag events and
-- W1 -> S1 counts the results the window handed the sink.
--
-- Deliberate deviations from the reference document, each one a place the document predates this job:
--   1. Every surrogate id draws from a sequence. The document declares plain INTEGER PRIMARY KEY,
--      which DuckDB does not fill in for an omitted column. (DuckDB 2.0 accepts neither SERIAL nor
--      GENERATED AS IDENTITY here, so the sequences are named and referenced explicitly.)
--   2. category holds the hashtag the window was keyed by. The document has no column for it, so
--      without it the results of two hashtags in one window are indistinguishable -- on
--      window_outputs, tin_states and tin_interactions alike, since W1 is one vertex but fires one
--      state and one interaction per (window, category).
--   3. tin_vertices is keyed by (run_id, vertex_id), not vertex_id. Ids are minted per run by
--      counting operator types, so every run calls its window operator W1 and a global key would make
--      the second run fail to insert it.
--   4. Natural unique keys on the tables a run writes. The JDBC sink is at-least-once, so a retry must
--      collide with something rather than land as an extra row.
--   5. tin_states.category and tin_interactions.category are nullable, because a K1 or M1 row is about
--      tweets rather than one hashtag and no hashtag is the truth there. (window_outputs keeps its
--      NOT NULL: every row of that table is one hashtag's result.) MEASURED COST, accepted knowingly:
--      DuckDB treats two NULL categories as distinct, so the unique key above does NOT reject a retried
--      tag-blind row -- ProvenanceRepositoryTest names this in code. UNIQUE NULLS NOT DISTINCT is the
--      fix and duckdb_jdbc 2.0.0.1 rejects that syntax at parse time, so the guard covers the
--      category-bearing rows only.

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
    vertex_id VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    vertex_type VARCHAR NOT NULL,
    description VARCHAR,
    PRIMARY KEY (run_id, vertex_id)
);

CREATE TABLE IF NOT EXISTS tin_edges (
    source_vertex VARCHAR NOT NULL,
    target_vertex VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    PRIMARY KEY (source_vertex, target_vertex, run_id)
);

CREATE SEQUENCE IF NOT EXISTS tin_interactions_interaction_id_seq START 1;

CREATE TABLE IF NOT EXISTS tin_interactions (
    interaction_id INTEGER PRIMARY KEY DEFAULT nextval('tin_interactions_interaction_id_seq'),
    run_id VARCHAR NOT NULL,
    source_vertex VARCHAR NOT NULL,
    target_vertex VARCHAR NOT NULL,
    category VARCHAR,
    timestamp BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    attribution_json VARCHAR NOT NULL,
    UNIQUE (run_id, source_vertex, target_vertex, category, timestamp)
);

CREATE SEQUENCE IF NOT EXISTS tin_states_state_id_seq START 1;

CREATE TABLE IF NOT EXISTS tin_states (
    state_id INTEGER PRIMARY KEY DEFAULT nextval('tin_states_state_id_seq'),
    run_id VARCHAR NOT NULL,
    vertex_id VARCHAR NOT NULL,
    category VARCHAR,
    time_start BIGINT NOT NULL,
    time_end BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    attribution_json VARCHAR NOT NULL,
    fired_at BIGINT,
    UNIQUE (run_id, vertex_id, category, time_start, time_end)
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
