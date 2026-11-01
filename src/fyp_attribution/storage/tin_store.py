import duckdb
import os
import json
from loguru import logger
from typing import Optional, Dict, List, Any

class TINStore:
    # Database service class for the TIN persistence layer using DuckDB.

    def __init__(self, db_path: str = "data/attribution.duckdb") -> None:
        # Initialize the database connection and create the necessary tables if they do not exist
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.db_path = db_path
        self.conn = duckdb.connect(self.db_path)
        self._initialize_db()

    def _initialize_db(self) -> None:
        "Initializes the database by creating necessary tables if they do not exist."

        # Create the runs table if it does not exist
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id VARCHAR PRIMARY KEY,
                started_at TIMESTAMP NOT NULL,
                finished_at TIMESTAMP,
                dataset VARCHAR NOT NULL,
                config_json VARCHAR,
                provenance_enabled BOOLEAN NOT NULL
            );
        """)

        # Create the tin_vertices table if it does not exist
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tin_vertices (
                vertex_id VARCHAR PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                vertex_type VARCHAR NOT NULL,
                description VARCHAR
            );
        """)

        # Create the tin_edges table if it does not exist
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tin_edges (
                source_vertex VARCHAR NOT NULL,
                target_vertex VARCHAR NOT NULL,
                run_id VARCHAR NOT NULL,
                PRIMARY KEY (source_vertex, target_vertex, run_id)
            );
        """)

        # Create the tin_interactions table if it does not exist
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tin_interactions (
                interaction_id INTEGER PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                source_vertex VARCHAR NOT NULL,
                target_vertex VARCHAR NOT NULL,
                timestamp BIGINT NOT NULL,
                quantity INTEGER NOT NULL,
                attribution_json VARCHAR NOT NULL
            );
        """)

        # Create the tin_states table if it does not exist
        self.conn.execute("""
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
        """)

        # Create the window_outputs table if it does not exist
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS window_outputs (
                output_id INTEGER PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                vertex_id VARCHAR NOT NULL,
                window_start BIGINT NOT NULL,
                window_end BIGINT NOT NULL,
                output_count INTEGER NOT NULL,
                attribution_json VARCHAR NOT NULL
            );
        """)

        logger.info("All TIN tables have been successfully created.")

    def insert_run(self, run_id: str, started_at: str, dataset: str, config_json: Optional[str], provenance_enabled: bool) -> None:
        # Inserts a new run into the runs table
        self.conn.execute("""
            INSERT INTO runs (run_id, started_at, dataset, config_json, provenance_enabled)
            VALUES (?, ?, ?, ?, ?);
        """, (run_id, started_at, dataset, config_json, provenance_enabled))

    def insert_vertex(self, vertex_id: str, run_id: str, vertex_type: str, description: Optional[str]) -> None:
        # Inserts a new vertex into the tin_vertices table
        self.conn.execute("""
            INSERT INTO tin_vertices (vertex_id, run_id, vertex_type, description)
            VALUES (?, ?, ?, ?);
        """, (vertex_id, run_id, vertex_type, description))

    def insert_edge(self, source_vertex: str, target_vertex: str, run_id: str) -> None:
        # Inserts a new edge into the tin_edges table
        self.conn.execute("""
            INSERT INTO tin_edges (source_vertex, target_vertex, run_id)
            VALUES (?, ?, ?);
        """, (source_vertex, target_vertex, run_id))

    def insert_interaction(self, interaction_id: int, run_id: str, source_vertex: str, target_vertex: str, timestamp: int, quantity: int, attribution_json: Dict[str, Any]) -> None:
        # Inserts a new interaction into the tin_interactions table
        self.conn.execute("""
            INSERT INTO tin_interactions (interaction_id, run_id, source_vertex, target_vertex, timestamp, quantity, attribution_json)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """, (interaction_id, run_id, source_vertex, target_vertex, timestamp, quantity, json.dumps(attribution_json)))

    def insert_state(self, state_id: int, run_id: str, vertex_id: str, time_start: int, time_end: Optional[int], quantity: int, attribution_json: Dict[str, Any], fired_at: Optional[int]) -> None:
        # Inserts a new state into the tin_states table
        self.conn.execute("""
            INSERT INTO tin_states (state_id, run_id, vertex_id, time_start, time_end, quantity, attribution_json, fired_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (state_id, run_id, vertex_id, time_start, time_end, quantity, json.dumps(attribution_json), fired_at))

    def insert_window_output(self, output_id: int, run_id: str, vertex_id: str, window_start: int, window_end: int, output_count: int, attribution_json: Dict[str, Any]) -> None:
        # Inserts a new window output into the window_outputs table
        self.conn.execute("""
            INSERT INTO window_outputs (output_id, run_id, vertex_id, window_start, window_end, output_count, attribution_json)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """, (output_id, run_id, vertex_id, window_start, window_end, output_count, json.dumps(attribution_json)))

    def close(self) -> None:
        # Closes the database connection
        if self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed.")

if __name__ == "__main__":
    # Test the TINStore class
    store = TINStore(db_path="data/test_attribution.duckdb")
    
    # Test inserting a window output
    store.insert_window_output(
        output_id=1,
        run_id="run-test-01",
        vertex_id="W1",
        window_start=1735696800000,
        window_end=1735696860000,
        output_count=4,
        attribution_json={"user_1": 2, "user_2": 1, "user_4": 1}
    )
    
    # Query the data to verify
    results = store.conn.execute("SELECT * FROM window_outputs").fetchall()
    print(f"Data in DB: {results}")
    
    store.close()