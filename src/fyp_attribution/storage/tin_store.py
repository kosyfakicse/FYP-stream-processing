import duckdb
import os
import threading
from loguru import logger
from typing import Optional

class TINStore:
    # Database service class for the TIN persistence layer using DuckDB.

    _instance = None
    _lock = threading.Lock()

    # Singleton pattern implementation to ensure only one instance of TINStore exists
    # This is important for managing the database connection and ensuring thread safety.
    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(TINStore, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
        
    def __init__(self, db_path: str = "data/attribution.duckdb") -> None:
        # The database file will be created at the specified path if it does not already exist.
        if getattr(self, "_initialized", False):
            return

        # Ensure the directory for the database file exists
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.db_path = db_path
        self.conn = duckdb.connect(self.db_path)
        self._initialize_db()
        self._initialized = True

    def _initialize_db(self) -> None:
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
                interaction_id BIGINT PRIMARY KEY,
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
                state_id BIGINT PRIMARY KEY,
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
                output_id BIGINT PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                vertex_id VARCHAR NOT NULL,
                window_start BIGINT NOT NULL,
                window_end BIGINT NOT NULL,
                output_count INTEGER NOT NULL,
                attribution_json VARCHAR NOT NULL
            );
        """)

        logger.info("All TIN tables have been successfully created.")

    def insert_run(self, run_id: str, started_at: str, dataset: str, config_json: Optional[str], provenance_enabled: bool) -> str:
        # Inserts a new run into the runs table
        self.conn.execute("""
            INSERT INTO runs (run_id, started_at, dataset, config_json, provenance_enabled)
            VALUES (?, ?, ?, ?, ?);
        """, (run_id, started_at, dataset, config_json, provenance_enabled))
        return run_id

    def insert_vertex(self, vertex_id: str, run_id: str, vertex_type: str, description: Optional[str]) -> str:
        # Inserts a new vertex into the tin_vertices table
        self.conn.execute("""
            INSERT INTO tin_vertices (vertex_id, run_id, vertex_type, description)
            VALUES (?, ?, ?, ?);
        """, (vertex_id, run_id, vertex_type, description))
        return vertex_id

    def insert_edge(self, source_vertex: str, target_vertex: str, run_id: str) -> bool:
        # Inserts a new edge into the tin_edges table
        self.conn.execute("""
            INSERT INTO tin_edges (source_vertex, target_vertex, run_id)
            VALUES (?, ?, ?);
        """, (source_vertex, target_vertex, run_id))
        return True

    def insert_interaction(self, interaction_id: int, run_id: str, source_vertex: str, target_vertex: str, timestamp: int, quantity: int, attribution_json: str) -> int:
        # Inserts a new interaction into the tin_interactions table
        self.conn.execute("""
            INSERT INTO tin_interactions (interaction_id, run_id, source_vertex, target_vertex, timestamp, quantity, attribution_json)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """, (interaction_id, run_id, source_vertex, target_vertex, timestamp, quantity, attribution_json))
        return interaction_id

    def insert_state(self, state_id: int, run_id: str, vertex_id: str, time_start: int, time_end: Optional[int], quantity: int, attribution_json: str, fired_at: Optional[int]) -> int:
        # Inserts a new state into the tin_states table
        self.conn.execute("""
            INSERT INTO tin_states (state_id, run_id, vertex_id, time_start, time_end, quantity, attribution_json, fired_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (state_id, run_id, vertex_id, time_start, time_end, quantity, attribution_json, fired_at))
        return state_id

    def insert_window_output(self, output_id: int, run_id: str, vertex_id: str, window_start: int, window_end: int, output_count: int, attribution_json: str) -> int:
        # Inserts a new window output into the window_outputs table
        self.conn.execute("""
            INSERT INTO window_outputs (output_id, run_id, vertex_id, window_start, window_end, output_count, attribution_json)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """, (output_id, run_id, vertex_id, window_start, window_end, output_count, attribution_json))
        return output_id

    def close(self) -> None:
        # Closes the database connection
        if self.conn:
            self.conn.close()
            self.conn = None
            self._initialized = False
            # Reset the singleton instance to allow for a new connection in future instances
            TINStore._instance = None 
            logger.info("DuckDB connection closed.")
