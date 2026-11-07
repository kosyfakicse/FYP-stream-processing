import duckdb
import os
import threading
from loguru import logger
from src.fyp_attribution.core.models import (
    RunModel, VertexModel, EdgeModel, 
    InteractionModel, StateModel, WindowOutputModel
)

class TINStore:
    """
    Data Access Layer (DAL) for the TIN persistence using DuckDB.
    Implements Thread-safe Singleton pattern.
    """

    # Singleton pattern implementation to ensure only one instance of TINStore exists.
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(TINStore, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
        
    def __init__(self, db_path: str = "data/attribution.duckdb") -> None:
        if getattr(self, "_initialized", False):
            return

        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.db_path = db_path
        self.conn = duckdb.connect(self.db_path)
        
        # Concurrency Control for Flink multi-threading
        self._write_lock = threading.Lock()
        
        self._initialize_db()
        self._initialized = True
        logger.info("DuckDB Connection Initialized (Singleton).")

    def _initialize_db(self) -> None:
        with self.conn.cursor() as cursor:
            # 1. runs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id VARCHAR PRIMARY KEY,
                    started_at TIMESTAMP NOT NULL,
                    finished_at TIMESTAMP,
                    dataset VARCHAR NOT NULL,
                    config_json VARCHAR,
                    provenance_enabled BOOLEAN NOT NULL
                );
            """)

            # 2. tin_vertices
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tin_vertices (
                    vertex_id VARCHAR PRIMARY KEY,
                    run_id VARCHAR NOT NULL,
                    vertex_type VARCHAR NOT NULL,
                    description VARCHAR
                );
            """)

            # 3. tin_edges
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tin_edges (
                    source_vertex VARCHAR NOT NULL,
                    target_vertex VARCHAR NOT NULL,
                    run_id VARCHAR NOT NULL,
                    PRIMARY KEY (source_vertex, target_vertex, run_id)
                );
            """)

            # 4. tin_interactions
            cursor.execute("""
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

            # 5. tin_states
            cursor.execute("""
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

            # 6. window_outputs
            cursor.execute("""
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

    # write methods for each table, returning the inserted model or an error message
    def insert_run(self, run: RunModel) -> RunModel | str:
        try:
            with self._write_lock:
                self.conn.execute("""
                    INSERT INTO runs (run_id, started_at, dataset, config_json, provenance_enabled)
                    VALUES (?, ?, ?, ?, ?);
                """, (run.run_id, run.started_at, run.dataset, run.config_json, run.provenance_enabled))
            return run
        except Exception as e:
            return f"Error inserting run {run.run_id}: {str(e)}"

    def insert_vertex(self, vertex: VertexModel) -> VertexModel | str:
        try:
            with self._write_lock:
                self.conn.execute("""
                    INSERT INTO tin_vertices (vertex_id, run_id, vertex_type, description)
                    VALUES (?, ?, ?, ?);
                """, (vertex.vertex_id, vertex.run_id, vertex.vertex_type, vertex.description))
            return vertex
        except Exception as e:
            return f"Error inserting vertex {vertex.vertex_id}: {str(e)}"

    def insert_edge(self, edge: EdgeModel) -> EdgeModel | str:
        try:
            with self._write_lock:
                self.conn.execute("""
                    INSERT INTO tin_edges (source_vertex, target_vertex, run_id)
                    VALUES (?, ?, ?);
                """, (edge.source_vertex, edge.target_vertex, edge.run_id))
            return edge
        except Exception as e:
            return f"Error inserting edge {edge.source_vertex}->{edge.target_vertex}: {str(e)}"

    def insert_interaction(self, interaction: InteractionModel, attr_str: str) -> InteractionModel | str:
        try:
            with self._write_lock:
                self.conn.execute("""
                    INSERT INTO tin_interactions (interaction_id, run_id, source_vertex, target_vertex, timestamp, quantity, attribution_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                """, (interaction.interaction_id, interaction.run_id, interaction.source_vertex, interaction.target_vertex, interaction.timestamp, interaction.quantity, attr_str))
            return interaction
        except Exception as e:
            return f"Error inserting interaction {interaction.interaction_id}: {str(e)}"

    def insert_state(self, state: StateModel, attr_str: str) -> StateModel | str:
        try:
            with self._write_lock:
                self.conn.execute("""
                    INSERT INTO tin_states (state_id, run_id, vertex_id, time_start, time_end, quantity, attribution_json, fired_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """, (state.state_id, state.run_id, state.vertex_id, state.time_start, state.time_end, state.quantity, attr_str, state.fired_at))
            return state
        except Exception as e:
            return f"Error inserting state {state.state_id}: {str(e)}"

    def insert_window_output(self, output_id: int, output: WindowOutputModel, attr_str: str) -> WindowOutputModel | str:
        try:
            with self._write_lock:
                self.conn.execute("""
                    INSERT INTO window_outputs (output_id, run_id, vertex_id, window_start, window_end, output_count, attribution_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                """, (output_id, output.run_id, output.vertex_id, output.window_start, output.window_end, output.output_count, attr_str))
            return output
        except Exception as e:
            return f"Error inserting window output {output_id}: {str(e)}"

    # Close the DuckDB connection and reset the singleton instance
    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None
            self._initialized = False
            TINStore._instance = None 
            logger.info("DuckDB connection closed.")