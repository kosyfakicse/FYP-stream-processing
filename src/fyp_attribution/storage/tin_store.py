from loguru import logger
from src.fyp_attribution.core.models import (
    RunModel, VertexModel, EdgeModel, 
    InteractionModel, StateModel, WindowOutputModel
)
from src.fyp_attribution.pipeline.db_writer import IDatabase

class TINStore:
    """
    Data Access Layer (DAL) for the TIN persistence using DuckDB.
    """


    def __init__(self, db: IDatabase) -> None:
        self.db = db
        self._initialize_db()
        logger.info("TINStore Initialized.")

    def _initialize_db(self) -> None:
        # 1. runs
        self.db.execute("""
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
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS tin_vertices (
                vertex_id VARCHAR PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                vertex_type VARCHAR NOT NULL,
                description VARCHAR
            );
        """)

        # 3. tin_edges
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS tin_edges (
                source_vertex VARCHAR NOT NULL,
                target_vertex VARCHAR NOT NULL,
                run_id VARCHAR NOT NULL,
                PRIMARY KEY (source_vertex, target_vertex, run_id)
            );
        """)

        # 4. tin_interactions
        self.db.execute("""
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
        self.db.execute("""
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
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS window_outputs (
                output_id BIGINT PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                vertex_id VARCHAR NOT NULL,
                category VARCHAR NOT NULL,
                window_start BIGINT NOT NULL,
                window_end BIGINT NOT NULL,
                output_count INTEGER NOT NULL,
                attribution_json VARCHAR NOT NULL
            );
        """)
        logger.info("All TIN tables have been successfully created.")

    # write methods for each table, returning the inserted model or an error message
    def insert_run(self, run: RunModel) -> RunModel:
        self.db.execute("""
            INSERT INTO runs (run_id, started_at, dataset, config_json, provenance_enabled)
            VALUES (?, ?, ?, ?, ?);
        """, (run.run_id, run.started_at, run.dataset, run.config_json, run.provenance_enabled))
        return run

    def insert_vertex(self, vertex: VertexModel) -> VertexModel:
        self.db.execute("""
            INSERT INTO tin_vertices (vertex_id, run_id, vertex_type, description)
            VALUES (?, ?, ?, ?);
        """, (vertex.vertex_id, vertex.run_id, vertex.vertex_type, vertex.description))
        return vertex

    def insert_edge(self, edge: EdgeModel) -> EdgeModel:
        
        self.db.execute("""
            INSERT INTO tin_edges (source_vertex, target_vertex, run_id)
            VALUES (?, ?, ?);
        """, (edge.source_vertex, edge.target_vertex, edge.run_id))
        return edge

    def insert_interaction(self, interaction: InteractionModel, attr_str: str) -> InteractionModel:
        self.db.execute("""
            INSERT INTO tin_interactions (interaction_id, run_id, source_vertex, target_vertex, timestamp, quantity, attribution_json)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """, (interaction.interaction_id, interaction.run_id, interaction.source_vertex, interaction.target_vertex, interaction.timestamp, interaction.quantity, attr_str))
        return interaction

    def insert_state(self, state: StateModel, attr_str: str) -> StateModel:
        self.db.execute("""
            INSERT INTO tin_states (state_id, run_id, vertex_id, time_start, time_end, quantity, attribution_json, fired_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (state.state_id, state.run_id, state.vertex_id, state.time_start, state.time_end, state.quantity, attr_str, state.fired_at))
        return state

    def insert_window_output(self, output_id: int, output: WindowOutputModel, attr_str: str) -> WindowOutputModel:
        self.db.execute("""
            INSERT INTO window_outputs (output_id, run_id, vertex_id, category, window_start, window_end, output_count, attribution_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (output_id, output.run_id, output.vertex_id, output.category, output.window_start, output.window_end, output.output_count, attr_str))
        return output

    # Bulk insert method for window outputs, allowing multiple records to be inserted in a single operation.
    def bulk_insert_window_outputs(self, records: list[tuple]) -> bool:
        self.db.executemany("""
            INSERT INTO window_outputs (output_id, run_id, vertex_id, category, window_start, window_end, output_count, attribution_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, records)
        return True

    # Close the DuckDB connection and reset the singleton instance
    def close(self) -> None:
        self.db.close()
        logger.info("Database connection closed.")