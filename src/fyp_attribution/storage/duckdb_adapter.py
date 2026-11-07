import duckdb
import threading
from loguru import logger
from typing import Any
from src.fyp_attribution.pipeline.db_writer import IDatabase

class DuckDB(IDatabase):
    """Concrete database implementation for DuckDB."""

    # Singleton pattern implementation to ensure only one instance of TINStore exists.
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DuckDB, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
        
    def __init__(self, db_path: str = "data/attribution.duckdb"):
        if getattr(self, "_initialized", False):
            return

        self.conn = duckdb.connect(db_path)
        self.db_path = db_path
        self.write_lock = threading.Lock()
        self._initialized = True
        logger.info("DuckDB Connection Initialized (Singleton).")

    # Execute a query and return the result
    def execute(self, query: str, params: tuple = None) -> Any:
        """Execute a single query."""
        try:
            with self.write_lock:
                if params:
                    return self.conn.execute(query, params)
                return self.conn.execute(query)
        except Exception as e:
            raise RuntimeError(f"Database error: {str(e)}") from e

    # Execute a bulk query with multiple parameter sets
    def executemany(self, query: str, params_list: list) -> Any:
        """Execute a bulk query."""
        try:
            with self.write_lock:
                return self.conn.executemany(query, params_list)
        except Exception as e:
            raise RuntimeError(f"Database bulk error: {str(e)}") from e

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None
            self._initialized = False
            DuckDB._instance = None