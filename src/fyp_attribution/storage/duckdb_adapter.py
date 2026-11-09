import duckdb
import threading
import json
from loguru import logger
from typing import Any, Protocol

class IDatabase(Protocol):
    def write(self, record, collection_name: str) -> None: 
        ...

    def bulk_write(self, records: list, collection_name: str) -> None: 
        ...

    def execute(self, query: str, params: tuple = None) -> Any: 
        ...

    def executemany(self, query: str, params_list: list) -> Any: 
        ...

    def close(self) -> None: 
        ...

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
        self._service = None
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

    # Write a single record to the database (not implemented)
    def write(self, record, collection_name: str) -> None:
        pass

    def bulk_write(self, records: list, collection_name: str) -> None:
        """
        Bulk write records to the specified collection in DuckDB.
        """
        # Ensure the ProvenanceService is initialized
        if self._service is None:
            from src.fyp_attribution.storage.provenance_service import ProvenanceService
            self._service = ProvenanceService(db=self)

        from src.fyp_attribution.core.models import WindowOutputModel

        # Convert records to WindowOutputModel instances
        models = []

        # Each record is expected to be a tuple in the format
        for record in records:
            user_id, hashtag_json_str, window_start, window_end = record
            hashtags_dict = json.loads(hashtag_json_str)
            total = sum(hashtags_dict.values())

            models.append(WindowOutputModel(
                run_id="flink-run-001",
                vertex_id="W1",
                category=", ".join(hashtags_dict.keys()),
                window_start=window_start,
                window_end=window_end,
                output_count=total,
                attribution_json={user_id: total}
            ))

        # Bulk save the models using ProvenanceService
        self._service.bulk_save_window_outputs(models)
        print(f"Successfully saved {len(records)} records to DuckDB.")

    def close(self) -> None:
            if self.conn:
                self.conn.close()
                self.conn = None
                self._initialized = False
                DuckDB._instance = None