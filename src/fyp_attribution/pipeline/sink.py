from pyflink.datastream.functions import SinkFunction
import duckdb
import json

class DuckDBSink(SinkFunction):
    def __init__(self, db_path: str, table_name: str):
        ...

    # DuckDB
    def create_table():
        ...

    # override
    def open():
        ...

    def invoke(self):
        ...

    def close(self):
        ...

class ListSink(SinkFunction):
    """Collect results into a list"""

    def __init__(self):
        self.results = []

    def invoke(self, value, context):
        self.results.append(value)

    def get_results(self):
        return self.results

