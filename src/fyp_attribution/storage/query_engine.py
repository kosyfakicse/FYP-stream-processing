import json
from typing import Any
from src.fyp_attribution.storage.tin_store import TINStore
from src.fyp_attribution.core.attribution_tracker import AttributionTracker

class QueryEngine:
    def __init__(self, store: TINStore):
        self.store = store

    # 1. Backward Query
    def get_backward_query(self, output_id: int) -> dict[str, Any] | str:
        try:
            # Query the database for the specified output_id
            result = self.store.db.execute("""
                SELECT category, output_count, attribution_json, window_start, window_end
                FROM window_outputs 
                WHERE output_id = ?
            """, (output_id,)).fetchone()

            # If no result is found, return an error message
            if not result:
                return f"Error: Output ID {output_id} not found."

            # Return the result as a dictionary
            return {
                "output_id": output_id,
                "category": result[0],
                "output_count": result[1],
                "attribution": json.loads(result[2]),
                "window_start": result[3],
                "window_end": result[4]
            }

        except Exception as e:
            return f"Query error: {str(e)}"

    # 2. Forward Query
    def get_forward_query(self, source_id: str) -> list[dict[str, Any]] | str:
        try:
            # Query the database for all outputs that include the specified source_id in their attribution_json
            query = """
                SELECT output_id, category, output_count, attribution_json
                FROM window_outputs
                WHERE json_extract_string(attribution_json, ?) IS NOT NULL
            """

            # Use json_extract_string to check if the source_id exists in the attribution_json
            json_path = f"$.{source_id}"
            results = self.store.db.execute(query, (json_path,)).fetchall()

            # If no results are found, return an error message
            if not results:
                return f"Error: No outputs found for source {source_id}."

            # Return the results as a list of dictionaries
            return [
                {"output_id": r[0], "category": r[1], "output_count": r[2], "attribution": json.loads(r[3])}
                for r in results
            ]
        
        except Exception as e:
            return f"Query error: {str(e)}"

    # 3. Temporal Lineage


    # 4. Flow Lineage


    # 5. Versioning Attribution
    