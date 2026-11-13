import json
from typing import Any, Optional
import pandas as pd
from src.fyp_attribution.storage.tin_store import TINStore


class QueryEngine:
    def __init__(self, store: TINStore):
        self.store = store
        self.db = store.db

    # ------------------------------------------------------------------
    # 1. Backward Query
    # ------------------------------------------------------------------
    def get_backward_query(
        self, 
        output_id: int | str, 
        run_id: Optional[str] = None
    ) -> dict[str, Any] | str:
        """
        Given an output_id (and optional run_id), return full details + attribution.
        """
        try:
            if run_id:
                result = self.db.execute("""
                    SELECT 
                        output_id, category, output_count, 
                        attribution_json, window_start, window_end, run_id
                    FROM window_outputs 
                    WHERE output_id = ? AND run_id = ?
                """, (int(output_id), run_id)).fetchone()
            else:
                result = self.db.execute("""
                    SELECT 
                        output_id, category, output_count, 
                        attribution_json, window_start, window_end, run_id
                    FROM window_outputs 
                    WHERE output_id = ?
                """, (int(output_id),)).fetchone()

            if not result:
                return f"Error: Output ID {output_id} not found."

            attribution = json.loads(result[3]) if result[3] else {}
            total = result[2] or 0

            weights = {
                k: (v / total if total > 0 else 0.0)
                for k, v in attribution.items()
            }

            return {
                "output_id": result[0],
                "category": result[1],
                "output_count": total,
                "attribution": attribution,
                "weights": weights,
                "window_start": result[4],
                "window_end": result[5],
                "run_id": result[6],
            }

        except Exception as e:
            return f"Query error: {str(e)}"

    # ------------------------------------------------------------------
    # 2. Forward Query
    # ------------------------------------------------------------------
    def get_forward_query(
        self, 
        source_id: str, 
        run_id: Optional[str] = None
    ) -> list[dict[str, Any]] | str:
        """
        Find all window outputs influenced by a source user.
        """
        try:
            json_path = f"$.{source_id}"

            if run_id:
                query = """
                    SELECT output_id, category, output_count, attribution_json, 
                           window_start, window_end, run_id
                    FROM window_outputs
                    WHERE run_id = ?
                      AND json_extract_string(attribution_json, ?) IS NOT NULL
                    ORDER BY window_start
                """
                results = self.db.execute(query, (run_id, json_path)).fetchall()
            else:
                query = """
                    SELECT output_id, category, output_count, attribution_json, 
                           window_start, window_end, run_id
                    FROM window_outputs
                    WHERE json_extract_string(attribution_json, ?) IS NOT NULL
                    ORDER BY window_start
                """
                results = self.db.execute(query, (json_path,)).fetchall()

            if not results:
                return f"Error: No outputs found for source '{source_id}'."

            output = []
            for r in results:
                attr = json.loads(r[3]) if r[3] else {}
                user_qty = attr.get(source_id, 0)
                total = r[2] or 0
                weight = user_qty / total if total > 0 else 0.0

                output.append({
                    "output_id": r[0],
                    "category": r[1],
                    "output_count": total,
                    "user_quantity": user_qty,
                    "user_weight": round(weight, 4),
                    "attributed_count": round(total * weight, 2),
                    "window_start": r[4],
                    "window_end": r[5],
                    "run_id": r[6],
                    "attribution": attr,
                })

            return output

        except Exception as e:
            return f"Query error: {str(e)}"

    # ------------------------------------------------------------------
    # 3. Temporal Lineage
    # ------------------------------------------------------------------
    def get_temporal_lineage(
        self, 
        category: str, 
        run_id: Optional[str] = None
    ) -> pd.DataFrame | str:
        """
        Return time series of a hashtag across windows.
        """
        try:
            if run_id:
                df = self.db.execute("""
                    SELECT 
                        output_id, category, output_count,
                        attribution_json, window_start, window_end, run_id
                    FROM window_outputs
                    WHERE category = ? AND run_id = ?
                    ORDER BY window_start
                """, (category, run_id)).fetchdf()
            else:
                df = self.db.execute("""
                    SELECT 
                        output_id, category, output_count,
                        attribution_json, window_start, window_end, run_id
                    FROM window_outputs
                    WHERE category = ?
                    ORDER BY window_start
                """, (category,)).fetchdf()

            if df.empty:
                return f"Error: No records found for category '{category}'."

            return df

        except Exception as e:
            return f"Query error: {str(e)}"

    # ------------------------------------------------------------------
    # 4. Flow Lineage
    # ------------------------------------------------------------------
    def get_flow_lineage(
        self, 
        run_id: Optional[str] = None,
        min_quantity: float = 0.0
    ) -> pd.DataFrame | str:
        """
        Aggregate source → hashtag flow (derived from window_outputs).
        """
        try:
            if run_id:
                rows = self.db.execute("""
                    SELECT category, output_count, attribution_json
                    FROM window_outputs
                    WHERE run_id = ?
                """, (run_id,)).fetchall()
            else:
                rows = self.db.execute("""
                    SELECT category, output_count, attribution_json
                    FROM window_outputs
                """).fetchall()

            if not rows:
                return "Error: No flow data available."

            flow_records = []
            for category, output_count, attr_json in rows:
                attr = json.loads(attr_json) if attr_json else {}
                for user_id, qty in attr.items():
                    flow_records.append({
                        "source_vertex": user_id,
                        "target_vertex": category,
                        "total_quantity": qty
                    })

            if not flow_records:
                return "Error: No attribution data found."

            df = pd.DataFrame(flow_records)
            df = (
                df.groupby(["source_vertex", "target_vertex"], as_index=False)
                  ["total_quantity"]
                  .sum()
            )
            df = df[df["total_quantity"] >= min_quantity]
            df = df.sort_values("total_quantity", ascending=False)

            return df

        except Exception as e:
            return f"Query error: {str(e)}"

    # ------------------------------------------------------------------
    # 5. Versioning Query
    # ------------------------------------------------------------------
    def get_versioning_query(
        self,
        output_id_1: int | str,
        output_id_2: int | str,
        run_id: Optional[str] = None
    ) -> dict[str, Any] | str:
        """
        Compare two windows. Optional run_id is applied to both lookups.
        """
        try:
            r1 = self.get_backward_query(output_id_1, run_id=run_id)
            r2 = self.get_backward_query(output_id_2, run_id=run_id)

            if isinstance(r1, str):
                return r1
            if isinstance(r2, str):
                return r2

            all_users = set(r1["attribution"].keys()) | set(r2["attribution"].keys())
            rows = []
            for user in sorted(all_users):
                w1 = r1["weights"].get(user, 0.0)
                w2 = r2["weights"].get(user, 0.0)
                rows.append({
                    "contributor": user,
                    "Window 1 Weight": round(w1, 4),
                    "Window 2 Weight": round(w2, 4),
                    "Weight Diff": round(w2 - w1, 4),
                    "Window 1 Count": r1["attribution"].get(user, 0),
                    "Window 2 Count": r2["attribution"].get(user, 0),
                })

            comparison_df = pd.DataFrame(rows)

            return {
                "window_1": {
                    "output_id": r1["output_id"],
                    "category": r1["category"],
                    "output_count": r1["output_count"],
                    "window_start": r1["window_start"],
                },
                "window_2": {
                    "output_id": r2["output_id"],
                    "category": r2["category"],
                    "output_count": r2["output_count"],
                    "window_start": r2["window_start"],
                },
                "delta_count": r2["output_count"] - r1["output_count"],
                "comparison": comparison_df,
            }

        except Exception as e:
            return f"Query error: {str(e)}"

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def list_categories(self, run_id: Optional[str] = None) -> list[str]:
        try:
            if run_id:
                df = self.db.execute(
                    "SELECT DISTINCT category FROM window_outputs WHERE run_id = ? ORDER BY category",
                    (run_id,)
                ).fetchdf()
            else:
                df = self.db.execute(
                    "SELECT DISTINCT category FROM window_outputs ORDER BY category"
                ).fetchdf()
            return df["category"].tolist()
        except Exception:
            return []

    def list_output_ids(
        self, 
        category: str, 
        run_id: Optional[str] = None
    ) -> list[str]:
        try:
            if run_id:
                df = self.db.execute("""
                    SELECT output_id FROM window_outputs 
                    WHERE category = ? AND run_id = ?
                    ORDER BY window_start
                """, (category, run_id)).fetchdf()
            else:
                df = self.db.execute("""
                    SELECT output_id FROM window_outputs 
                    WHERE category = ?
                    ORDER BY window_start
                """, (category,)).fetchdf()
            return df["output_id"].astype(str).tolist()
        except Exception:
            return []

    def list_contributors(self, run_id: Optional[str] = None) -> list[str]:
        try:
            if run_id:
                rows = self.db.execute(
                    "SELECT attribution_json FROM window_outputs WHERE run_id = ?",
                    (run_id,)
                ).fetchall()
            else:
                rows = self.db.execute(
                    "SELECT attribution_json FROM window_outputs"
                ).fetchall()

            users = set()
            for (attr_json,) in rows:
                if attr_json:
                    users.update(json.loads(attr_json).keys())
            return sorted(users)
        except Exception:
            return []