import json
from typing import Any, Optional
import pandas as pd

class QueryEngine:
    def __init__(self, window_df: pd.DataFrame):
        self.df = window_df.copy() if not window_df.empty else pd.DataFrame()

    def list_categories(self, run_id: Optional[str] = None) -> list[str]:
        if self.df.empty or "category" not in self.df.columns:
            return []
        return sorted(self.df["category"].dropna().unique().tolist())

    def list_output_ids(self, category: str, run_id: Optional[str] = None) -> list[str]:
        if self.df.empty:
            return []
        sub = self.df[self.df["category"] == category]
        if "output_id" not in sub.columns:
            # API data may not have output_id; use index as fallback
            return [str(i) for i in sub.index]
        return sub["output_id"].astype(str).tolist()

    def list_contributors(self, run_id: Optional[str] = None) -> list[str]:
        users = set()

        try:
            import streamlit as st
            if "prepared_df" in st.session_state and not st.session_state.prepared_df.empty:
                users.update(st.session_state.prepared_df["user_id"].dropna().unique().tolist())
        except Exception:
            pass

        if not self.df.empty and "user_id" in self.df.columns:
            users.update(self.df["user_id"].dropna().unique().tolist())

        if not self.df.empty and "attribution_json" in self.df.columns:
            for raw in self.df["attribution_json"].dropna():
                try:
                    data = raw if isinstance(raw, dict) else json.loads(raw)
                    users.update(data.keys())
                except Exception:
                    pass

        return sorted(users)

    def get_backward_query(self, output_id, run_id=None):
        if self.df.empty:
            return "No data"
        # match by output_id if present, otherwise by index
        if "output_id" in self.df.columns:
            row = self.df[self.df["output_id"].astype(str) == str(output_id)]
        else:
            try:
                row = self.df.iloc[[int(output_id)]]
            except Exception:
                return f"Output ID {output_id} not found"
        if row.empty:
            return f"Output ID {output_id} not found"
        rec = row.iloc[0]
        attr = rec.get("attribution_json", {})
        if isinstance(attr, str):
            attr = json.loads(attr)
        total = int(rec.get("output_count", 0))
        weights = {k: (v / total if total else 0.0) for k, v in attr.items()}
        return {
            "output_id": output_id,
            "category": rec.get("category"),
            "output_count": total,
            "attribution": attr,
            "weights": weights,
            "window_start": rec.get("window_start"),
            "window_end": rec.get("window_end"),
            "run_id": run_id,
        }

    def get_forward_query(self, source_id: str, run_id=None):
        if self.df.empty:
            return f"No outputs for {source_id}"
        results = []
        for _, rec in self.df.iterrows():
            attr = rec.get("attribution_json", {})
            if isinstance(attr, str):
                attr = json.loads(attr)
            if source_id in attr:
                total = int(rec.get("output_count", 0))
                qty = attr[source_id]
                results.append({
                    "output_id": rec.get("output_id", ""),
                    "category": rec.get("category"),
                    "output_count": total,
                    "user_quantity": qty,
                    "user_weight": round(qty / total, 4) if total else 0.0,
                    "attributed_count": round(qty, 2),
                    "window_start": rec.get("window_start"),
                    "window_end": rec.get("window_end"),
                    "run_id": run_id,
                    "attribution": attr,
                })
        return results or f"No outputs found for source '{source_id}'."

    def get_temporal_lineage(self, category: str, run_id=None):
        if self.df.empty:
            return f"No records for {category}"
        sub = self.df[self.df["category"] == category].sort_values("window_start")
        return sub if not sub.empty else f"No records for category '{category}'."

    def get_flow_lineage(self, run_id=None, min_quantity: float = 0.0):
        if self.df.empty:
            return "No flow data"
        records = []
        for _, rec in self.df.iterrows():
            attr = rec.get("attribution_json", {})
            if isinstance(attr, str):
                attr = json.loads(attr)
            for user, qty in attr.items():
                records.append({
                    "source_vertex": user,
                    "target_vertex": rec.get("category"),
                    "total_quantity": qty,
                })
        if not records:
            return "No attribution data"
        df = pd.DataFrame(records)
        df = df.groupby(["source_vertex", "target_vertex"], as_index=False)["total_quantity"].sum()
        return df[df["total_quantity"] >= min_quantity].sort_values("total_quantity", ascending=False)

    def get_versioning_query(self, output_id_1, output_id_2, run_id=None):
        r1 = self.get_backward_query(output_id_1, run_id)
        r2 = self.get_backward_query(output_id_2, run_id)
        if isinstance(r1, str):
            return r1
        if isinstance(r2, str):
            return r2
        all_users = set(r1["attribution"]) | set(r2["attribution"])
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
        return {
            "window_1": {"output_id": r1["output_id"], "category": r1["category"],
                         "output_count": r1["output_count"], "window_start": r1["window_start"]},
            "window_2": {"output_id": r2["output_id"], "category": r2["category"],
                         "output_count": r2["output_count"], "window_start": r2["window_start"]},
            "delta_count": r2["output_count"] - r1["output_count"],
            "comparison": pd.DataFrame(rows),
        }
