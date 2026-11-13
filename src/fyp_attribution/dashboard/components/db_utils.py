import pandas as pd
import streamlit as st
from src.fyp_attribution.storage.duckdb_adapter import DuckDB
from src.fyp_attribution.dashboard.components.page_setup import DB_PATH


@st.cache_resource
def get_db():
    return DuckDB(db_path=DB_PATH, read_only=True)


def load_run_data(selected_run_id: str, mock_store: dict):
    """Load window_outputs and flow_lineage from either Mock Session State or DuckDB."""
    if selected_run_id in mock_store:
        data = mock_store[selected_run_id]
        window_df = data.get("window_outputs", pd.DataFrame()).copy()
        flow_df = data.get("flow_lineage", pd.DataFrame()).copy()
        return window_df, flow_df, "Mock Session"

    # Load from DuckDB
    window_df = pd.DataFrame()
    flow_df = pd.DataFrame()
    try:
        db = get_db()
        cols_df = db.execute("DESCRIBE window_outputs").fetchdf()
        col_names = cols_df["column_name"].tolist()

        if "run_id" in col_names and selected_run_id != "All Runs (DuckDB)":
            window_df = db.execute(
                "SELECT * FROM window_outputs WHERE run_id = ?",
                [selected_run_id],
            ).fetchdf()
        else:
            window_df = db.execute("SELECT * FROM window_outputs").fetchdf()

        tables_df = db.execute("SHOW TABLES").fetchdf()
        if "flow_lineage" in tables_df["name"].tolist():
            if selected_run_id != "All Runs (DuckDB)":
                flow_df = db.execute(
                    "SELECT * FROM flow_lineage WHERE run_id = ?",
                    [selected_run_id],
                ).fetchdf()
            else:
                flow_df = db.execute("SELECT * FROM flow_lineage").fetchdf()
    except Exception as e:
        st.warning(f"Database query notice: {e}")

    return window_df, flow_df, "DuckDB Live"


def get_available_run_ids(mock_store: dict) -> list:
    """Gather available Run IDs from both Mock Session State and DuckDB."""
    run_ids = list(mock_store.keys())
    try:
        db = get_db()
        cols_df = db.execute("DESCRIBE window_outputs").fetchdf()
        if "run_id" in cols_df["column_name"].tolist():
            db_runs = (
                db.execute("SELECT DISTINCT run_id FROM window_outputs")
                .fetchdf()["run_id"]
                .dropna()
                .tolist()
            )
            for r in db_runs:
                if r not in run_ids:
                    run_ids.append(r)
    except Exception:
        pass

    if not run_ids:
        run_ids.append("All Runs (DuckDB)")
    return run_ids