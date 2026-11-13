import os
import duckdb
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Read path from .env, with a sensible default
MOCK_DB_PATH = Path(os.getenv("MOCK_DB_PATH", "data/mock_attribution.duckdb"))


def get_mock_conn():
    """Always open a consistent connection."""
    MOCK_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(MOCK_DB_PATH))


def init_mock_db():
    """Create tables if they don't exist."""
    conn = get_mock_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mock_runs (
            run_id VARCHAR PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mock_window_outputs (
            run_id VARCHAR,
            output_id VARCHAR,
            category VARCHAR,
            output_count INTEGER,
            attribution_json VARCHAR,
            window_start BIGINT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mock_flow_lineage (
            run_id VARCHAR,
            source_vertex VARCHAR,
            target_vertex VARCHAR,
            total_quantity INTEGER
        )
    """)
    conn.close()


def save_mock_run(run_id: str, window_df: pd.DataFrame, flow_df: pd.DataFrame):
    init_mock_db()
    conn = get_mock_conn()
    try:
        conn.execute("INSERT OR REPLACE INTO mock_runs (run_id) VALUES (?)", [run_id])
        if not window_df.empty:
            window_df = window_df.copy()
            window_df["run_id"] = run_id
            conn.execute("DELETE FROM mock_window_outputs WHERE run_id = ?", [run_id])
            conn.register("window_tmp", window_df)
            conn.execute("""
                INSERT INTO mock_window_outputs
                SELECT run_id, output_id, category, output_count, attribution_json, window_start
                FROM window_tmp
            """)
        if not flow_df.empty:
            flow_df = flow_df.copy()
            flow_df["run_id"] = run_id
            conn.execute("DELETE FROM mock_flow_lineage WHERE run_id = ?", [run_id])
            conn.register("flow_tmp", flow_df)
            conn.execute("""
                INSERT INTO mock_flow_lineage
                SELECT run_id, source_vertex, target_vertex, total_quantity
                FROM flow_tmp
            """)
    finally:
        conn.close()


def load_mock_run(run_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not MOCK_DB_PATH.exists():
        return pd.DataFrame(), pd.DataFrame()
    conn = get_mock_conn()
    try:
        window_df = conn.execute(
            "SELECT * FROM mock_window_outputs WHERE run_id = ?", [run_id]
        ).fetchdf()
        flow_df = conn.execute(
            "SELECT * FROM mock_flow_lineage WHERE run_id = ?", [run_id]
        ).fetchdf()
        return window_df, flow_df
    except Exception:
        return pd.DataFrame(), pd.DataFrame()
    finally:
        conn.close()


def list_mock_run_ids() -> list[str]:
    if not MOCK_DB_PATH.exists():
        return []
    conn = get_mock_conn()
    try:
        df = conn.execute(
            "SELECT run_id FROM mock_runs ORDER BY created_at DESC"
        ).fetchdf()
        return df["run_id"].tolist()
    except Exception:
        return []
    finally:
        conn.close()


def delete_mock_run(run_id: str):
    if not MOCK_DB_PATH.exists():
        return
    conn = get_mock_conn()
    try:
        conn.execute("DELETE FROM mock_window_outputs WHERE run_id = ?", [run_id])
        conn.execute("DELETE FROM mock_flow_lineage WHERE run_id = ?", [run_id])
        conn.execute("DELETE FROM mock_runs WHERE run_id = ?", [run_id])
    finally:
        conn.close()