import requests
import pandas as pd
from components.page_setup import BACKEND_API_URL

BASE = BACKEND_API_URL.rstrip("/")

def get_runs():
    resp = requests.get(f"{BASE}/runs", timeout=10)
    resp.raise_for_status()
    return resp.json()

def get_run(run_id: str):
    resp = requests.get(f"{BASE}/runs/{run_id}", timeout=10)
    resp.raise_for_status()
    return resp.json()

def windows_to_df(run_data: dict) -> pd.DataFrame:
    """Turn the windows list from GET /runs/{id} into a DataFrame."""
    windows = run_data.get("windows", [])
    if not windows:
        return pd.DataFrame()
    df = pd.DataFrame(windows)
    # rename to match what the charts/pages expect
    df = df.rename(columns={
        "windowStart": "window_start",
        "windowEnd": "window_end",
        "outputCount": "output_count",
        "attribution": "attribution_json",
    })
    return df