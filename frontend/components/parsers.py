import json
import pandas as pd


def parse_attribution_json(attr_str) -> pd.DataFrame:
    """Parse attribution JSON string (or dict) into a sorted contributor DataFrame."""
    try:
        if isinstance(attr_str, dict):
            data = attr_str
        else:
            data = json.loads(attr_str)
        rows = [{"contributor": k, "weight": float(v)} for k, v in data.items()]
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(by="weight", ascending=False)
        return df
    except Exception:
        return pd.DataFrame(columns=["contributor", "weight"])