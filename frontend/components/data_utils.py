import random
import time
import pandas as pd


def validate_schema(df: pd.DataFrame):
    required_cols = ["user_id", "text", "timestamp"]
    if list(df.columns) != required_cols:
        return (
            False,
            f"Invalid header format. Expected {required_cols}, but got"
            f" {list(df.columns)}.",
        )
    if df.empty:
        return False, "Dataset cannot be empty. Please provide at least one row."
    if (
        df.isnull().any().any()
        or (df["user_id"].astype(str).str.strip() == "").any()
        or (df["text"].astype(str).str.strip() == "").any()
    ):
        return False, "Dataset contains null or empty whitespace values."
    if not pd.api.types.is_numeric_dtype(df["timestamp"]):
        return False, "Timestamp must be numeric (13-digit Unix Epoch ms)."
    return True, "Schema validation passed (user_id, text, timestamp)."


def generate_mock_tweets(
    num_rows: int, num_users: int, hashtags: list, interval_ms: int
) -> pd.DataFrame:
    templates = [
        "{t1} {t2}",
        "Who else uses {t1}?",
        "Loving {t1}!",
        "Learning {t1} today",
        "Check out {t1}",
        "{t1} is awesome",
        "{t1} for the win",
        "Just tried {t1} and its amazing",
    ]
    users = [f"user_{i}" for i in range(1, num_users + 1)]
    start_ts = int(time.time() * 1000)
    rows = []
    for i in range(num_rows):
        t1 = random.choice(hashtags)
        t2 = random.choice(hashtags)
        rows.append({
            "user_id": random.choice(users),
            "text": random.choice(templates).format(t1=t1, t2=t2),
            "timestamp": start_ts + (i * interval_ms),
        })
    return pd.DataFrame(rows, columns=["user_id", "text", "timestamp"])