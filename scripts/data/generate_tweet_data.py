"""Generate tweet/hashtag test data for the FYP attribution pipeline.

Usage:
    # Dry run (default): uses record_count and user_count from config
    python scripts/data/generate_tweet_data.py

    # Generate real CSV file
    python scripts/data/generate_tweet_data.py --mode run
"""

import argparse
import csv
import json
import random
import sys
from datetime import datetime
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
CONFIG_PATH = PROJECT_ROOT / "configs" / "tweets_config.json"
OUTPUT_PATH = PROJECT_ROOT / "data" / "raw" / "tweet_hashtag.csv"

# Default time range (fallback if not in config)
DEFAULT_START_TIME = "2026-11-05 10:00:00"
DEFAULT_END_TIME = "2026-11-05 10:01:00"


def load_config() -> dict:
    """Load tweet generation configuration from JSON file."""
    if not CONFIG_PATH.exists():
        print(f"Error: Config file not found at {CONFIG_PATH}")
        sys.exit(1)

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

    required_keys = ["hashtags", "templates", "users_prefix", "record_count", "user_count"]
    for key in required_keys:
        if key not in config:
            print(f"Error: Missing required config key: {key}")
            sys.exit(1)

    # Set default time range if not provided
    if "time_range" not in config:
        config["time_range"] = {
            "start_time": DEFAULT_START_TIME,
            "end_time": DEFAULT_END_TIME
        }

    return config


def generate_tweet_text(templates: list[str], hashtags: list[str]) -> str:
    """Generate a random tweet text from templates and hashtags."""
    template = random.choice(templates)
    hashtag1 = random.choice(hashtags)
    hashtag2 = random.choice(hashtags)
    text = template.replace("{hashtag}", hashtag1).replace("{hashtag2}", hashtag2)
    return text


def parse_time_to_ms(time_str: str) -> int:
    """Convert datetime string to milliseconds timestamp.
    
    Args:
        time_str: Datetime string in format 'YYYY-MM-DD HH:MM:SS'
    
    Returns:
        Timestamp in milliseconds
    """
    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    # Convert to milliseconds (Unix timestamp * 1000)
    return int(dt.timestamp() * 1000)


def generate_records(
    config: dict,
    record_count: int,
    user_count: int,
) -> list[dict]:
    """Generate tweet records.

    Args:
        config: Tweet generation configuration.
        record_count: Number of records to generate.
        user_count: Number of unique users.

    Returns:
        List of tweet record dictionaries.
    """
    hashtags = config["hashtags"]
    templates = config["templates"]
    prefix = config["users_prefix"]
    time_range = config["time_range"]

    users = [f"{prefix}{i}" for i in range(1, user_count + 1)]

    # Parse time range to milliseconds
    start_ms = parse_time_to_ms(time_range["start_time"])
    end_ms = parse_time_to_ms(time_range["end_time"])
    
    if start_ms >= end_ms:
        print(f"Error: start_time must be before end_time")
        sys.exit(1)
    
    time_span_ms = end_ms - start_ms

    records = []
    for i in range(record_count):
        user_id = random.choice(users)
        text = generate_tweet_text(templates, hashtags)
        
        # Distribute timestamps evenly across the time range
        # Each record gets a timestamp at position i/record_count of the range
        timestamp = start_ms + int((i / record_count) * time_span_ms)

        records.append({
            "user_id": user_id,
            "text": text,
            "timestamp": timestamp,
        })

    return records


def print_dry_run(records: list[dict], config: dict) -> None:
    """Print generated records to console without writing file."""
    rc = config["record_count"]
    uc = config["user_count"]
    time_range = config["time_range"]
    
    print("=== Generate Tweet Data (DRY RUN) ===")
    print(f"Config: record count={rc}, user count={uc}")
    print(f"Time range: {time_range['start_time']} to {time_range['end_time']}")
    print(f"Hashtags: {config['hashtags']}")
    print()

    # Header
    print(f"{'#':>3}  {'user_id':<10} {'text':<50} {'timestamp'}")
    print(f"{'---':>3}  {'----------':<10} {'--------------------------------------------------':<50} {'-------------'}")

    for i, record in enumerate(records, start=1):
        print(f"{i:>3}  {record['user_id']:<10} {record['text']:<50} {record['timestamp']}")

    print()
    print(f"[DRY RUN] No file written. Use --mode run to generate CSV.")


def write_csv(records: list[dict], output_path: Path) -> None:
    """Write records to CSV file.

    Args:
        records: List of tweet record dictionaries.
        output_path: Path to output CSV file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["user_id", "text", "timestamp"])
        writer.writeheader()
        writer.writerows(records)

    print(f"[OK] Generated {len(records)} records -> {output_path}")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate tweet/hashtag test data for FYP attribution pipeline.",
    )
    parser.add_argument(
        "--mode",
        "-m",
        type=str,
        choices=["test", "run"],
        default="test",
        help="Mode: 'test' for dry run (default), 'run' to write CSV file.",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_args()
    config = load_config()

    records = generate_records(
        config,
        record_count=config["record_count"],
        user_count=config["user_count"],
    )

    if args.mode == "test":
        print_dry_run(records, config)
    elif args.mode == "run":
        write_csv(records, OUTPUT_PATH)


if __name__ == "__main__":
    main()
