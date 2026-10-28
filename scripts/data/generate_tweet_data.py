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
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
CONFIG_PATH = PROJECT_ROOT / "configs" / "tweets_config.json"
OUTPUT_PATH = PROJECT_ROOT / "data" / "raw" / "tweet_hashtag.csv"

# Base timestamp: 2025-01-01 00:00:00 UTC in milliseconds
BASE_TIMESTAMP = 1_735_689_600_000
TIMESTAMP_STEP_MS = 1_000  # 1 second between records


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

    return config


def generate_tweet_text(templates: list[str], hashtags: list[str]) -> str:
    """Generate a random tweet text from templates and hashtags."""
    template = random.choice(templates)
    hashtag1 = random.choice(hashtags)
    hashtag2 = random.choice(hashtags)
    text = template.replace("{hashtag}", hashtag1).replace("{hashtag2}", hashtag2)
    return text


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

    users = [f"{prefix}{i}" for i in range(1, user_count + 1)]

    records = []
    for i in range(record_count):
        user_id = random.choice(users)
        text = generate_tweet_text(templates, hashtags)
        timestamp = BASE_TIMESTAMP + (i * TIMESTAMP_STEP_MS)

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
    print("=== Generate Tweet Data (DRY RUN) ===")
    print(f"Config: record count={rc}, user count={uc}")
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
