"""Generate a tweet/hashtag csv for the attribution pipeline.

The original generator lived at this path until `fcc44ef refactor: python to java` deleted it with all
of `scripts/`, together with the `configs/tweets_config.json` it read. This is that script with the
config folded into flags, so one stdlib-only file makes the dataset and no Python package is needed.

Edit RECORDS, START, END and WINDOW_SIZE_SECONDS below this docstring to set how many rows the file holds,
the event-time range they fall in, and the tumbler size, then run one of:

    python scripts/data/generate_tweet_data.py                  # write data/raw/tweet_hashtag.csv
    python scripts/data/generate_tweet_data.py --dry-run        # look at it first
    python scripts/data/generate_tweet_data.py --records 20000 --end "2026-11-05 11:00:00"
    python scripts/data/generate_tweet_data.py --window-size-seconds 30

START and END are local wall clock. A window is [start, start + size), so END is exclusive. The
per-window output the script prints tells you what the run should store; you can check it against
`GET /runs/{runId}` after the API runs the file.

Three rules come from the Java reader rather than from csv convention, and the output is only usable if
all three hold:

- `CsvDatasetLoader.requireHeader` wants the header exactly `user_id,text,timestamp`.
- `CsvDatasetLoader.parseLine` splits on the first two commas and does no quote handling, so a comma
  anywhere in the text lands in the timestamp column and the row fails to parse. No template here
  contains one, and `--hashtags` is checked for it.
- `DatasetScan.rowCount` refuses an upload whose timestamps are not non-decreasing, because the job runs
  `forMonotonousTimestamps()` and would otherwise drop every late event without saying so. Timestamps
  are drawn at random and then sorted, so the file is ordered by construction.

Event time is epoch milliseconds, and the start is floored onto a `WINDOW_SIZE_SECONDS` boundary because
Flink aligns tumbling windows to the epoch. `HashtagExtractor` upper-cases each tag and drops the `#`, so
`#AI` is stored as the category `AI`, and two mentions in one tweet are two contributions.
"""

from __future__ import annotations

import argparse
import random
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "raw" / "tweet_hashtag.csv"

HEADER = "user_id,text,timestamp"

# Mirrors HashtagExtractor.HASHTAG in Java, which compiles "#(\w+)" with UNICODE_CHARACTER_CLASS.
HASHTAG_PATTERN = r"#(\w+)"

# From the deleted configs/tweets_config.json. No entry may contain a comma; see the parseLine note above.
HASHTAGS = ["#AI", "#UST", "#DATA"]
TEMPLATES = [
    "Loving {hashtag}!",
    "{hashtag} is awesome",
    "Check out {hashtag}",
    "Learning {hashtag} today",
    "{hashtag} {hashtag2}",
    "Just tried {hashtag} and its amazing",
    "Who else uses {hashtag}?",
    "{hashtag} for the win",
]
USERS_PREFIX = "user_"

# How many tweets the file holds. One row is one tweet, not one event: the extractor fires one event per
# hashtag mention, so a `#AI #DATA` row feeds two categories and a `#AI #AI` row feeds two counts of AI.
# Every template mentions at least one tag, so the per-window outputCounts add up to at least RECORDS.
RECORDS = 100000000

# The event-time range the rows are drawn from: local wall clock, start inclusive, end exclusive.
# Edit these, or pass --start / --end. The end need not sit on a window boundary; the last window then
# fires partly full, exactly as the per-window summary prints it.
START = "2026-11-05 10:00:00"
END = "2026-11-05 10:01:00"

# Tumblers are [start, start + size) aligned to the epoch, and the job takes its size from
# pipeline.window_size_seconds in backend/src/main/resources/config.yaml. Change one, change the other,
# or the window boundaries the script reports are not the windows the run actually fires.
WINDOW_SIZE_SECONDS = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate the tweet/hashtag csv the pipeline reads.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--records", type=int, default=RECORDS, help="rows to write")
    parser.add_argument("--users", type=int, default=3, help="distinct user ids drawing the rows")
    parser.add_argument("--start", default=START, help="local 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--end", default=END, help="same format, must be after --start")
    parser.add_argument(
        "--window-size-seconds",
        type=int,
        default=WINDOW_SIZE_SECONDS,
        help="tumbler size; keep equal to pipeline.window_size_seconds",
    )
    parser.add_argument("--hashtags", default=",".join(HASHTAGS), help="comma separated pool")
    parser.add_argument("--seed", type=int, default=42, help="same seed means the same file")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT, help="csv to write")
    parser.add_argument("--dry-run", action="store_true", help="print instead of writing")
    parser.add_argument("--force", action="store_true", help="overwrite an existing output file")

    args = parser.parse_args()
    for name in ("records", "users", "window_size_seconds"):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be at least 1")
    moments = {}
    for name in ("start", "end"):
        try:
            moments[name] = datetime.strptime(getattr(args, name), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            parser.error(f"--{name} wants 'YYYY-MM-DD HH:MM:SS', got {getattr(args, name)!r}")
    if moments["end"] <= moments["start"]:
        parser.error(f"--end {args.end!r} must be after --start {args.start!r}")
    if args.out.exists() and not args.force and not args.dry_run:
        parser.error(f"{args.out} already exists and is not in git; pass --force to replace it")
    return args


def to_millis(local_time: str) -> int:
    """Local wall clock to epoch milliseconds."""
    return int(datetime.strptime(local_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)


def onto_window(millis: int, window_size_seconds: int) -> int:
    """Floor onto a window boundary, because Flink aligns tumbling windows to the epoch."""
    boundary = window_size_seconds * 1000
    return millis - millis % boundary


def tweet_text(rng: random.Random, hashtags: list[str]) -> str:
    return rng.choice(TEMPLATES).format(hashtag=rng.choice(hashtags), hashtag2=rng.choice(hashtags))


def generate(args: argparse.Namespace) -> list[tuple[str, str, int]]:
    rng = random.Random(args.seed)
    hashtags = [tag.strip() for tag in args.hashtags.split(",") if tag.strip()]
    if not hashtags:
        sys.exit("--hashtags ended up empty")
    for tag in hashtags:
        # A tag without the `#` still writes a fine row, and HashtagExtractor then finds nothing in it:
        # the row costs the run an event and nobody says so.
        if not re.fullmatch(HASHTAG_PATTERN, tag):
            sys.exit(f"--hashtags must each look like #AI, word characters only: got {tag!r}")
    for value in TEMPLATES + [USERS_PREFIX]:
        if "," in value or '"' in value:
            sys.exit(f"Refusing to generate: {value!r} holds a comma or a quote the loader cannot parse")

    # The start is floored so the first window is a full one; windows are [start, start + size), which
    # is why --end is exclusive and why a span that is not a whole number of windows ends partly full.
    start_ms = onto_window(to_millis(args.start), args.window_size_seconds)
    span_ms = to_millis(args.end) - start_ms
    users = [f"{USERS_PREFIX}{i}" for i in range(1, args.users + 1)]

    rows = []
    for _ in range(args.records):
        # Random instant first, sorted afterwards: an evenly strided file looks tidy and is no more
        # realistic, while a shuffled one uploaded as-is would be rejected by DatasetScan.
        timestamp = start_ms + rng.randrange(span_ms)
        rows.append((rng.choice(users), tweet_text(rng, hashtags), timestamp))
    rows.sort(key=lambda row: row[2])
    return rows


def categories(text: str) -> list[str]:
    """The same tags `HashtagExtractor` emits: word chars after `#`, upper-cased, repeats kept."""
    return [tag.upper() for tag in re.findall(HASHTAG_PATTERN, text)]


def as_csv(rows: list[tuple[str, str, int]]) -> str:
    return "\n".join([HEADER, *(f"{user},{text},{millis}" for user, text, millis in rows)]) + "\n"


def main() -> None:
    args = parse_args()
    rows = generate(args)
    body = as_csv(rows)

    size_ms = args.window_size_seconds * 1000
    start_ms, end_ms = rows[0][2], rows[-1][2]
    by_window: dict[int, Counter] = {}
    for _, text, millis in rows:
        counts = by_window.setdefault(millis // size_ms, Counter())
        counts.update(categories(text))

    print(f"{len(rows)} rows, {args.users} users, {len(by_window)} window(s) touched")
    print(
        f"{datetime.fromtimestamp(start_ms / 1000)} to {datetime.fromtimestamp(end_ms / 1000)}"
        f" ({(end_ms - start_ms) / 1000:.1f}s span, aligned to {args.window_size_seconds}s)"
    )
    print()
    print("what the run should store, per firing window (categories, so #AI is AI):")
    for index, (window, counts) in enumerate(sorted(by_window.items())):
        if index == 8:
            print(f"  ... and {len(by_window) - 8} more windows")
            break
        total = sum(counts.values())
        spread = ", ".join(f"{tag} {count}" for tag, count in sorted(counts.items()))
        print(f"  {window * size_ms} -> {(window + 1) * size_ms}: outputCount {total}, {spread}")

    if args.dry_run:
        print()
        print(body if len(rows) <= 15 else "\n".join(body.splitlines()[:15]) + "\n... truncated")
        print(f"[dry run] nothing written; drop --dry-run to write {args.out}")
        return

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(body, encoding="utf-8", newline="\n")
    print(f"[ok] {args.out} ({len(body)} bytes)")


if __name__ == "__main__":
    main()
