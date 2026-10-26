# Data Directory

This directory contains datasets and database files for the FYP Attribution system.

## Structure

```
data/
├── raw/              # Original datasets
│   └── tweet_hashtag.csv    # Sample tweet/hashtag data (30 rows)
├── processed/        # Cleaned/transformed datasets
└── attribution.duckdb       # DuckDB database (generated at runtime)
```

## Datasets

### Tier 1: Tweet/Hashtag (v1)
- **File**: `raw/tweet_hashtag.csv`
- **Size**: ~50 tweets
- **Purpose**: Demo + correctness verification
- **Timeline**: Weeks 3-8

### Tier 2: NYC Taxi (v2)
- **File**: `raw/nyc_taxi.parquet` (to download)
- **Size**: ~10M rows
- **Purpose**: Real-world validation
- **Timeline**: Weeks 11-12

### Tier 3: Nexmark Benchmark (v3)
- **Size**: Configurable
- **Purpose**: Performance evaluation
- **Timeline**: Weeks 11-12

## Database

The `attribution.duckdb` file is generated at runtime and contains:
- `runs` table — Experiment metadata
- `tin_vertices` table — Dataflow operators
- `tin_edges` table — Data flow connections
- `tin_interactions` table — Data transfers
- `tin_states` table — Temporal state snapshots
- `window_outputs` table — Aggregation results

## Notes

- Raw data files are gitignored (except tweet_hashtag.csv)
- DuckDB database is regenerated on each pipeline run
- Use `scripts/data/generate_tweet_data.py` to create test data
