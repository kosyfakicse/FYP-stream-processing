# Scripts

This directory contains utility scripts for data management, pipeline operations, and experiments.

## Structure

```
scripts/
├── data/           # Data utilities
├── pipeline/       # Pipeline utilities
├── experiments/    # Experiment utilities
└── utils/          # General utilities
```

## Data Scripts (`data/`)

- `generate_tweet_data.py` — Generate sample tweet/hashtag data (Week 3)
- `download_nyc_taxi.py` — Download NYC Taxi dataset (Week 11)
- `load_to_kafka.py` — Load data into Kafka topics (Week 3)

## Pipeline Scripts (`pipeline/`)

- `run_pipeline.py` — Run the complete pipeline (Week 3-4)
- `run_flink_job.py` — Submit Flink job (Week 3-4)
- `reset_pipeline.py` — Reset pipeline state (Week 3-4)

## Experiment Scripts (`experiments/`)

- `run_experiment.py` — Run experiment configurations (Week 9)
- `compare_provenance.py` — Compare with/without provenance (Week 11)
- `collect_metrics.py` — Collect performance metrics (Week 11)

## Utility Scripts (`utils/`)

- `verify_schema.py` — Verify schema correctness (Week 5)
- `export_results.py` — Export results for analysis (Week 12)

## Usage

Most scripts can be run directly:

For The Tweet csv Generator
1. Dry-run 
2. Run
```bash
python scripts/data/generate_tweet_data.py --mode

python scripts/data/generate_tweet_data.py --mode run
```

Or via Make commands:
```bash
make setup-data
make verify-schema
```
