# FYP Temporal Attribution System

Demonstration and Evaluation of How-Much Provenance in Streaming Dataflows

## Overview

This project implements a streaming system that demonstrates **how-much provenance** using **temporal attribution** (TIN-based), focusing on **backward attribution** (Where-From queries).

### Core Question
> "Which tweets/users caused this hashtag to trend?"

### Architecture
```
Dataset (CSV/Parquet) → Kafka (replay) → Flink (process + attribution) → DuckDB (TIN store) → Streamlit (dashboard)
```

## Current Project Structure

```
FYP-stream-processing/
|
|-- README.md                           # Project overview + setup
|-- pyproject.toml                      # Package configuration
|-- .gitignore                          # Git ignore rules
|-- .env                                # Local environment (gitignored)
|-- .env.example                        # Environment template
|-- Makefile                            # Common commands
|
|-- configs/                            # Configuration files
|   +-- README.md
|
|-- data/                               # Datasets and database
|   |-- README.md
|   |-- raw/                            # Original datasets
|   +-- processed/                      # Cleaned/transformed data
|
|-- src/                                # Source code
|   |-- README.md
|   +-- fyp_attribution/                # Main package
|       |-- __init__.py
|       |-- __version__.py
|       |-- logging_config.py           # Loguru logging configuration
|       |-- core/                       # Core business logic
|       |   |-- README.md
|       |   +-- __init__.py
|       |-- pipeline/                   # Data pipeline components
|       |   +-- README.md
|       |-- storage/                    # Data storage layer
|       |   +-- README.md
|       +-- dashboard/                  # Streamlit UI
|           |-- README.md
|           |-- __init__.py
|           |-- pages/                  # Dashboard pages
|           +-- components/             # UI components
|
|-- tests/                              # Test suite
|   |-- README.md
|   |-- __init__.py
|   |-- conftest.py                     # Shared fixtures
|   |-- unit/                           # Unit tests
|   |   |-- README.md
|   |   +-- __init__.py
|   |-- integration/                    # Integration tests
|   |   +-- README.md
|   +-- e2e/                            # End-to-end tests
|       +-- README.md
|
|-- scripts/                            # Utility scripts
|   +-- README.md
|
|-- docs/                               # Documentation
|   +-- README.md
|
+-- related-work/                       # Reference papers
    |-- temporal-attribution.pdf
    |-- flink-deb.pdf
    +-- s00778-023-00819-8.pdf
```

## Logging

This project uses [loguru](https://github.com/Delgan/loguru) for logging. Loguru provides a simpler and more powerful logging experience than the standard library logging module.

### Usage

```python
from fyp_attribution.logging_config import setup_logging, get_logger

# Configure logging
setup_logging(log_level="INFO", log_file="logs/app.log")

# Get a logger
logger = get_logger(__name__)

# Log messages
logger.info("This is an info message")
logger.debug("This is a debug message")
logger.warning("This is a warning")
logger.error("This is an error")
```

### Features

- Console logging with colors
- File logging with rotation (10 MB)
- Retention policy (10 days)
- Compression of old logs (zip)
- Structured logging with module, function, and line information

## Complete Project Structure (Final State)

```
FYP-stream-processing/
|
|-- README.md                           # Project overview + setup
|-- CODING_STYLE.md                     # Coding conventions + Python version
|-- pyproject.toml                      # Package configuration
|-- requirements.txt                    # Dependencies
|-- .python-version                     # Python 3.10.13
|-- .gitignore                          # Git ignore rules
|-- .env.example                        # Environment template
|-- .env                                # Local environment (gitignored)
|-- Makefile                            # Common commands
|
|-- PLAN.md                             # Master plan overview
|-- IMPLEMENTATION_PLAN.md              # Technical details
|-- WORK_DISTRIBUTION.md                # 2-student work split
|-- GANTT_CHART.md                      # 16-week timeline
|-- SCHEMA.md                           # Schema reference
|
|-- configs/                            # Configuration files
|   |-- README.md
|   +-- config.yaml                     # Main configuration
|
|-- data/                               # Datasets and database
|   |-- README.md
|   |-- raw/
|   |   +-- tweet_hashtag.csv           # Test data (30 sample tweets)
|   |-- processed/                      # Cleaned/transformed data
|   +-- attribution.duckdb              # DuckDB database (generated at runtime)
|
|-- src/                                # Source code
|   |-- README.md
|   +-- fyp_attribution/                # Main package
|       |-- __init__.py
|       |-- __version__.py
|       |
|       |-- core/                       # Core business logic
|       |   |-- README.md
|       |   |-- __init__.py
|       |   |-- models.py               # Pydantic models (Attribution, TIN, etc.)
|       |   |-- attribution.py          # AttributionTracker implementation
|       |   +-- tin.py                  # TIN operations
|       |
|       |-- pipeline/                   # Data pipeline components
|       |   |-- README.md
|       |   |-- __init__.py
|       |   |-- kafka_consumer.py       # Kafka message consumer
|       |   |-- flink_processor.py      # Apache Flink stream processing
|       |   +-- duckdb_writer.py        # DuckDB database writer
|       |
|       |-- storage/                    # Data storage layer
|       |   |-- README.md
|       |   |-- __init__.py
|       |   |-- tin_store.py            # TIN persistence layer
|       |   +-- query_engine.py         # Backward attribution queries (BFS)
|       |
|       |-- dashboard/                  # Streamlit UI
|       |   |-- README.md
|       |   |-- __init__.py
|       |   |-- app.py                  # Main Streamlit application
|       |   |-- pages/
|       |   |   |-- overview.py         # Overview page
|       |   |   +-- backward_attribution.py  # Query interface
|       |   +-- components/
|       |       +-- charts.py           # Reusable chart components
|       |
|       |-- config.py                   # Configuration management
|       |-- utils.py                    # Helper functions
|       +-- logging_config.py           # Loguru logging configuration
|
|-- tests/                              # Test suite
|   |-- README.md
|   |-- __init__.py
|   |-- conftest.py                     # Shared fixtures
|   |
|   |-- unit/                           # Unit tests
|   |   |-- README.md
|   |   |-- __init__.py
|   |   |-- test_models.py              # Pydantic model validation
|   |   |-- test_attribution.py         # AttributionTracker logic
|   |   |-- test_tin_store.py           # TIN storage operations
|   |   |-- test_config.py              # Configuration loading
|   |   +-- test_dataset_loader.py      # Dataset loading
|   |
|   |-- integration/                    # Integration tests
|   |   |-- README.md
|   |   |-- __init__.py
|   |   |-- test_kafka_flink.py         # Kafka -> Flink pipeline
|   |   |-- test_flink_duckdb.py        # Flink -> DuckDB pipeline
|   |   +-- test_duckdb_query.py        # DuckDB query operations
|   |
|   +-- e2e/                            # End-to-end tests
|       |-- README.md
|       |-- __init__.py
|       |-- test_workflow.py            # Full pipeline workflow
|       |-- test_known_answer.py        # Known-answer test validation
|       |-- test_correctness.py         # All 8 correctness tests
|       +-- test_performance.py         # Performance benchmarks
|
|-- scripts/                            # Utility scripts
|   |-- README.md
|   |
|   |-- data/                           # Data utilities
|   |   |-- generate_tweet_data.py      # Generate sample tweet data
|   |   |-- download_nyc_taxi.py        # Download NYC Taxi dataset
|   |   +-- load_to_kafka.py            # Load data into Kafka
|   |
|   |-- pipeline/                       # Pipeline utilities
|   |   |-- run_pipeline.py             # Run complete pipeline
|   |   |-- run_flink_job.py            # Submit Flink job
|   |   +-- reset_pipeline.py           # Reset pipeline state
|   |
|   |-- experiments/                    # Experiment utilities
|   |   |-- run_experiment.py           # Run experiment configurations
|   |   |-- compare_provenance.py       # Compare with/without provenance
|   |   +-- collect_metrics.py          # Collect performance metrics
|   |
|   +-- utils/                          # General utilities
|       |-- verify_schema.py            # Verify schema correctness
|       +-- export_results.py           # Export results for analysis
|
|-- docs/                               # Documentation
|   |-- README.md
|   |-- architecture.md                 # System architecture
|   |-- schema.md                       # Schema reference
|   |-- api.md                          # API documentation
|   |-- dashboard.md                    # Dashboard user guide
|   |-- deployment.md                   # Deployment guide
|   |
|   |-- design/                         # Design documents
|   |   |-- tin_model.md                # TIN model design
|   |   |-- attribution_logic.md        # Attribution propagation logic
|   |   +-- query_algorithms.md         # Query algorithm design
|   |
|   +-- reports/                        # Progress reports
|       |-- week_4_report.md            # Baseline complete
|       |-- week_8_report.md            # First demo
|       +-- final_report.md             # FYP report draft
|
+-- related-work/                       # Reference papers
    |-- temporal-attribution.pdf
    |-- flink-deb.pdf
    +-- s00778-023-00819-8.pdf
```

## Setup

### Prerequisites
- Python 3.10.13 (enforced by `.python-version`)
- Kafka
- Apache Flink
- DuckDB

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd FYP-stream-processing
```

2. Install dependencies:
```bash
make install-dev
```

3. Copy environment configuration:
```bash
cp .env.example .env
```

4. Edit `.env` with your configuration if needed.

## Usage

### Run the Pipeline

```bash
# Generate test data
make setup-data

# Run the complete pipeline
python scripts/pipeline/run_pipeline.py
```

### Run the Dashboard

```bash
make run-dashboard
```

### Run Tests

```bash
# Run all tests
make test

# Run unit tests only
make test-unit

# Run with coverage
make test-cov
```

## Demo Scenario

**Tweet/Hashtag Pipeline:**
```
Tweets → Extract Hashtags → Filter (#python) → Window (COUNT) → Trending Score
```

**Known-Answer Test:**
```
Input: 5 tweets in 1-minute window
  - user_1: "Loving #python!" (2 tweets)
  - user_2: "#python is awesome" (1 tweet)
  - user_3: "Check out #java" (filtered out)
  - user_4: "#python #streaming" (1 tweet)

Output for #python: COUNT = 4
Expected Attribution: {user_1: 2, user_2: 1, user_4: 1}
```

## Datasets

| Tier | Dataset | Purpose | Size | Timeline |
|------|---------|---------|------|----------|
| v1 | Tweet/Hashtag | Demo + Correctness | ~50 tweets | Weeks 3-8 |
| v2 | NYC Taxi | Real-world validation | ~10M rows | Weeks 11-12 |
| v3 | Nexmark benchmark | Performance evaluation | Configurable | Weeks 11-12 |

## Development

### Coding Standards
- Python 3.10+ with strict type hints
- Pydantic V2 for data validation
- See `CODING_STYLE.md` for conventions

### Common Commands
```bash
make help           # Show all available commands
make install        # Install dependencies
make lint           # Run linters
make format         # Format code
make clean          # Clean up artifacts
```

## Team

- **Student A (Kalifa):** Data Pipeline & Processing
- **Student B:** Provenance, Storage & Dashboard

## References

- **Paper:** Kosyfaki, C., Zhang, R., Mamoulis, N., & Zhou, X. (2026). Toward temporal attribution analytics in dataflows. PVLDB 19(10), 2831-2838.
- **Related Work:** See `related-work/` directory for reference papers

## License

MIT
