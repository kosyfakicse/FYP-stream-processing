# Source Code

This directory contains the main Python package for the FYP Attribution system.

## Package Structure

```
src/fyp_attribution/
├── core/           # Core business logic (models, attribution, TIN)
├── pipeline/       # Data pipeline (Kafka, Flink, DuckDB writer)
├── storage/        # Data storage (TIN store, query engine)
├── dashboard/      # Streamlit UI
├── config.py       # Configuration management
└── utils.py        # Helper functions
```

## Modules

### core/
- `models.py` — Pydantic data models (Attribution, UnifiedEvent, TIN structures)
- `attribution.py` — AttributionTracker for provenance tracking
- `tin.py` — TIN (Temporal Information Network) operations

### pipeline/
- `kafka_consumer.py` — Kafka message consumer
- `flink_processor.py` — Apache Flink stream processing
- `duckdb_writer.py` — DuckDB database writer

### storage/
- `tin_store.py` — TIN persistence layer
- `query_engine.py` — Backward attribution queries (BFS)

### dashboard/
- `app.py` — Main Streamlit application
- `pages/` — Dashboard pages (overview, backward attribution)
- `components/` — Reusable UI components (charts)

## Development

Install the package in development mode:
```bash
pip install -e ".[dev]"
```

Run tests:
```bash
pytest tests/
```

## Coding Standards

- Python 3.10+ with strict type hints
- Pydantic V2 for all data structures
- See `CODING_STYLE.md` for conventions
