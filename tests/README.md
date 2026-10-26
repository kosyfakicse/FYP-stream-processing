# Tests

This directory contains the test suite for the FYP Attribution system.

## Structure

```
tests/
├── unit/           # Unit tests for individual components
├── integration/    # Integration tests for component interactions
└── e2e/            # End-to-end workflow tests
```

## Test Categories

### Unit Tests (`unit/`)
Test individual components in isolation:
- `test_models.py` — Pydantic model validation
- `test_attribution.py` — AttributionTracker logic
- `test_tin_store.py` — TIN storage operations
- `test_config.py` — Configuration loading
- `test_dataset_loader.py` — Dataset loading

### Integration Tests (`integration/`)
Test component interactions:
- `test_kafka_flink.py` — Kafka → Flink pipeline
- `test_flink_duckdb.py` — Flink → DuckDB pipeline
- `test_duckdb_query.py` — DuckDB query operations

### End-to-End Tests (`e2e/`)
Test complete workflows:
- `test_workflow.py` — Full pipeline workflow
- `test_known_answer.py` — Known-answer test validation
- `test_correctness.py` — All 8 correctness tests
- `test_performance.py` — Performance benchmarks

## Running Tests

```bash
# Run all tests
pytest

# Run specific test category
pytest tests/unit/
pytest tests/integration/
pytest tests/e2e/

# Run with coverage
pytest --cov=src/fyp_attribution --cov-report=html

# Or use make
make test
make test-unit
make test-cov
```

## Test Coverage

Coverage reports are generated automatically when running tests with `--cov`.
View HTML coverage report: `htmlcov/index.html`

## Known-Answer Test

The key correctness test validates attribution for the tweet/hashtag scenario:

```
Input: 5 tweets in 1-minute window
  - user_1: "Loving #python!" (2 tweets)
  - user_2: "#python is awesome" (1 tweet)
  - user_3: "Check out #java" (filtered out)
  - user_4: "#python #streaming" (1 tweet)

Output for #python: COUNT = 4
Expected Attribution: {user_1: 2, user_2: 1, user_4: 1}
Invariant: sum(attributed counts) = output count (2+1+1=4 ✓)
```
