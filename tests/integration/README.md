# Integration Tests

This directory contains integration tests for component interactions.

## Test Files

- `test_kafka_flink.py` — Kafka → Flink pipeline (Week 3-4)
- `test_flink_duckdb.py` — Flink → DuckDB pipeline (Week 5-6)
- `test_duckdb_query.py` — DuckDB query operations (Week 7-8)

## Running Integration Tests

```bash
pytest tests/integration/
```

## Purpose

Integration tests verify that components work together correctly.
These tests may require external services (Kafka, Flink) to be running.

## Requirements

- Kafka broker running on configured port
- Flink cluster running
- DuckDB database accessible
