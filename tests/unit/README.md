# Unit Tests

This directory contains unit tests for individual components.

## Test Files

- `test_models.py` — Pydantic model validation (Week 1-2)
- `test_attribution.py` — AttributionTracker logic (Week 5-6)
- `test_tin_store.py` — TIN storage operations (Week 5-6)
- `test_config.py` — Configuration loading (Week 3)
- `test_dataset_loader.py` — Dataset loading (Week 3)

## Running Unit Tests

```bash
pytest tests/unit/
```

## Purpose

Unit tests verify that individual components work correctly in isolation,
without dependencies on external services like Kafka or Flink.
