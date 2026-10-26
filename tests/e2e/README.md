# End-to-End Tests

This directory contains end-to-end tests for complete workflows.

## Test Files

- `test_workflow.py` — Full pipeline workflow (Week 7-8)
- `test_known_answer.py` — Known-answer test validation (Week 9-10)
- `test_correctness.py` — All 8 correctness tests (Week 9-10)
- `test_performance.py` — Performance benchmarks (Week 11-12)

## Running E2E Tests

```bash
pytest tests/e2e/
```

## Purpose

End-to-end tests verify that the entire system works together,
from data ingestion through processing to query results.

## Known-Answer Test

The critical correctness test:

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
