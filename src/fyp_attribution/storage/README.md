# Storage Module

This module contains data storage and query components.

## Components

- `tin_store.py` — TIN (Temporal Information Network) persistence layer
- `query_engine.py` — Backward attribution query engine (BFS)

## TIN Store

The TIN store manages persistence of the Temporal Information Network in DuckDB.

### Tables
- `tin_vertices` — Operators in dataflow graph
- `tin_edges` — Data flow connections
- `tin_interactions` — Data transfers with quantities
- `tin_states` — Temporal state snapshots

## Query Engine

Implements backward attribution queries using BFS traversal.

### Query Types
- **Backward Attribution (Q1: Where-From)**: "Which tweets/users caused this hashtag to trend?"
- Traverses TIN graph backwards from output to sources
- Returns attribution breakdown by source

## Implementation Status

These components will be implemented in Weeks 5-6:
- TIN graph construction and storage
- BFS backward query algorithm
- Attribution result aggregation

## Usage Example

```python
from fyp_attribution.storage import TINStore, QueryEngine

store = TINStore(config.duckdb)
engine = QueryEngine(store)

# Query backward attribution
result = engine.backward_query(output_id="window_123")
print(result.attribution)  # {user_1: 2, user_2: 1, user_4: 1}
```
