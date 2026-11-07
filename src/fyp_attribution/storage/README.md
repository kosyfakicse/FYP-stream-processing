# Storage Module

This module contains data storage and query components.

## Components

- `tin_store.py` — TIN (Temporal Information Network) persistence layer (Data Access Layer)
- `provenance_service.py` — Service Layer handling business logic and DTO mapping
- `query_engine.py` — Backward attribution query engine (BFS)

## TIN Store

The TIN store manages persistence of the Temporal Information Network in DuckDB.

### Tables
- `runs` — Experiment run metadata
- `tin_vertices` — Operators in dataflow graph
- `tin_edges` — Data flow connections
- `tin_interactions` — Data transfers with quantities
- `tin_states` — Temporal state snapshots
- `window_outputs` — Final aggregated results for backward queries

## Provenance Service
The Service layer handles (e.g., generating unique 64-bit `output_id`s using UUID, converting attribution dictionaries to JSON strings) before passing pure SQL parameters to the Data Access Layer (`tin_store.py`).

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
from fyp_attribution.core.models import WindowOutputDTO
from fyp_attribution.storage.provenance_service import ProvenanceService

# 1. Initialize the Service (internally uses the TINStore Singleton)
service = ProvenanceService()

# 2. Create the DTO (Pydantic automatically validates types)
data = WindowOutputDTO(
    run_id="run-001",
    vertex_id="W1",
    window_start=1735696800000,
    window_end=1735696860000,
    output_count=4,
    attribution_json={"user_1": 2, "user_2": 1, "user_4": 1}
)

# 3. Save via Service Layer (handles JSON conversion & UUID generation)
output_id = service.save_window_output(data)
print(f"Saved with ID: {output_id}")

store = TINStore(config.duckdb)
engine = QueryEngine(store)

# 4. Query backward attribution (To be implemented in Weeks 5-6)
result = engine.backward_query(output_id="window_123")
print(result.attribution)  # {user_1: 2, user_2: 1, user_4: 1}
```
