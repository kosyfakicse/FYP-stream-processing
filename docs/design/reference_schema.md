# Schema Reference
# Demonstration and Evaluation of How-Much Provenance in Streaming Dataflows

**Reference:** Kosyfaki et al. (2026) - Toward temporal attribution analytics in dataflows, PVLDB 19(10)

---

## Overview

This document defines all data schemas used in the system:
- **Event Schema**: Unified format for streaming events
- **TIN Model**: Temporal Interaction Network structure
- **DuckDB Tables**: Storage schema for TINs and results
- **Attribution Metadata**: How provenance is tracked through operators

---

## 1. Unified Event Schema

All datasets (small_test, NYC Taxi, Nexmark) are normalized to this format before entering the pipeline.

### Base Event Structure

```python
{
    # Identification
    "run_id": "run-001",              # Experiment run identifier
    "source_id": "A",                  # Origin source (K1, VTS, bidder, etc.)
    "event_id": 1,                     # Unique event number within run
    
    # Data
    "quantity": 1,                     # Contributed quantity (usually 1 for count)
    "category": "X",                   # Grouping key for windows (optional)
    "timestamp": 1735696800000,        # Event time in milliseconds
    
    # Provenance (added at ingestion, propagated through operators)
    "attribution": {                   # How-much provenance dictionary
        "A": 1                         # {source_id: quantity}
    },
    
    # Raw data (original dataset fields)
    "raw": {                           # Original record from dataset
        "vendor_id": 1,
        "passenger_count": 2,
        "tpep_pickup_datetime": "2024-01-01 00:05:23",
        ...
    }
}
```

### Field Descriptions

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | VARCHAR | Yes | Experiment identifier for isolation |
| `source_id` | VARCHAR | Yes | Origin source ID (e.g., "user_1", "VTS", "bidder") |
| `event_id` | INTEGER | Yes | Sequential event number within run |
| `quantity` | INTEGER | Yes | Quantity contributed (default: 1) |
| `category` | VARCHAR | No | Grouping key for windowed aggregation (e.g., hashtag) |
| `timestamp` | BIGINT | Yes | Event time in milliseconds (epoch) |
| `attribution` | JSON | Yes | Dictionary `{source_id: quantity}` |
| `raw` | JSON | No | Original dataset record |

### Attribution Propagation Rules

| Operator | Behavior | Example (Tweet Scenario) |
|----------|----------|---------------------------|
| **Source** | Initialize: `{source_id: quantity}` | Tweet from user_1: `{user_1: 1}` |
| **Map** | Preserve attribution unchanged | Extract hashtags: `{user_1: 1}` -> `{user_1: 1}` |
| **Filter** | Pass through if accepted, drop if rejected | Filter #python: `{user_1: 1}` -> `{user_1: 1}` or `null` |
| **Window** | Merge attributions by summing | Count tweets: `{user_1: 2}`, `{user_2: 1}` -> `{user_1: 2, user_2: 1}` |
| **Branch** | Copy attribution to all branches | Multi-hashtag: `{user_1: 1}` -> #python `{user_1: 1}`, #streaming `{user_1: 1}` |
| **Merge** | Combine attributions from all inputs | Merge branches: `{user_1: 2}`, `{user_2: 3}` -> `{user_1: 2, user_2: 3}` |

---

## 2. TIN Model (Temporal Interaction Network)

The TIN is a directed graph **G = (V, E, R)** where:
- **V** = Vertices (operators)
- **E** = Edges (data flow paths)
- **R** = Interactions (data transfers with quantities)

### TIN Components

```
+-------------------------------------------------------------+
|                    TIN Structure                             |
+-------------------------------------------------------------+
|                                                              |
|  Vertices (V): Operators                                     |
|  |-- Source: K1, K2, K3 (data ingestion)                    |
|  |-- Map: M1, M2 (transformation)                           |
|  |-- Filter: F1 (acceptance/rejection)                      |
|  |-- Window: W1 (aggregation)                               |
|  +-- Sink: S1 (output)                                      |
|                                                              |
|  Edges (E): Data flow connections                            |
|  |-- K1 -> M1                                               |
|  |-- K2 -> M1                                               |
|  |-- M1 -> F1                                               |
|  |-- F1 -> W1                                               |
|  +-- W1 -> S1                                               |
|                                                              |
|  Interactions (R): Data transfers                            |
|  |-- (K1, M1, t=1000, qty=5, attr={K1: 5})                 |
|  |-- (K2, M1, t=1000, qty=3, attr={K2: 3})                 |
|  +-- (M1, F1, t=1001, qty=8, attr={K1: 5, K2: 3})          |
|                                                              |
|  States: Temporal snapshots at each vertex                   |
|  |-- (M1, t_start=1000, t_end=1010, qty=8, attr={K1:5,K2:3})|
|  +-- (W1, t_start=1000, t_end=1060, qty=4, attr={K1:3,K2:1})|
|                                                              |
+-------------------------------------------------------------+
```

### TIN Formal Definition

```
TIN = (V, E, R, S)

V = {v | v is an operator in the dataflow}
E = {(u, v) | u, v in V and data flows from u to v}
R = {(u, v, t, q, a) | interaction at time t with quantity q and attribution a}
S = {(v, t_start, t_end, q, a) | temporal state at vertex v}

Where:
  u, v in V (vertices)
  t in BIGINT (timestamp in ms)
  q in INTEGER (quantity)
  a in JSON ({source_id: quantity})
```

---

## 3. DuckDB Schema

### Table: `runs`

Stores experiment run metadata.

```sql
CREATE TABLE runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    dataset VARCHAR NOT NULL,
    config_json VARCHAR,
    provenance_enabled BOOLEAN NOT NULL
);
```

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | VARCHAR | Unique experiment identifier (PK) |
| `started_at` | TIMESTAMP | Run start time |
| `finished_at` | TIMESTAMP | Run completion time (NULL if running) |
| `dataset` | VARCHAR | Dataset name (small_test, nyc_taxi, nexmark) |
| `config_json` | VARCHAR | Full config.yaml as JSON string |
| `provenance_enabled` | BOOLEAN | Whether provenance was captured |

**Example:**
```
run-001 | 2026-10-26 10:00:00 | 2026-10-26 10:05:23 | small_test | {...} | true
```

---

### Table: `tin_vertices`

Stores operators in the dataflow graph.

```sql
CREATE TABLE tin_vertices (
    vertex_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    vertex_type VARCHAR NOT NULL,
    description VARCHAR
);
```

| Column | Type | Description |
|--------|------|-------------|
| `vertex_id` | VARCHAR | Operator identifier (PK) |
| `run_id` | VARCHAR | Experiment run (FK -> runs) |
| `vertex_type` | VARCHAR | Operator type: source, map, filter, window, sink |
| `description` | VARCHAR | Human-readable description |

**Vertex Types:**

| Type | Description | Example |
|------|-------------|---------|
| `source` | Data ingestion point | K1, VTS, bidder |
| `map` | Transformation operator | M1, identity-map |
| `filter` | Acceptance/rejection operator | F1, acceptance-filter |
| `window` | Aggregation operator | W1, tumbling-count-60s |
| `sink` | Output operator | S1, duckdb-sink |

**Example:**
```
K1 | run-001 | source | Source A (small_test)
M1 | run-001 | map    | Identity map operator
W1 | run-001 | window | Tumbling window, 60s, COUNT
```

---

### Table: `tin_edges`

Stores data flow connections between operators.

```sql
CREATE TABLE tin_edges (
    source_vertex VARCHAR NOT NULL,
    target_vertex VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    PRIMARY KEY (source_vertex, target_vertex, run_id)
);
```

| Column | Type | Description |
|--------|------|-------------|
| `source_vertex` | VARCHAR | Upstream operator |
| `target_vertex` | VARCHAR | Downstream operator |
| `run_id` | VARCHAR | Experiment run (FK -> runs) |

**Example:**
```
K1 -> M1 | run-001
K2 -> M1 | run-001
M1 -> F1 | run-001
F1 -> W1 | run-001
W1 -> S1 | run-001
```

---

### Table: `tin_interactions`

Stores individual data transfers between operators.

```sql
CREATE TABLE tin_interactions (
    interaction_id INTEGER PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    source_vertex VARCHAR NOT NULL,
    target_vertex VARCHAR NOT NULL,
    timestamp BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    attribution_json VARCHAR NOT NULL
);
```

| Column | Type | Description |
|--------|------|-------------|
| `interaction_id` | INTEGER | Unique interaction (PK) |
| `run_id` | VARCHAR | Experiment run (FK -> runs) |
| `source_vertex` | VARCHAR | Upstream operator |
| `target_vertex` | VARCHAR | Downstream operator |
| `timestamp` | BIGINT | Transfer time (ms) |
| `quantity` | INTEGER | Quantity transferred |
| `attribution_json` | VARCHAR | JSON: `{source_id: quantity}` |

**Example:**
```
1 | run-001 | K1 | M1 | 1735696800000 | 5 | {"A": 5}
2 | run-001 | K2 | M1 | 1735696800000 | 3 | {"B": 3}
3 | run-001 | M1 | F1 | 1735696800001 | 8 | {"A": 5, "B": 3}
4 | run-001 | F1 | W1 | 1735696800002 | 4 | {"A": 3, "B": 1}
```

---

### Table: `tin_states`

Stores temporal state snapshots at each vertex (for backward query indexing).

```sql
CREATE TABLE tin_states (
    state_id INTEGER PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    vertex_id VARCHAR NOT NULL,
    time_start BIGINT NOT NULL,
    time_end BIGINT,
    quantity INTEGER NOT NULL,
    attribution_json VARCHAR NOT NULL,
    fired_at BIGINT
);
```

| Column | Type | Description |
|--------|------|-------------|
| `state_id` | INTEGER | Unique state (PK) |
| `run_id` | VARCHAR | Experiment run (FK -> runs) |
| `vertex_id` | VARCHAR | Operator at this state |
| `time_start` | BIGINT | State start time (ms) |
| `time_end` | BIGINT | State end time (ms, NULL if ongoing) |
| `quantity` | INTEGER | Quantity at this state |
| `attribution_json` | VARCHAR | JSON: `{source_id: quantity}` |
| `fired_at` | BIGINT | When state was emitted (for windows) |

**Example:**
```
1 | run-001 | M1 | 1735696800000 | 1735696810000 | 8 | {"A": 5, "B": 3} | null
2 | run-001 | F1 | 1735696800000 | 1735696810000 | 4 | {"A": 3, "B": 1} | null
3 | run-001 | W1 | 1735696800000 | 1735696860000 | 4 | {"A": 3, "B": 1} | 1735696860000
```

---

### Table: `window_outputs`

Stores completed window aggregation results (query targets for backward attribution).

```sql
CREATE TABLE window_outputs (
    output_id INTEGER PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    vertex_id VARCHAR NOT NULL,
    window_start BIGINT NOT NULL,
    window_end BIGINT NOT NULL,
    output_count INTEGER NOT NULL,
    attribution_json VARCHAR NOT NULL
);
```

| Column | Type | Description |
|--------|------|-------------|
| `output_id` | INTEGER | Unique output (PK) |
| `run_id` | VARCHAR | Experiment run (FK -> runs) |
| `vertex_id` | VARCHAR | Window operator |
| `window_start` | BIGINT | Window start time (ms) |
| `window_end` | BIGINT | Window end time (ms) |
| `output_count` | INTEGER | Aggregated count |
| `attribution_json` | VARCHAR | JSON: `{source_id: quantity}` |

**Example:**
```
1 | run-001 | W1 | 1735696800000 | 1735696860000 | 4 | {"A": 3, "B": 1}
```

**Query:** "Where did W1's count=4 in window [1735696800000, 1735696860000) come from?"
**Answer:** A=3, B=1

---

## 4. Attribution Metadata Format

### JSON Structure

Attribution is stored as a JSON object mapping source IDs to quantities:

```json
{
    "A": 3,
    "B": 1,
    "C": 2
}
```

**Semantics:**
- Keys: `source_id` (VARCHAR) -- origin source identifier
- Values: `quantity` (INTEGER) -- contributed amount
- **Invariant:** `sum(values) = total_quantity` at that operator/state

### Merge Operation

When two events merge (e.g., at a window):

```python
def merge_attribution(attr1: dict, attr2: dict) -> dict:
    """Merge two attribution dictionaries by summing quantities."""
    merged = {}
    for source_id, qty in attr1.items():
        merged[source_id] = merged.get(source_id, 0) + qty
    for source_id, qty in attr2.items():
        merged[source_id] = merged.get(source_id, 0) + qty
    return merged

# Example:
# attr1 = {"A": 3, "B": 1}
# attr2 = {"A": 2, "C": 2}
# merged = {"A": 5, "B": 1, "C": 2}
```

### Backward Attribution Query (BFS)

```python
def backward_attribution(vertex_id: str, time_t: int) -> list:
    """
    Given a vertex at time t, trace backward to find source contributions.
    
    Returns:
        List of {source_id, quantity, time, path}
    """
    # 1. Find target state at (vertex_id, time_t)
    # 2. BFS upstream through tin_interactions
    # 3. Collect attribution at each step
    # 4. Return when reaching source vertices
```

---

## 5. Dataset-Specific Mappings

### tweet_hashtag (v1)

**Source CSV:**
```csv
timestamp,user_id,text,hashtags
1735696800000,user_1,"Loving #python and #streaming!","#python,#streaming"
1735696800001,user_2,"#python is awesome","#python"
1735696800002,user_3,"Check out #java","#java"
1735696800003,user_1,"More #python content","#python"
...
```

**Mapping to Unified Event:**
```python
{
    "run_id": "run-001",
    "source_id": row["user_id"],           # "user_1", "user_2", etc.
    "event_id": row_index,
    "quantity": 1,                          # Always 1 per tweet
    "category": row["hashtags"].split(",")[0],  # Primary hashtag: "#python"
    "timestamp": row["timestamp"],
    "attribution": {row["user_id"]: 1},    # {"user_1": 1}
    "raw": {
        "text": row["text"],
        "user_id": row["user_id"],
        "hashtags": row["hashtags"].split(",")  # ["#python", "#streaming"]
    }
}
```

**Pipeline:**
1. Source: Read tweets from CSV/Kafka
2. Map: Extract hashtags from tweet text
3. Filter: Keep only tweets with target hashtag (e.g., #python)
4. Window: Count tweets per user in time window
5. Sink: Write trending hashtags with attribution to DuckDB

**Query Example:**
```
"Which tweets/users caused #python to trend in window [t1, t2)?"
Answer: user_1 (2 tweets), user_2 (1 tweet), user_4 (1 tweet)
```

---

### NYC Taxi (v2)

**Source Parquet columns:**
- `VendorID` (1 or 2)
- `tpep_pickup_datetime`
- `passenger_count`
- `payment_type`

**Mapping to Unified Event:**
```python
source_mapping = {"1": "VTS", "2": "CTS"}

{
    "run_id": "run-002",
    "source_id": source_mapping[str(row["VendorID"])],  # "VTS" or "CTS"
    "event_id": row_index,
    "quantity": 1,                                        # Count trips
    "category": str(row["payment_type"]),                 # Group by payment
    "timestamp": int(row["tpep_pickup_datetime"].timestamp() * 1000),
    "attribution": {source_mapping[str(row["VendorID"])]: 1},
    "raw": row.to_dict()
}
```

---

### Nexmark (v3)

**Generator produces 3 event types:**
- `bidder` (Person)
- `auction` (Auction)
- `seller` (Seller)

**Mapping to Unified Event:**
```python
{
    "run_id": "run-003",
    "source_id": event_type,               # "bidder", "auction", "seller"
    "event_id": event.id,
    "quantity": 1,
    "category": event.category if hasattr(event, "category") else None,
    "timestamp": event.date_time,
    "attribution": {event_type: 1},
    "raw": event.to_dict()
}
```

---

## 6. Schema Relationships

```
+--------------------------------------------------------------+
|                     Schema Relationships                      |
+--------------------------------------------------------------+
|                                                               |
|  runs (1) ------< (N) tin_vertices                            |
|    |                    |                                     |
|    |                    | (1)                                 |
|    |                    v                                     |
|    |               (N) tin_edges                               |
|    |                    |                                     |
|    |                    | (N)                                 |
|    |                    v                                     |
|    |--------------< (N) tin_interactions                       |
|    |                    |                                     |
|    |                    |                                     |
|    |--------------< (N) tin_states                             |
|    |                    |                                     |
|    |                    |                                     |
|    +--------------< (N) window_outputs                         |
|                                                               |
|  Legend:                                                      |
|    (1) = one side of relationship                             |
|    (N) = many side of relationship                            |
|    ------< = foreign key relationship                         |
|                                                               |
+--------------------------------------------------------------+
```

---

## 7. Indexes for Performance

```sql
-- Speed up backward attribution queries
CREATE INDEX idx_states_vertex_time ON tin_states(vertex_id, time_start, time_end);
CREATE INDEX idx_interactions_source ON tin_interactions(source_vertex, target_vertex, timestamp);
CREATE INDEX idx_outputs_vertex ON window_outputs(vertex_id, window_start, window_end);

-- Run isolation
CREATE INDEX idx_run_id ON tin_vertices(run_id);
CREATE INDEX idx_run_id_interactions ON tin_interactions(run_id);
CREATE INDEX idx_run_id_states ON tin_states(run_id);
```

---

## 8. Example Queries

### Query: Get all vertices for a run
```sql
SELECT vertex_id, vertex_type, description
FROM tin_vertices
WHERE run_id = 'run-001'
ORDER BY vertex_id;
```

### Query: Get interactions between two operators
```sql
SELECT interaction_id, timestamp, quantity, attribution_json
FROM tin_interactions
WHERE source_vertex = 'K1' AND target_vertex = 'M1'
ORDER BY timestamp;
```

### Query: Get window output with attribution
```sql
SELECT window_start, window_end, output_count, attribution_json
FROM window_outputs
WHERE vertex_id = 'W1' AND run_id = 'run-001';
```

### Query: Find state at specific time
```sql
SELECT state_id, vertex_id, time_start, time_end, quantity, attribution_json
FROM tin_states
WHERE vertex_id = 'M1'
  AND time_start <= 1735696805000
  AND (time_end IS NULL OR time_end > 1735696805000);
```

---

## 9. Invariants and Constraints

| Invariant | Description | Check |
|-----------|-------------|-------|
| **Quantity Conservation** | Sum of attribution = total quantity | `sum(attribution.values()) == quantity` |
| **Run Isolation** | No cross-run contamination | All queries filter by `run_id` |
| **Source Identity** | Attribution keys are valid sources | `attribution.keys() subset of {source_ids}` |
| **Temporal Ordering** | `time_start < time_end` | All states respect time ordering |
| **Flow Conservation** | Input quantity = output quantity (for map/filter) | `sum(input.qty) == output.qty` |

---

## 10. Schema Evolution

### Current Version (Phase 1)
- Fixed tumbling windows
- COUNT aggregation only
- Single category grouping
- Group-level provenance (not per-record)

### Future Extensions
- Sliding windows
- SUM, AVG aggregations
- Multi-key grouping
- Joins
- Recovery-aware provenance

---

## Reference

Kosyfaki, C., Zhang, R., Mamoulis, N., & Zhou, X. (2026). Toward temporal attribution analytics in dataflows. *Proceedings of the VLDB Endowment, 19*(10), 2831-2838. https://doi.org/10.14778/3828612.3828635
