# Implementation Plan
# Demonstration and Evaluation of How-Much Provenance in Streaming Dataflows

**Reference:** Kosyfaki et al. (2026) - Toward temporal attribution analytics in dataflows, PVLDB 19(10)

---

## Project Summary

Build a streaming system that demonstrates **how-much provenance** using **temporal attribution** (TIN-based), focusing on **backward attribution**. The system processes events through Kafka -> Flink -> DuckDB, with a Streamlit dashboard for demonstration and evaluation.

### Minimum Successful Project
> A supported streaming workload, group-level provenance capture, correct backward attribution, a clear demonstration, and reproducible cost measurements.

---

## Architecture

```
config.yaml (dataset + pipeline config)
       |
       v
+------------+    +----------+    +----------+    +----------+
|  Dataset   |--->|  Kafka   |--->|  Flink   |--->|  DuckDB  |
|  Loader    |    |  Replay  |    | +Prov    |    | TIN Store|
+------------+    +----------+    +----------+    +----------+
                                                    |
                                                    v
                                              +----------+
                                              | Streamlit|
                                              +----------+
                                              - Overview
                                              - Q1: Backward
```

---

## Scope

### In Scope (Phase 1)

| Component | Details |
|-----------|---------|
| Sources | 2-3 explicitly identified sources (source_id) |
| Operators | Quantity-preserving `map`, simple `filter` |
| Windows | Fixed tumbling time windows, COUNT aggregation |
| Outputs | Completed-window outputs only |
| Queries | Backward attribution (Q1: Where-From) |
| Parallelism | Single instance / low parallelism |

### Deferred

- Joins, sliding windows, arbitrary operators
- Recovery-aware provenance
- Hardware-resource prediction (supporting extension, after core works)

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Source identity | Explicit `source_id` at ingestion | Matches TIN model |
| Attributed quantity | Number of accepted input records contributing to a count | Count workloads |
| Output identity | `run_id`, operator/key, window interval, output ID | Reproducibility |
| Query input | A completed window-count output | Post-run analysis |
| Query result | Source ID, origin interval, contributed count | How-much provenance |
| Attribution metadata | Dictionary `{source_id: quantity}` per event/state | O(1) merge, BFS tracing |

### Known-Answer Test (Tweet/Hashtag Scenario)

```
Scenario: Track which tweets/users caused a hashtag to trend

Input Tweets (5 tweets in 1-minute window):
  - user_1: "Loving #python!"           -> hashtag: #python
  - user_2: "#python is awesome"        -> hashtag: #python
  - user_3: "Check out #java"           -> hashtag: #java (filtered out, not #python)
  - user_1: "More #python content"      -> hashtag: #python
  - user_4: "#python #streaming"        -> hashtag: #python

Pipeline:
  Source (tweets) -> Extract Hashtags -> Filter (#python only) -> Window (COUNT)

Window Output for #python:
  - user_1: 2 tweets contributed
  - user_2: 1 tweet contributed
  - user_4: 1 tweet contributed
  - Total COUNT = 4

Expected Attribution:
  {user_1: 2, user_2: 1, user_4: 1}
  
Invariant: sum(attributed counts) = output count
  2 + 1 + 1 = 4 [verified]

Query: "Which tweets caused #python to trend in window [t1, t2)?"
Answer: user_1 (2 tweets), user_2 (1 tweet), user_4 (1 tweet)
```

---

## Dataset Strategy (3 Tiers)

| Tier | Dataset | Purpose | Size | When |
|------|---------|---------|------|------|
| **v1** | Tweet/Hashtag (social media) | Demo + Correctness verification | ~50 tweets | Weeks 3-8 |
| **v2** | NYC Taxi | Real-world validation | ~10M rows | Weeks 11-12 |
| **v3** | Nexmark benchmark | Performance evaluation | Configurable | Weeks 11-12 |

### Configuration

```yaml
# config.yaml
dataset:
  name: "tweet_hashtag"    # switch to "nyc_taxi" or "nexmark"

  tweet_hashtag:
    file: "data/tweet_hashtag.csv"
    description: "Social media scenario: tweets with hashtags"
    # Pipeline: Tweet -> Extract Hashtags -> Count -> Trending
    # Query: "Which tweets/users caused this hashtag to trend?"

  nyc_taxi:
    file: "data/nyc_taxi.parquet"
    source_column: "vendor_id"
    timestamp_column: "tpep_pickup_datetime"
    quantity_column: "passenger_count"
    category_column: "payment_type"

  nexmark:
    scenario: "query3"
    num_events: 1000000
    events_per_second: 10000
    sources: ["bidder", "auction", "seller"]
```

### Unified Event Schema

All datasets map to:
```python
{
    "run_id": "run-001",
    "source_id": "user_1",           # Tweet author
    "event_id": 1,
    "quantity": 1,
    "category": "#python",           # Hashtag
    "timestamp": 1735696800000,
    "attribution": {"user_1": 1},
    "raw": {
        "text": "Loving #python and #streaming!",
        "user_id": "user_1",
        "hashtags": ["#python", "#streaming"]
    }
}
```

---

## DuckDB Schema (TIN Storage)

```sql
-- Experiment runs
CREATE TABLE runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    dataset VARCHAR,
    config_json VARCHAR,
    provenance_enabled BOOLEAN
);

-- TIN Vertices (operators)
CREATE TABLE tin_vertices (
    vertex_id VARCHAR PRIMARY KEY,
    run_id VARCHAR,
    vertex_type VARCHAR,        -- source, map, filter, window, sink
    description VARCHAR
);

-- TIN Edges (data flow)
CREATE TABLE tin_edges (
    source_vertex VARCHAR,
    target_vertex VARCHAR,
    run_id VARCHAR
);

-- TIN Interactions (data transfers)
CREATE TABLE tin_interactions (
    interaction_id INTEGER PRIMARY KEY,
    run_id VARCHAR,
    source_vertex VARCHAR,
    target_vertex VARCHAR,
    timestamp BIGINT,
    quantity INTEGER,
    attribution_json VARCHAR     -- {"A": 3, "B": 1}
);

-- TIN States (temporal state index)
CREATE TABLE tin_states (
    state_id INTEGER PRIMARY KEY,
    run_id VARCHAR,
    vertex_id VARCHAR,
    time_start BIGINT,
    time_end BIGINT,
    quantity INTEGER,
    attribution_json VARCHAR,
    fired_at BIGINT
);

-- Completed window outputs
CREATE TABLE window_outputs (
    output_id INTEGER PRIMARY KEY,
    run_id VARCHAR,
    vertex_id VARCHAR,
    window_start BIGINT,
    window_end BIGINT,
    output_count INTEGER,
    attribution_json VARCHAR
);
```

---

## 16-Week Implementation Plan

### Phase 1: Specification and Design (Weeks 1-2)

**Goal:** Design document + manually worked example

| Task | Output |
|------|--------|
| Define event schema and attribution metadata | `config.yaml`, schema doc |
| Design TIN tables for DuckDB | SQL schema |
| Work through known-answer test by hand | Expected results JSON |
| Design operator pipeline | Source -> Map -> Filter -> Window -> Sink |

**Milestone (End of Week 2):** Semantics and schema agreed

**Files to create:**
- `config.yaml`
- `data/raw/tweet_hashtag.csv` (known-answer test data)
- `data/raw/tweet_hashtag_expected.json` (expected backward attribution results)
- `scripts/data/generate_tweet_data.py`

---

### Phase 2: Streaming Baseline (Weeks 3-4)

**Goal:** Pipeline produces correct outputs WITHOUT provenance

| Task | Output |
|------|--------|
| Implement dataset loader (v1: small CSV) | `src/fyp_attribution/pipeline/dataset_loader.py` |
| Implement Kafka producer (load + replay) | `scripts/data/load_to_kafka.py` |
| Implement Flink job (map, filter, window) | `src/fyp_attribution/pipeline/flink_processor.py` |
| Implement DuckDB writer (outputs only) | `src/fyp_attribution/pipeline/duckdb_writer.py` |
| Verify: outputs match expected counts | Test passes |

**Milestone (End of Week 4):** Baseline pipeline works

**Key constraint:** Build and test the application pipeline BEFORE adding provenance capture.

---

### Phase 3: Provenance Capture and Storage (Weeks 5-6)

**Goal:** Small workload produces inspectable provenance in DuckDB

| Task | Output |
|------|--------|
| Add attribution metadata to events | Updated event schema |
| Implement attribution propagation in Flink operators | Map/filter preserve, window merges |
| Implement TIN state writer | `src/fyp_attribution/storage/tin_store.py` |
| Store: vertices, edges, interactions, states | DuckDB tables populated |
| Verify: provenance inspectable in DuckDB | Manual query check |

**Milestone (End of Week 6):** Provenance persisted

**Key design:**
- Keep application stream and provenance stream separate
- Filters must retain which source groups survived (not just input/output totals)
- Use one collector/writer for DuckDB

---

### Phase 4: Backward Query and UI (Weeks 7-8)

**Goal:** Selecting an output returns correct source contributions

| Task | Output |
|------|--------|
| Implement backward attribution query (BFS) | `src/fyp_attribution/storage/query_engine.py` |
| Implement Streamlit Overview page | `src/fyp_attribution/dashboard/app.py` |
| Implement Streamlit Backward Attribution page | Sankey + bar chart + table |
| Verify: known-answer test passes | A=3, B=1 |

**Milestone (End of Week 8):** First end-to-end demonstration

**Query flow:**
```
Select output -> Find provenance target -> Follow dependencies upstream -> Combine at sources
```

**Avoid:** Double counting when traversing through branches and merges.

---

### Phase 5: Correctness and Integration (Weeks 9-10)

**Goal:** Automated tests pass; core features frozen

| Test | Description |
|------|-------------|
| Single source / identity map | Attribution = output count |
| Two sources sum to expected count | A + B = output |
| Filter: only accepted records contribute | Filtered sources excluded |
| No leakage between consecutive windows | Window boundaries respected |
| Empty window behaviour defined | Zero output, no attribution |
| Branch and merge without duplicate attribution | Correct merge |
| Incomplete capture is reported, not hidden | Flag partial results |
| Results isolated by run_id | No cross-run contamination |

**Invariant:** `sum(attributed_count(s)) == output_count` for all count workloads

**Milestone (End of Week 10):** Correctness suite passes

**Files:**
- `tests/unit/test_attribution.py`
- `tests/e2e/test_correctness.py`
- `tests/e2e/test_known_answer.py`

---

### Phase 6: Performance Evaluation (Weeks 11-12)

**Goal:** Reproducible results and experiment logs

**Method:** Compare same workload WITH and WITHOUT provenance capture on same hardware.

| Metric | Measurement |
|--------|-------------|
| Query latency | Repeated queries; median and tail percentile |
| Throughput | Records processed per second |
| Storage overhead | Bytes of captured provenance |
| Resource usage | CPU and memory during processing |
| Scalability | Vary input rate, history length, state count |

**Datasets:**
- NYC Taxi (v2): Real-world workload
- Nexmark (v3): Standardized benchmark

**Experiment matrix:** 3 input rates x 3 history lengths, repeated runs

**Milestone (End of Week 12):** Main performance results collected

**Files:**
- `scripts/data/download_nyc_taxi.py`
- `src/fyp_attribution/pipeline/nexmark_adapter.py`
- `tests/e2e/test_performance.py`
- `scripts/experiments/` (experiment configs and results)

---

### Phase 7: Hardware-Resource Analysis (Weeks 13-14) [Supporting]

**Goal:** Validated estimates, or documented resource characterisation

| Step | Description |
|------|-------------|
| 1 | Measure resource use under several workloads |
| 2 | Repeat selected runs with different resource allocations |
| 3 | Start with one target (e.g., storage growth or peak memory) |
| 4 | Fit a simple baseline model |
| 5 | Validate on held-out workloads; report only within tested ranges |

**Milestone (End of Week 14):** Supporting analysis completed or explicitly limited

---

### Phase 8: Finalisation (Weeks 15-16)

**Goal:** Report, demo, and reproducible package ready

| Task | Output |
|------|--------|
| Finalise report (write continuously) | FYP report |
| Record demo video | Demo artifact |
| Package reproducible environment | Docker/setup scripts |
| Final review and submission | Submitted FYP |

**Milestone (End of Week 16):** Final submission ready

---

## File Structure

See `README.md` for the complete current and final-state project structures.

---

## Correctness Tests (Detail)

### Test 1: Single User / Identity Map
```
user_1: 10 tweets with #python -> Extract Hashtags (identity) -> Window COUNT = 10
Expected: user_1 = 10
```

### Test 2: Two Users Sum
```
user_1: 7 tweets with #python -> Extract -> Window COUNT = 12
user_2: 5 tweets with #python -> Extract -> Window
Expected: user_1 = 7, user_2 = 5, sum = 12
```

### Test 3: Filter (Hashtag Selection)
```
user_1: 4 tweets with #python -> Filter (#python only, keep 3) -> Window COUNT = 4
user_2: 3 tweets with #python -> Filter (keep 1) -> Window
Expected: user_1 = 3, user_2 = 1
```

### Test 4: No Leakage Between Windows
```
Window 1 (t=0-60s): user_1=3, user_2=2 (tweets in window 1 only)
Window 2 (t=60-120s): user_1=1, user_2=4 (tweets in window 2 only)
No cross-window attribution.
```

### Test 5: Empty Window
```
No tweets with #python in window -> COUNT = 0, attribution = {}
```

### Test 6: Branch and Merge (Multi-Hashtag Tweet)
```
user_1 tweets "#python #streaming" -> Branch1 (extract #python) -> Merge -> Window
                                  -> Branch2 (extract #streaming) -> Merge
user_1 counted once for #python, not twice.
```

### Test 7: Incomplete Capture
```
If provenance capture fails mid-run (e.g., hashtag extraction error), flag result as incomplete.
```

### Test 8: Run Isolation
```
Run 1 and Run 2 with same tweet data -> results isolated by run_id.
```

---

## Performance Evaluation Plan

### Experiment Matrix

| Factor | Values |
|--------|--------|
| Input rate | 1K, 10K, 100K records/sec |
| History length | 1 min, 10 min, 1 hour |
| Provenance | Enabled, Disabled |
| Dataset | NYC Taxi, Nexmark |
| Repetitions | 3 per configuration |

### Metrics to Collect

```python
metrics = {
    "throughput_rps": "records per second",
    "query_latency_median_ms": "median backward query time",
    "query_latency_p99_ms": "99th percentile query time",
    "storage_bytes": "total provenance storage used",
    "storage_overhead_ratio": "prov_storage / no_prov_storage",
    "cpu_percent": "average CPU usage",
    "memory_peak_mb": "peak memory usage"
}
```

---

## Priorities If Behind Schedule

1. Correct backward attribution
2. Reproducible demonstration
3. Correctness tests
4. Main performance evaluation
5. Extra workloads and live UI features
6. Hardware prediction

---

## Dependencies

```txt
# requirements.txt
confluent-kafka>=2.3.0
apache-flink>=1.18.0
duckdb>=1.0.0
streamlit>=1.30.0
plotly>=5.18.0
pandas>=2.0.0
pyarrow>=14.0.0
pyyaml>=6.0
pydantic>=2.5.0
pydantic-settings>=2.1.0
loguru>=0.7.0
pytest>=7.0.0
pytest-cov>=4.1.0
```

---

## Reference

Kosyfaki, C., Zhang, R., Mamoulis, N., & Zhou, X. (2026). Toward temporal attribution analytics in dataflows. *Proceedings of the VLDB Endowment, 19*(10), 2831-2838. https://doi.org/10.14778/3828612.3828635
