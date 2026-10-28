# Progress Report: Demonstration and Evaluation of How-Much Provenance in Streaming Dataflows

**Date:** October 27, 2026  
**Students:** Student A (CHUI Chun Sing) + Student B (LAU Kam Wing)  
**Reference Paper:** Kosyfaki et al. (2026) - Toward temporal attribution analytics in dataflows, PVLDB 19(10)  
**Current Phase:** Phase 1 -- Specification and Design (Weeks 1-2)

---

## 1. Project Overview

This FYP project builds a streaming system that demonstrates **how-much provenance** using **temporal attribution** based on the Temporal Interaction Network (TIN) model. The system focuses on **backward attribution** (Where-From queries), answering questions such as:

> "Which tweets/users caused this hashtag to trend?"

### System Architecture

```
Dataset (CSV/Parquet) --> Kafka (replay) --> Flink (process + attribution) --> DuckDB (TIN store) --> Streamlit (dashboard)
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Language | Python 3.10+ | Core implementation with strict type hints |
| Streaming | Apache Flink | Stream processing with provenance tracking |
| Message Queue | Apache Kafka | Data replay and streaming |
| Database | DuckDB | TIN storage and query engine |
| Dashboard | Streamlit | Interactive visualization |
| Validation | Pydantic V2 | Data models and configuration |
| Logging | Loguru | Centralized logging |

---

## 2. Current Progress

### Completed (Weeks 1-2)

| Item | Status | Details |
|------|--------|---------|
| Project structure | Complete | Full Python src/ layout with all directories and packages |
| Planning documents | Complete | SCHEMA.md, IMPLEMENTATION_PLAN.md, WORK_DISTRIBUTION.md, CODING_STYLE.md |
| Schema design | Complete | Unified event schema, TIN model, DuckDB table definitions |
| Configuration files | Complete | pyproject.toml, requirements.txt, .gitignore, .env.example, Makefile |
| Coding standards | Complete | Python 3.10+ strict typing, Pydantic V2 patterns, Google-style docstrings |
| Logging infrastructure | Complete | Loguru-based logging with file rotation and retention |
| Known-answer test design | Complete | Tweet/hashtag scenario with expected attribution results |
| Work distribution | Complete | Split by concern between two students |
| Git repository | Complete | Initial commit pushed to GitHub |

### In Progress

| Item | Status | Details |
|------|--------|---------|
| Pydantic data models | Planned | UnifiedEvent, TINVertex, TINEdge, TINInteraction, TINState, AppConfig |
| config.yaml | Planned | Dataset, Kafka, Flink, DuckDB, Streamlit configuration |
| Test data (tweet_hashtag.csv) | Planned | Sample tweet data for known-answer test |

---

## 3. Implementation Plan Summary

The project follows an 8-phase, 16-week plan:

### Phase 1: Specification and Design (Weeks 1-2) -- CURRENT

**Goal:** Design document + manually worked example

- Define event schema and attribution metadata format
- Design TIN tables for DuckDB storage
- Work through known-answer test by hand
- Design operator pipeline: Source -> Map -> Filter -> Window -> Sink

**Milestone:** Semantics and schema agreed

---

### Phase 2: Streaming Baseline (Weeks 3-4)

**Goal:** Pipeline produces correct outputs WITHOUT provenance

- Implement dataset loader (v1: small CSV)
- Implement Kafka producer for data replay
- Implement Flink job (map, filter, window operators)
- Implement DuckDB writer for baseline outputs
- Verify outputs match expected counts

**Milestone:** Baseline pipeline works end-to-end

**Key constraint:** Build and test the application pipeline BEFORE adding provenance capture.

---

### Phase 3: Provenance Capture and Storage (Weeks 5-6)

**Goal:** Small workload produces inspectable provenance in DuckDB

- Add attribution metadata to events at ingestion
- Implement attribution propagation in Flink operators (map/filter preserve, window merges)
- Implement TIN state writer to DuckDB
- Store vertices, edges, interactions, and states
- Verify provenance is inspectable via manual DuckDB queries

**Milestone:** Provenance persisted and queryable

---

### Phase 4: Backward Query and UI (Weeks 7-8)

**Goal:** Selecting an output returns correct source contributions

- Implement backward attribution query using BFS traversal
- Build Streamlit dashboard with Overview page
- Build Backward Attribution page with Sankey diagram, bar chart, and table
- Verify known-answer test passes (user_1=2, user_2=1, user_4=1)

**Milestone:** First end-to-end demonstration

---

### Phase 5: Correctness and Integration (Weeks 9-10)

**Goal:** Automated tests pass; core features frozen

8 correctness tests covering:
1. Single source / identity map
2. Two sources sum to expected count
3. Filter: only accepted records contribute
4. No leakage between consecutive windows
5. Empty window behaviour
6. Branch and merge without duplicate attribution
7. Incomplete capture is reported, not hidden
8. Results isolated by run_id

**Milestone:** All 8 correctness tests pass

---

### Phase 6: Performance Evaluation (Weeks 11-12)

**Goal:** Reproducible results and experiment logs

- Compare workloads WITH and WITHOUT provenance on same hardware
- Datasets: NYC Taxi (~10M rows) and Nexmark benchmark
- Metrics: throughput, query latency, storage overhead, CPU/memory usage
- Experiment matrix: 3 input rates x 3 history lengths, 3 repetitions each

**Milestone:** Main performance results collected

---

### Phase 7: Hardware-Resource Analysis (Weeks 13-14) [Supporting]

**Goal:** Validated resource estimates or documented characterisation

- Measure resource use under several workloads
- Fit a simple baseline model
- Validate on held-out workloads

**Milestone:** Supporting analysis completed or explicitly limited

---

### Phase 8: Finalisation (Weeks 15-16)

**Goal:** Report, demo, and reproducible package ready

- Finalise FYP report
- Record demo video
- Package reproducible environment
- Final review and submission

**Milestone:** Final submission ready

---

## 4. Work Distribution

Work is split by **concern** (not by layer):

### Student A (Kalifa) -- Data Pipeline and Processing

- Configuration (config.yaml)
- Dataset loaders (tweet, NYC Taxi, Nexmark)
- Kafka producer / data replay
- Flink stream processor with attribution propagation
- Pipeline-related correctness tests (Tests 1-4)

### Student B -- Provenance, Storage, and Dashboard

- Pydantic data models
- Attribution tracker
- DuckDB TIN store
- Backward attribution query engine (BFS)
- Streamlit dashboard
- Correctness tests (Tests 5-8) and known-answer test

### Both Students

- Design decisions and schema agreement
- Integration testing (Weeks 9-10)
- Performance evaluation (Weeks 11-12)
- Report writing (continuous)
- Demo preparation (Weeks 15-16)

---

## 5. Demo Scenario

The primary demonstration uses a tweet/hashtag scenario:

```
Pipeline:
  Tweets --> Extract Hashtags --> Filter (#python) --> Window (COUNT) --> Trending Score

Known-Answer Test:
  Input: 5 tweets in 1-minute window
    - user_1: "Loving #python!" (2 tweets)
    - user_2: "#python is awesome" (1 tweet)
    - user_3: "Check out #java" (filtered out)
    - user_4: "#python #streaming" (1 tweet)

  Output for #python: COUNT = 4
  Expected Attribution: {user_1: 2, user_2: 1, user_4: 1}
  Invariant: sum(attributed counts) = output count (2+1+1=4)
```

---

## 6. Dataset Strategy

| Tier | Dataset | Purpose | Size | Timeline |
|------|---------|---------|------|----------|
| v1 | Tweet/Hashtag | Demo + correctness verification | ~50 tweets | Weeks 3-8 |
| v2 | NYC Taxi | Real-world validation | ~10M rows | Weeks 11-12 |
| v3 | Nexmark benchmark | Performance evaluation | Configurable | Weeks 11-12 |

---

## 7. Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Source identity | Explicit source_id at ingestion | Matches TIN model from reference paper |
| Attribution format | Dictionary {source_id: quantity} per event | O(1) merge, efficient BFS tracing |
| Query type | Backward attribution (Q1: Where-From) | Core provenance question for stakeholders |
| Storage | DuckDB | Embedded, fast analytical queries, no server setup |
| Window type | Fixed tumbling time windows | Simplicity for Phase 1; extensible later |
| Aggregation | COUNT only (Phase 1) | Foundation for SUM, AVG extensions |

---

## 8. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Flink setup complexity | Medium | Medium | Start early; have fallback with pure Python pipeline |
| Kafka integration issues | Medium | Medium | Use file-based replay as backup |
| Attribution propagation bugs | High | High | Known-answer test provides ground truth for verification |
| Scope creep | Medium | High | Strict phase priorities; defer hardware prediction if needed |
| Performance overhead too high | Low | Medium | Document and analyse; not a blocking issue |

---

## 9. Next Steps (Weeks 3-4)

1. Set up Kafka and Flink environments
2. Implement Pydantic data models (models.py)
3. Implement dataset loader for tweet/hashtag CSV
4. Implement Kafka producer for data replay
5. Implement Flink job with map, filter, and window operators
6. Implement DuckDB writer for baseline outputs
7. Verify baseline pipeline produces correct counts WITHOUT provenance

---

## 10. Demo Deliverables and Supervisor Checkpoints

### Deliverables by Milestone

| Week | Milestone | Demo Deliverable |
|------|-----------|------------------|
| **4** | Baseline Pipeline | Pipeline running end-to-end WITHOUT provenance. Show: CSV data loaded into Kafka, Flink processing tweets, filtering hashtags, counting in windows, results written to DuckDB. Output counts match expected values. |
| **6** | Provenance Capture | Same pipeline WITH provenance. Show: DuckDB tables populated with TIN vertices, edges, interactions, states. Run manual SQL queries to inspect provenance data. Verify attribution invariants hold. |
| **8** | First End-to-End Demo | Full system demo. Show: Run pipeline, open Streamlit dashboard, select a trending hashtag, see backward attribution results with charts (Sankey diagram, bar chart). Known-answer test passes live (user_1=2, user_2=1, user_4=1). |
| **10** | Correctness Suite | Show: All 8 correctness tests passing in pytest. Demonstrate edge cases (empty windows, branch/merge, run isolation). Core features frozen. |
| **12** | Performance Evaluation | Show: NYC Taxi and Nexmark experiments running. Compare WITH vs WITHOUT provenance overhead. Present throughput, latency, and storage metrics with charts. |
| **14** | Resource Analysis | Show: Resource monitoring data, fitted model predictions vs actual measurements. |
| **16** | Final Submission | Full polished demo video, reproducible environment, final report. |

### Recommended Supervisor Checkpoints

**Checkpoint 1 -- Week 4 (Baseline Pipeline)**
- Prove the data pipeline works before adding complexity
- Show data flowing: CSV -> Kafka -> Flink -> DuckDB
- Demonstrate correct output counts

**Checkpoint 2 -- Week 8 (First End-to-End Demo)**
- This is the "wow moment" -- the full system working
- Live demo of the Streamlit dashboard
- Answer the core question: "Which tweets caused #python to trend?"
- Show the known-answer test passing

**Checkpoint 3 -- Week 12 (Performance Results)**
- Show real experimental data
- Compare provenance overhead with/without
- Validate on real-world datasets (NYC Taxi, Nexmark)

---

## 11. Repository

**GitHub:** https://github.com/kosyfakicse/FYP-stream-processing

### Key Documents in Repository

| File | Description |
|------|-------------|
| README.md | Project overview, setup instructions, complete project structure |
| IMPLEMENTATION_PLAN.md | Detailed 16-week technical plan |
| SCHEMA.md | Complete schema reference (event, TIN, DuckDB tables) |
| WORK_DISTRIBUTION.md | Student assignments and phase-by-phase breakdown |
| CODING_STYLE.md | Python coding conventions and Pydantic patterns |
| pyproject.toml | Package configuration and dependencies |
| requirements.txt | All Python dependencies |

---

**Next review:** End of Week 4 (Baseline pipeline milestone)
