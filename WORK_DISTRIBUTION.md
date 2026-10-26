# Work Distribution Plan (2 Students)
# Demonstration and Evaluation of How-Much Provenance in Streaming Dataflows

**Student A:** Kalifa Flink (Data Pipeline & Processing)
**Student B:** [Name] (Provenance, Storage & Dashboard)

---

## Split Strategy

Split by **concern** (not by layer):
- **Student A (Kalifa)**: Data Pipeline & Processing -- owns data flow from dataset to Flink output
- **Student B**: Provenance, Storage & Dashboard -- owns attribution tracking, TIN storage, queries, and UI

Both students collaborate on: design decisions, integration, testing, evaluation, and report writing.

---

## Component Ownership

```
Student A (Kalifa)                    Student B
------------------                    ------------------
[config.yaml]                         [attribution_tracker.py]
[dataset_loader.py]                   [duckdb_tin_store.py]
[load_to_kafka.py]                    [backward_query.py]
[flink_processor.py]                  [dashboard/app.py]
[download_nyc_taxi.py]                [generate_test_data.py]
[nexmark_adapter.py]                  [tests/test_correctness.py]
                                      [tests/test_known_answer.py]

Both:
-----
- Design document (Weeks 1-2)
- Integration testing (Weeks 9-10)
- Performance evaluation (Weeks 11-12)
- Report writing (continuous)
- Demo preparation (Weeks 15-16)
```

---

## Phase-by-Phase Distribution

### Phase 1: Specification and Design (Weeks 1-2)

| Task | Student A | Student B |
|------|-----------|-----------|
| Define event schema | Lead | Review |
| Design TIN tables | Review | Lead |
| Work through known-answer test | Together | Together |
| Design operator pipeline | Lead | Review |
| Write design document | Split sections | Split sections |

**Deliverables:**
- Student A: `config.yaml`, operator pipeline design
- Student B: TIN schema, expected results JSON
- Both: Design document

---

### Phase 2: Streaming Baseline (Weeks 3-4)

| Task | Student A | Student B |
|------|-----------|-----------|
| Dataset loader (v1: small CSV) | **Implement** | Test |
| Kafka producer (load + replay) | **Implement** | Test |
| Flink job (map, filter, window) | **Implement** | Test |
| DuckDB writer (outputs only) | Interface spec | **Implement** |
| Verify outputs match expected | Together | Together |

**Deliverables:**
- Student A: `dataset_loader.py`, `load_to_kafka.py`, `flink_processor.py`
- Student B: `duckdb_writer.py` (baseline, no provenance)
- Both: Integration test passes

**Key:** Build pipeline WITHOUT provenance first.

---

### Phase 3: Provenance Capture and Storage (Weeks 5-6)

| Task | Student A | Student B |
|------|-----------|-----------|
| Add attribution metadata to events | **Modify Flink job** | Define format |
| Attribution propagation in operators | **Implement in Flink** | Review logic |
| TIN state writer | Interface | **Implement** |
| Store vertices, edges, interactions, states | Verify data flow | **Verify storage** |
| Inspect provenance in DuckDB | Together | Together |

**Deliverables:**
- Student A: Updated `flink_processor.py` with attribution propagation
- Student B: `duckdb_tin_store.py`, `attribution_tracker.py`
- Both: Provenance inspectable in DuckDB

---

### Phase 4: Backward Query and UI (Weeks 7-8)

| Task | Student A | Student B |
|------|-----------|-----------|
| Backward attribution query (BFS) | Test with known data | **Implement** |
| Streamlit Overview page | Provide metrics | **Implement** |
| Streamlit Backward Attribution page | Verify correctness | **Implement** |
| Known-answer test passes | Together | Together |

**Deliverables:**
- Student A: Test data, integration support
- Student B: `backward_query.py`, `dashboard/app.py`
- Both: First end-to-end demo

---

### Phase 5: Correctness and Integration (Weeks 9-10)

| Task | Student A | Student B |
|------|-----------|-----------|
| Test 1-4 (single source, two sources, filter, no leakage) | **Implement tests** | Review |
| Test 5-8 (empty window, branch/merge, incomplete, isolation) | Review | **Implement tests** |
| Fix bugs found | **Fix pipeline bugs** | **Fix query/storage bugs** |
| Freeze core features | Together | Together |

**Deliverables:**
- Student A: `tests/test_pipeline.py` (Tests 1-4)
- Student B: `tests/test_correctness.py` (Tests 5-8)
- Both: All 8 tests pass

---

### Phase 6: Performance Evaluation (Weeks 11-12)

| Task | Student A | Student B |
|------|-----------|-----------|
| NYC Taxi adapter | **Implement** | Test with provenance |
| Nexmark adapter | **Implement** | Test with provenance |
| Run experiments (with/without provenance) | **Run pipeline** | **Run queries** |
| Collect metrics (throughput, latency, storage) | **Measure pipeline** | **Measure queries** |
| Write evaluation section | **Write pipeline metrics** | **Write query metrics** |

**Deliverables:**
- Student A: `download_nyc_taxi.py`, `nexmark_adapter.py`, pipeline metrics
- Student B: Experiment configs, query/storage metrics
- Both: Experiment results, evaluation section of report

---

### Phase 7: Hardware-Resource Analysis (Weeks 13-14) [Supporting]

| Task | Student A | Student B |
|------|-----------|-----------|
| Measure resource use | **Implement monitoring** | Analyze data |
| Fit baseline model | Provide data | **Implement model** |
| Validate on held-out workloads | Together | Together |
| Write supporting analysis section | Review | **Write** |

**Deliverables:**
- Student A: Resource monitoring scripts
- Student B: Prediction model, analysis section
- Both: Validated results or documented limits

---

### Phase 8: Finalisation (Weeks 15-16)

| Task | Student A | Student B |
|------|-----------|-----------|
| Finalise report | **Write pipeline chapters** | **Write provenance/query chapters** |
| Record demo video | **Demo pipeline** | **Demo dashboard** |
| Package reproducible environment | **Docker/setup** | **Test data/docs** |
| Final review | Together | Together |
| Submission | Together | Together |

**Deliverables:**
- Student A: Pipeline chapters, demo video (pipeline part), setup scripts
- Student B: Provenance/query chapters, demo video (dashboard part), documentation
- Both: Final report, demo, submission

---

## Integration Points (Both Students Must Collaborate)

| When | What | Why |
|------|------|-----|
| Week 2 | Schema agreement | Event schema must match pipeline + storage |
| Week 4 | Baseline integration | Pipeline must write to DuckDB correctly |
| Week 6 | Provenance integration | Flink attribution must match TIN schema |
| Week 8 | End-to-end demo | Query must read from pipeline output |
| Week 10 | Correctness freeze | All tests must pass together |
| Week 12 | Evaluation | Both pipeline and query metrics needed |
| Week 16 | Submission | Unified report and demo |

---

## Weekly Sync Meeting

**Frequency:** Once per week (mandatory)

**Agenda:**
1. What did each student complete this week?
2. What blockers exist?
3. What needs integration/collaboration?
4. What are next week's priorities?

**Communication:**
- Shared Git repository with clear branch strategy
- Shared document (report) with section ownership
- Issue tracker for bugs and tasks

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Student A falls behind | Student B can help with pipeline (knows interface) |
| Student B falls behind | Student A can help with queries (knows data format) |
| Integration fails | Weekly integration checkpoints (Weeks 4, 6, 8, 10) |
| Scope creep | Stick to priorities list; defer hardware prediction if needed |
| Both stuck | Escalate to supervisor immediately |

---

## Milestones Summary

| End of Week | Milestone | Student A Deliverable | Student B Deliverable |
|-------------|-----------|----------------------|----------------------|
| 2 | Semantics and schema agreed | config.yaml, pipeline design | TIN schema, expected results |
| 4 | Baseline pipeline works | Kafka + Flink scripts | DuckDB writer |
| 6 | Provenance persisted | Updated Flink with attribution | TIN store, tracker |
| 8 | First end-to-end demo | Test data, integration | Query, dashboard |
| 10 | Correctness suite passes | Tests 1-4 | Tests 5-8 |
| 12 | Main performance results | NYC Taxi, Nexmark, pipeline metrics | Experiment configs, query metrics |
| 14 | Supporting analysis done | Monitoring scripts | Prediction model |
| 16 | Final submission ready | Pipeline chapters, setup | Query chapters, docs |
