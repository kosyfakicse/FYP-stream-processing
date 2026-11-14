# Weekly Report

**Project:** FYP - Temporal Attribution Analytics in Streaming Dataflows  
**Week:** 3-4  
**Date:** November 9, 2026

---

## Summary

The application pipeline is mostly complete. This week focused on implementing the end-to-end streaming pipeline with Kafka, PyFlink, and DuckDB integration.

---

## Completed Tasks

### 1. Kafka Setup on Docker

- Deployed Apache Kafka 3.7.0 using Docker container (`fyp-kafka`)
- Configured two topics for different purposes:
  - **`tweets-storage`**: Testing topic - bulk load all messages at once
  - **`tweets-raw`**: Replay topic - simulates real streaming dataflow with controlled rate

### 2. Event Generator (Kafka Producer)

- Created `scripts/kafka/bulk_load_tweets.py` for instant bulk loading
- Dataset: 1000 tweets from `data/raw/tweet_hashtag.csv`
- Unified event format with attribution tracking:
  ```python
  {
      "source_id": user_id,
      "category": hashtag,
      "timestamp": timestamp_ms,
      "attribution": {user_id: 1}
  }
  ```

### 3. PyFlink Pipeline (Kafka Consumer)

Implemented complete data processing pipeline:

| Operator | Function | Description |
|----------|----------|-------------|
| Source | Kafka Consumer | Reads from `tweets-storage` topic |
| Map | `parse_json()` | Extract (user_id, text, timestamp) |
| Map | `extract_hashtags()` | Find hashtags in text |
| FlatMap | `flatten_hashtags()` | Split tweet into (category, user_id, timestamp) per hashtag |
| Filter | `has_python()` | Filter by target hashtag (optional) |
| KeyBy | `key_by_category()` | Group by hashtag category |
| Window | TumblingProcessingTimeWindows | 5-second windows (processing time) |
| Process | `CategoryAttributionProcessFunction` | Aggregate per-hashtag attribution |
| Sink | `write_category_to_db()` | Write to database (currently PrintDB) |

**Output Format:**
```python
(window_start, window_end, category, output_count, attribution_json)
```

### 4. Storage Layer

- Implemented `DatabaseBatchWriter` with batch processing
- Created `PrintDB` (mimics DuckDB with print function for testing)
- Schema matches `window_outputs` table design

---

## Technical Challenges

### Challenge 1: PyFlink Version Compatibility (Major Blocker)

**Problem:**
- PyFlink 2.3.0 has critical issues:
  - Cannot work as Kafka consumer
  - Dependency conflicts with Kafka connector JARs
  - Bundled JARs cause classpath precedence problems

**Solution:**
- Downgraded to PyFlink 1.18.1
- This version has stable Kafka connector support
- All pipeline components work correctly after downgrade

**Time Spent:** ~60% of this week's effort

### Challenge 2: Custom Sink Implementation

**Problem:**
- PyFlink's `SinkFunction` API requires Java backend implementation
- Custom Python SinkFunction fails with `_j_function` AttributeError
- Java interop requirement makes custom sinks difficult to implement

**Solution:**
- Used `map()` function as workaround to replace sink functionality
- `write_to_db()` function handles database writes within the pipeline
- This approach works but has limitations:
  - Global variable isolation between main/worker processes
  - Batch writer effectively flushes after each record (batch_size=1)

**Impact:**
- May need to use `map()` function to capture provenance metadata
- Custom Sink API not viable without Java implementation effort

---

## Current Status

| Component | Status | Notes |
|-----------|--------|-------|
| Kafka Docker Setup | ✅ Complete | Two topics configured |
| Event Generator | ✅ Complete | Bulk load and replay scripts |
| PyFlink Pipeline | ✅ Complete | All operators implemented |
| Window Aggregation | ✅ Complete | Per-hashtag attribution |
| Database Sink | ⚠️ Partial | PrintDB works, DuckDB stub only |
| Provenance Tracking | ⚠️ Partial | Attribution dict captured, TIN not implemented |

---

## Next Week Plan

1. **Complete DuckDB Implementation**
   - Replace PrintDB with actual DuckDB writer
   - Implement `window_outputs` table schema

2. **Provenance Metadata Capture**
   - Use map function approach to track TIN interactions
   - Store attribution at each pipeline operator

3. **Event Time Windows**
   - Switch from processing time to event time
   - Implement watermark strategy using dataset timestamps

4. **Testing & Validation**
   - Verify attribution sums match output counts
   - Test with larger datasets (1000+ tweets)

---

## Key Learnings

1. **PyFlink Version Matters**: Always verify Kafka connector compatibility before choosing PyFlink version
2. **Java Interop Limitation**: Custom SinkFunction requires Java implementation - use map() workaround for Python-only projects
3. **Worker Process Isolation**: Global variables in PyFlink workers are not shared with main process - affects batch writer design
4. **Tweet Flattening**: Multi-hashtag tweets must be flattened before key_by() to achieve per-hashtag aggregation

---

## Files Modified This Week

- `src/fyp_attribution/main.py` - Dual workflow (legacy + category-based)
- `src/fyp_attribution/pipeline/flink.py` - CategoryAttributionProcessFunction
- `src/fyp_attribution/pipeline/db_writer.py` - DatabaseBatchWriter, PrintDB
- `configs/config.yaml` - Topic and window configuration
- `scripts/kafka/bulk_load_tweets.py` - Kafka producer script
- `docs/design/baseline.md` - Pipeline documentation
