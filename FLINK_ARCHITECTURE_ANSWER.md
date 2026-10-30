# Flink Architecture Answer

## Do you need an adapter between Flink and DuckDB?

**No, you don't need a separate adapter.**

Flink can write directly to DuckDB using its **Sink API** (SinkFunction or custom connector).

## Architecture

```
Dataset Loader -> Kafka -> Flink -> DuckDB
                              |
                         (direct write)
```

## Flink's Main Operations

1. **Source**: Read events from Kafka topic
2. **Map**: Extract hashtags from tweet text
3. **Filter**: Keep only target hashtag (e.g., #python)
4. **Window**: Tumbling time window for COUNT aggregation
5. **Sink**: Write results directly to DuckDB tables

## Why No Adapter Needed?

- Flink has built-in connectors for various systems
- For DuckDB, you implement a custom SinkFunction
- The sink receives Flink's output and writes to database
- No intermediate component required

## When Would You Need an Adapter?

Only if:
- You need complex transformation after Flink processing
- You want to decouple Flink from storage (rare)
- Multiple downstream consumers need the data

For your use case: **Flink writes directly to DuckDB**.
