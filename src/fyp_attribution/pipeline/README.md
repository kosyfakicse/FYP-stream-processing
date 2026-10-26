# Pipeline Module

This module contains data pipeline components for streaming processing.

## Components

- `kafka_consumer.py` — Kafka message consumer
- `flink_processor.py` — Apache Flink stream processing
- `duckdb_writer.py` — DuckDB database writer

## Architecture

```
Kafka → Flink Processor → DuckDB Writer
       (stream processing)  (persistence)
```

## Pipeline Flow

1. **Kafka Consumer**: Reads events from Kafka topics
2. **Flink Processor**: Applies transformations, filters, windows
3. **DuckDB Writer**: Stores results and provenance in DuckDB

## Implementation Status

These components will be implemented in Weeks 3-4:
- Event deserialization
- Stream processing operators
- Attribution propagation through pipeline
- Database persistence

## Usage Example

```python
from fyp_attribution.pipeline import KafkaConsumer, FlinkProcessor, DuckDBWriter

consumer = KafkaConsumer(config.kafka)
processor = FlinkProcessor(config.flink)
writer = DuckDBWriter(config.duckdb)

# Pipeline will be fully implemented in Phase 2
```
