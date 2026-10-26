# Core Module

This module contains the core business logic for the FYP Attribution system.

## Components

- `models.py` — Pydantic data models for the entire system
- `attribution.py` — AttributionTracker for provenance tracking
- `tin.py` — TIN (Temporal Information Network) operations

## Key Models

### Attribution Models
- `Attribution` — Provenance dictionary with merge operations
- `UnifiedEvent` — Unified event schema for all datasets
- `TweetEvent` / `TweetRawData` — Tweet-specific models

### TIN Models
- `TINVertex` — Operators (source, map, filter, window, sink)
- `TINEdge` — Data flow connections
- `TINInteraction` — Data transfers with quantities
- `TINState` — Temporal state snapshots

### Output Models
- `WindowOutput` — Completed window aggregations
- `BackwardAttributionResult` — Query results

### Configuration Models
- `AppConfig` — Complete application configuration
- `DatasetConfig`, `KafkaConfig`, `FlinkConfig`, `DuckDBConfig`, etc.

## Attribution Propagation Rules

| Operator | Behavior |
|----------|----------|
| **Source** | Initialize: `{source_id: quantity}` |
| **Map** | Preserve attribution unchanged |
| **Filter** | Pass through if accepted, drop if rejected |
| **Window** | Merge attributions by summing |
| **Branch** | Copy attribution to all branches |
| **Merge** | Combine attributions from all inputs |

## Invariants

1. **Quantity Conservation**: `sum(attribution.values()) == quantity`
2. **Run Isolation**: All queries filter by `run_id`
3. **Source Identity**: Attribution keys are valid sources
4. **Temporal Ordering**: `time_start < time_end`
5. **Flow Conservation**: Input quantity = output quantity (for map/filter)
