# Configuration Files

This directory contains configuration files for the FYP Attribution system.

## Files

- `config.yaml` — Main configuration file for the entire system

## config.yaml Structure

The configuration file contains settings for:

- **Dataset**: Which dataset to use (tweet_hashtag, nyc_taxi, nexmark)
- **Kafka**: Broker addresses, topic names, consumer groups
- **Flink**: Parallelism, checkpointing, window sizes
- **DuckDB**: Database path, table names
- **Streamlit**: Dashboard port, display settings
- **Provenance**: Attribution tracking settings

## Usage

```python
from fyp_attribution.config import load_config

config = load_config("configs/config.yaml")
```

## Notes

- Configuration is validated using Pydantic models at runtime
- Environment variables can override config file values
- See `src/fyp_attribution/core/models.py` for configuration schema
