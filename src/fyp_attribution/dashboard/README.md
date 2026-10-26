# Dashboard Module

This module contains the Streamlit dashboard for visualization and interaction.

## Components

- `app.py` — Main Streamlit application entry point
- `pages/overview.py` — Overview page with pipeline status
- `pages/backward_attribution.py` — Backward attribution query interface
- `components/charts.py` — Reusable chart components

## Pages

### Overview
- Pipeline status and metrics
- Recent window outputs
- Dataflow visualization

### Backward Attribution
- Query interface for "Where-From" queries
- Select window output to analyze
- Display attribution breakdown by source
- Visualize attribution with charts

## Implementation Status

These components will be implemented in Weeks 7-8:
- Streamlit app structure
- Page layouts and navigation
- Interactive charts with Plotly
- Real-time pipeline monitoring

## Usage

```bash
# Run the dashboard
streamlit run src/fyp_attribution/dashboard/app.py

# Or use make
make run-dashboard
```

## Features

- Interactive backward attribution queries
- Real-time pipeline monitoring
- Attribution visualization with charts
- Export query results
- Filter by time range, dataset, run ID
