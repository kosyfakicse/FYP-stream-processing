import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from src.fyp_attribution.dashboard.components.page_setup import init_page

init_page("TIN Network")

st.title("TIN Network Topology")
st.markdown(
    "Visualize the flow of data through the Temporal Interaction Network"
    " (Vertices and Edges) using mock data."
)

# ==========================================
# 1. Define Mock Nodes and Edges
# ==========================================
nodes = [
    # Users (Contributors)
    {"id": "U1", "label": "User A", "x": -2, "y": 1,  "color": "#9467bd", "type": "User"},
    {"id": "U2", "label": "User B", "x": -2, "y": 0,  "color": "#9467bd", "type": "User"},
    {"id": "U3", "label": "User C", "x": -2, "y": -1, "color": "#9467bd", "type": "User"},

    # Operators
    {"id": "K1", "label": "Kafka Source", "x": 0,   "y": 0, "color": "#ff7f0e", "type": "Source"},
    {"id": "M1", "label": "Map (Extract)", "x": 1.5, "y": 0, "color": "#1f77b4", "type": "Map"},
    {"id": "F1", "label": "Filter (#python)", "x": 3, "y": 0, "color": "#2ca02c", "type": "Filter"},
    {"id": "W1", "label": "Window (Count)", "x": 4.5, "y": 0, "color": "#d62728", "type": "Window"},
]

edges = [
    {"source": "U1", "target": "K1", "weight": 2},
    {"source": "U2", "target": "K1", "weight": 3},
    {"source": "U3", "target": "K1", "weight": 1},

    {"source": "K1", "target": "M1", "weight": 6},
    {"source": "M1", "target": "F1", "weight": 4},  # Filter drops some data
    {"source": "F1", "target": "W1", "weight": 4},
]

# ==========================================
# 2. Prepare Plotly Graph
# ==========================================
edge_x = []
edge_y = []
for edge in edges:
    x0 = next(n["x"] for n in nodes if n["id"] == edge["source"])
    y0 = next(n["y"] for n in nodes if n["id"] == edge["source"])
    x1 = next(n["x"] for n in nodes if n["id"] == edge["target"])
    y1 = next(n["y"] for n in nodes if n["id"] == edge["target"])
    edge_x.extend([x0, x1, None])
    edge_y.extend([y0, y1, None])

edge_trace = go.Scatter(
    x=edge_x, y=edge_y,
    line=dict(width=2, color="#666"),
    hoverinfo="none",
    mode="lines",
)

node_x = [n["x"] for n in nodes]
node_y = [n["y"] for n in nodes]
node_text = [n["label"] for n in nodes]
node_color = [n["color"] for n in nodes]

node_trace = go.Scatter(
    x=node_x, y=node_y,
    mode="markers+text",
    hoverinfo="text",
    text=node_text,
    textposition="bottom center",
    marker=dict(size=30, color=node_color, line_width=2, line_color="white"),
)

fig = go.Figure(
    data=[edge_trace, node_trace],
    layout=go.Layout(
        title=dict(text="Data Flow & Provenance Architecture", font=dict(size=18)),
        showlegend=False,
        hovermode="closest",
        margin=dict(b=20, l=20, r=20, t=40),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=500,
    ),
)

# ==========================================
# 3. Render Graph and Data Tables
# ==========================================
st.markdown("---")

col1, col2 = st.columns([2, 1])

with col1:
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("### Vertices (Nodes)")
    st.dataframe(pd.DataFrame(nodes), use_container_width=True, hide_index=True)

    st.markdown("---")

    st.markdown("### Edges (Connections)")
    st.dataframe(pd.DataFrame(edges), use_container_width=True, hide_index=True)