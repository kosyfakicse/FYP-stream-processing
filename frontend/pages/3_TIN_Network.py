import pandas as pd
import streamlit as st

from datetime import datetime, timezone
from components.page_setup import init_page
from components.api_client import get_runs, get_run_graph
from components.charts import create_tin_network_chart


init_page("TIN Network")

st.title("TIN Network Topology")
st.markdown(
    "Visualize the Temporal Interaction Network (vertices, edges, states, and interactions) "
    "for a pipeline run."
)

# ------------------------------------------------------------------
# Run selector
# ------------------------------------------------------------------
try:
    runs = get_runs()
    finished_runs = [r for r in runs if r.get("finishedAt")]
    available_runs = [r["runId"] for r in finished_runs] or ["No finished runs"]
except Exception as e:
    st.error(f"Could not reach the API: {e}")
    available_runs = ["No finished runs"]

selected_run_id = st.selectbox(
    "Select Run",
    options=available_runs,
    help="Choose a finished pipeline run to visualize its TIN graph.",
)

if selected_run_id == "No finished runs":
    st.info("No finished runs available. Run the pipeline from Insert Data first.")
    st.stop()

# ------------------------------------------------------------------
# Load graph
# ------------------------------------------------------------------
try:
    graph = get_run_graph(selected_run_id)
except Exception as e:
    st.error("Failed to load TIN graph for this run.")
    detail = ""
    if hasattr(e, "response") and e.response is not None:
        try:
            detail = e.response.json().get("error", e.response.text)
        except Exception:
            detail = getattr(e.response, "text", str(e))
    else:
        detail = str(e)

    st.code(detail, language="text")

    st.info(
        "**Common causes:**\n"
        "- The run is older than 2026-11-22 (before the full TIN capture).\n"
        "- The DuckDB file was created before the `category` column was added to `tin_states` / `tin_interactions`.\n"
        "- Check `logs/backend.log` on the server for the exact exception.\n\n"
        "**Workaround:** Run a new dataset from **Insert Data**. New runs should produce a complete graph."
    )
    st.stop()

vertices = graph.get("vertices", [])
edges = graph.get("edges", [])
states = graph.get("states", [])
interactions = graph.get("interactions", [])

if not vertices:
    st.warning("This run has no TIN graph data.")
    st.stop()

# ------------------------------------------------------------------
# Controls
# ------------------------------------------------------------------
vertex_ids = [v["vertexId"] for v in vertices]

# Preferred pipeline order for stepping
type_order = {"source": 0, "map": 1, "filter": 2, "window": 3, "sink": 4}
ordered_ids = [
    v["vertexId"]
    for v in sorted(
        vertices,
        key=lambda v: (type_order.get(v.get("vertexType", ""), 99), v.get("vertexId", "")),
    )
]

def format_ms(ms):
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def categories_in(rows):
    found = []
    seen = set()
    for row in rows:
        cat = row.get("category")
        if cat and cat not in seen:
            seen.add(cat)
            found.append(cat)
    return sorted(found)

def windows_for(category):
    marks = set()
    for row in interactions:
        if row.get("category") == category and row.get("timestamp") is not None:
            marks.add(int(row["timestamp"]))
    for row in states:
        if row.get("category") == category and row.get("timeEnd") is not None:
            marks.add(int(row["timeEnd"]))
    return sorted(marks)

def rows_for(rows, category, window_ms, time_key):
    kept = []
    for row in rows:
        if row.get("category") != category:
            continue
        if window_ms is not None:
            value = row.get(time_key)
            if value is None or int(value) != window_ms:
                continue
        kept.append(row)
    return kept

def with_upstream(involved):
    """Category rows start at the map. Walk the edges backward so replay begins at the source."""
    reached = set(involved)
    changed = True
    while changed:
        changed = False
        for edge in edges:
            src = edge.get("sourceVertex")
            tgt = edge.get("targetVertex")
            if tgt in reached and src not in reached:
                reached.add(src)
                changed = True
    return reached

def quantity_for(vertex, cat_interactions, cat_states, window_ms):
    outgoing = [row for row in cat_interactions if row.get("sourceVertex") == vertex]
    incoming = [row for row in cat_interactions if row.get("targetVertex") == vertex]
    state_rows = [row for row in cat_states if row.get("vertexId") == vertex]
    if outgoing and not incoming:
        return sum((row.get("quantity") or 0) for row in outgoing)
    if incoming and not outgoing:
        return sum((row.get("quantity") or 0) for row in incoming)
    if state_rows:
        return sum((row.get("quantity") or 0) for row in state_rows)

    # K1 (and any node before the tag exists) stores a null category.
    plain = [
        row for row in states
        if row.get("vertexId") == vertex and not row.get("category")
        and (window_ms is None or (row.get("timeEnd") is not None and int(row["timeEnd"]) == window_ms))
    ]
    if not plain:
        return 0
    if window_ms is None:
        latest = max(plain, key=lambda row: int(row.get("timeEnd") or 0))
        return latest.get("quantity") or 0
    return sum((row.get("quantity") or 0) for row in plain)

replay_on = st.toggle(
    "Replay mode",
    value=False,
    help="Pick a category and step through the nodes it flows through.",
)

categories = categories_in(interactions + states)
highlighted = set()
highlighted_edges = set()
graph_vertices = vertices
graph_edges = edges
table_filter_nodes = None
table_edges_closed = False
table_filter_category = None
table_window_ms = None
table_window_col = None

if replay_on:
    if not categories:
        st.info("This run has no category to replay. Tags appear from the map onward.")
    else:
        col_cat, col_win = st.columns(2)
        with col_cat:
            selected_category = st.selectbox(
                "Category",
                options=categories,
                help="Hashtag to follow through the graph.",
            )
        window_marks = windows_for(selected_category)
        window_labels = ["All windows"] + [format_ms(ms) for ms in window_marks]
        with col_win:
            window_label = st.selectbox(
                "Time window",
                options=window_labels,
                help="One firing of this category, or every window added together.",
            )
        window_ms = None if window_label == "All windows" else window_marks[window_labels.index(window_label) - 1]

        cat_interactions = rows_for(interactions, selected_category, window_ms, "timestamp")
        cat_states = rows_for(states, selected_category, window_ms, "timeEnd")

        involved = set()
        for row in cat_interactions:
            involved.add(row.get("sourceVertex"))
            involved.add(row.get("targetVertex"))
        involved = with_upstream(involved)
        stages = [vid for vid in ordered_ids if vid in involved]

        if "replay_index" not in st.session_state:
            st.session_state.replay_index = 0
        if "replay_was_on" not in st.session_state:
            st.session_state.replay_was_on = False
        if "replay_sequence_key" not in st.session_state:
            st.session_state.replay_sequence_key = None

        # Window is not part of the key, so changing it keeps the current node.
        sequence_key = (selected_category, tuple(stages))
        if sequence_key != st.session_state.replay_sequence_key or not st.session_state.replay_was_on:
            st.session_state.replay_index = 0
            st.session_state.replay_sequence_key = sequence_key
        st.session_state.replay_was_on = True

        if not stages:
            st.info(f"No path for {selected_category} in this window.")
        else:
            st.session_state.replay_index = max(0, min(st.session_state.replay_index, len(stages) - 1))
            current = stages[st.session_state.replay_index]
            quantity = quantity_for(current, cat_interactions, cat_states, window_ms)

            col_prev, col_info, col_next = st.columns([1, 2, 1])
            with col_prev:
                if st.button("Previous", use_container_width=True, disabled=st.session_state.replay_index == 0):
                    st.session_state.replay_index -= 1
                    st.rerun()
            with col_next:
                if st.button("Next", use_container_width=True, disabled=st.session_state.replay_index >= len(stages) - 1):
                    st.session_state.replay_index += 1
                    st.rerun()
            with col_info:
                st.caption(
                    f"Step {st.session_state.replay_index + 1} / {len(stages)}  ·  "
                    f"**{selected_category}** at **{current}**  ·  quantity {quantity}"
                )

            highlighted = {current}
            table_filter_nodes = {current}
            table_filter_category = selected_category
            table_window_ms = window_ms
else:
    st.session_state.replay_was_on = False
    selected_nodes = st.multiselect(
        "Select node(s)",
        options=vertex_ids,
        default=[],
        help="Show the selected nodes, the nodes they connect to, and the rows for that group.",
    )
    if selected_nodes:
        selected = set(selected_nodes)
        visible = set(selected)
        kept_edges = []
        for edge in edges:
            src = edge.get("sourceVertex")
            tgt = edge.get("targetVertex")
            if src in selected or tgt in selected:
                if src:
                    visible.add(src)
                if tgt:
                    visible.add(tgt)
                kept_edges.append(edge)
        graph_vertices = [v for v in vertices if v.get("vertexId") in visible]
        graph_edges = kept_edges
        highlighted = selected
        table_filter_nodes = visible
        table_edges_closed = True
        related = [vid for vid in ordered_ids if vid in visible and vid not in selected]
        if related:
            st.caption(f"Selected: {', '.join(selected_nodes)}  ·  connected: {', '.join(related)}")
        else:
            st.caption(f"Selected: {', '.join(selected_nodes)}")

# ------------------------------------------------------------------
# Graph
# ------------------------------------------------------------------
fig = create_tin_network_chart(
    graph_vertices,
    graph_edges,
    run_id=selected_run_id,
    highlighted=highlighted,
    highlighted_edges=highlighted_edges,
)
st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------
# Tables
# ------------------------------------------------------------------
def filter_by_nodes(df, nodes, require_both_ends=False):
    if df.empty or not nodes:
        return df
    if (
        require_both_ends
        and "sourceVertex" in df.columns
        and "targetVertex" in df.columns
    ):
        return df[df["sourceVertex"].isin(nodes) & df["targetVertex"].isin(nodes)]
    mask = False
    for col in ["vertexId", "sourceVertex", "targetVertex"]:
        if col in df.columns:
            mask = mask | df[col].isin(nodes)
    return df[mask] if isinstance(mask, pd.Series) else df

def filter_by_category(df, category):
    if df.empty or not category or "category" not in df.columns:
        return df
    matched = df[df["category"] == category]
    if not matched.empty:
        return matched
    # Source and map rows are stored with no category.
    return df[df["category"].isna()]

def filter_by_window(df, window_ms, col):
    if df.empty or window_ms is None or col not in df.columns:
        return df
    return df[pd.to_numeric(df[col], errors="coerce") == window_ms]

tab_v, tab_e, tab_s, tab_i = st.tabs(["Vertices", "Edges", "States", "Interactions"])

with tab_v:
    df_v = pd.DataFrame(graph_vertices)
    if table_filter_nodes and "vertexId" in df_v.columns:
        df_v = filter_by_nodes(df_v, table_filter_nodes)
    if not df_v.empty:
        st.dataframe(df_v, use_container_width=True, hide_index=True)
    else:
        st.info("No vertices.")

with tab_e:
    df_e = pd.DataFrame(graph_edges)
    if table_filter_nodes:
        df_e = filter_by_nodes(df_e, table_filter_nodes, table_edges_closed)
    if not df_e.empty:
        st.dataframe(df_e, use_container_width=True, hide_index=True)
    else:
        st.info("No edges.")

with tab_s:
    if states:
        df_s = pd.DataFrame(states)
        cols = [c for c in ["vertexId", "category", "timeStart", "timeEnd", "quantity", "attribution", "firedAt"] if c in df_s.columns]
        df_s = df_s[cols] if cols else df_s
        if table_filter_nodes:
            df_s = filter_by_nodes(df_s, table_filter_nodes)
        df_s = filter_by_category(df_s, table_filter_category)
        df_s = filter_by_window(df_s, table_window_ms, "timeEnd")
        if not df_s.empty:
            st.dataframe(df_s, use_container_width=True, hide_index=True)
        else:
            st.info("No state rows for this step.")
    else:
        st.info("No state rows.")

with tab_i:
    if interactions:
        df_i = pd.DataFrame(interactions)
        cols = [c for c in ["sourceVertex", "targetVertex", "category", "timestamp", "quantity", "attribution"] if c in df_i.columns]
        df_i = df_i[cols] if cols else df_i
        if table_filter_nodes:
            df_i = filter_by_nodes(df_i, table_filter_nodes, table_edges_closed)
        df_i = filter_by_category(df_i, table_filter_category)
        df_i = filter_by_window(df_i, table_window_ms, "timestamp")
        if not df_i.empty:
            st.dataframe(df_i, use_container_width=True, hide_index=True)
        else:
            st.info("No interaction rows for this step.")
    else:
        st.info("No interaction rows.")
