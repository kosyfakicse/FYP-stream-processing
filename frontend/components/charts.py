import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# ------------------------------------------------------------------
# Overview
# ------------------------------------------------------------------
def create_trending_hashtags_chart(top_df: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar chart of top hashtags.
    Expects columns: category (or hashtag) and total_heat (or count).
    """
    df = top_df.copy()

    # Normalise column names
    if "hashtag" in df.columns and "category" not in df.columns:
        df = df.rename(columns={"hashtag": "category"})
    if "count" in df.columns and "total_heat" not in df.columns:
        df = df.rename(columns={"count": "total_heat"})

    fig = px.bar(
        df,
        x="total_heat",
        y="category",
        orientation="h",
        title="Most Discussed Hashtags (Across all windows)",
        color="total_heat",
        color_continuous_scale="Blues",
        text="total_heat",
        labels={"total_heat": "Total Count", "category": "Hashtag"},
    )
    fig.update_layout(
        yaxis={"categoryorder": "total ascending"},
        coloraxis_showscale=False,
        margin=dict(l=20, r=20, t=50, b=20),
        height=350,
    )
    fig.update_traces(textposition="outside")
    return fig


# ------------------------------------------------------------------
# Backward Query
# ------------------------------------------------------------------
def create_backward_bar_chart(attr_df: pd.DataFrame, output_id: str) -> go.Figure:
    """
    Vertical bar chart of contributor attribution weights for one output window.
    Expects columns: contributor, weight
    """
    df = attr_df.copy()
    fig = px.bar(
        df,
        x="contributor",
        y="weight",
        title=f"Contributor Weights · {output_id}",
        labels={"contributor": "User", "weight": "Attribution Weight"},
        color="weight",
        color_continuous_scale="Teal",
        text=df["weight"].map(lambda x: f"{x:.1%}"),
    )
    fig.update_layout(
        coloraxis_showscale=False,
        margin=dict(l=20, r=20, t=50, b=20),
        height=320,
        xaxis_tickangle=-30,
    )
    fig.update_traces(textposition="outside")
    return fig


def create_attribution_donut_chart(
    attr_df: pd.DataFrame, title: str = "Attribution Weight Share"
) -> go.Figure:
    """
    Donut chart of contributor weight share.
    Expects columns: contributor, weight
    """
    df = attr_df.copy()
    fig = go.Figure(
        data=[
            go.Pie(
                labels=df["contributor"],
                values=df["weight"],
                hole=0.45,
                textinfo="label+percent",
                hovertemplate="%{label}<br>Weight: %{value:.1%}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title=title,
        margin=dict(l=20, r=20, t=50, b=20),
        height=320,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.15),
    )
    return fig


# ------------------------------------------------------------------
# Temporal Lineage
# ------------------------------------------------------------------
def create_temporal_trend_chart(
    tag_df: pd.DataFrame, selected_tag: str
) -> go.Figure:
    """
    Line chart of output_count over successive windows for one hashtag.
    Expects columns: window_start (or window_label), output_count
    """
    df = tag_df.copy()

    # Prefer human-readable label if present
    x_col = "window_label" if "window_label" in df.columns else "window_start"

    fig = px.line(
        df,
        x=x_col,
        y="output_count",
        markers=True,
        title=f"Temporal Trend · {selected_tag}",
        labels={x_col: "Time Window", "output_count": "Total Count"},
    )
    fig.update_traces(line=dict(width=3), marker=dict(size=9))
    fig.update_layout(
        margin=dict(l=20, r=20, t=50, b=20),
        height=360,
        xaxis_title="Time Window",
        yaxis_title="Output Count",
    )
    return fig


# ------------------------------------------------------------------
# Flow Lineage
# ------------------------------------------------------------------
def create_flow_network_chart(
    flow_df: pd.DataFrame, run_id: str = ""
) -> go.Figure:
    """
    Dot-to-dot (node-link) chart for flow lineage.
    Hover on a node shows summary data.
    """
    df = flow_df.copy()

    # Aggregate duplicates
    df = (
        df.groupby(["source_vertex", "target_vertex"], as_index=False)["total_quantity"]
        .sum()
    )

    if df.empty:
        fig = go.Figure()
        fig.update_layout(title="No flow data", height=300)
        return fig

    sources = df["source_vertex"].unique().tolist()
    targets = df["target_vertex"].unique().tolist()

    # Pre-calculate totals for hover
    source_totals = df.groupby("source_vertex")["total_quantity"].sum().to_dict()
    target_totals = df.groupby("target_vertex")["total_quantity"].sum().to_dict()

    # Position nodes
    node_x, node_y, node_text, node_color, node_hover = [], [], [], [], []

    # Users on the left
    for i, user in enumerate(sources):
        total = source_totals.get(user, 0)
        node_x.append(0)
        node_y.append(i)
        node_text.append(user)
        node_color.append("#8b5cf6")
        node_hover.append(
            f"<b>{user}</b><br>"
            f"Total contribution: {total:.0f}<br>"
            f"Type: Source User"
        )

    # Hashtags on the right
    y_scale = len(sources) / max(len(targets), 1)
    for i, tag in enumerate(targets):
        total = target_totals.get(tag, 0)
        node_x.append(1)
        node_y.append(i * y_scale)
        node_text.append(tag)
        node_color.append("#3b82f6")
        node_hover.append(
            f"<b>{tag}</b><br>"
            f"Total received: {total:.0f}<br>"
            f"Type: Hashtag"
        )

    # Edges
    edge_x, edge_y = [], []
    for _, row in df.iterrows():
        src = row["source_vertex"]
        tgt = row["target_vertex"]
        x0, y0 = 0, sources.index(src)
        x1 = 1
        y1 = targets.index(tgt) * y_scale
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    fig = go.Figure()

    # Lines
    fig.add_trace(
        go.Scatter(
            x=edge_x,
            y=edge_y,
            mode="lines",
            line=dict(width=1.8, color="rgba(148, 163, 184, 0.55)"),
            hoverinfo="none",
            showlegend=False,
        )
    )

    # Nodes (with hover)
    fig.add_trace(
        go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers+text",
            text=node_text,
            textposition=["middle right"] * len(sources) + ["middle left"] * len(targets),
            textfont=dict(size=12, color="#e2e8f0"),
            marker=dict(
                size=20,
                color=node_color,
                line=dict(width=2, color="white"),
            ),
            hovertext=node_hover,
            hoverinfo="text",
            showlegend=False,
        )
    )

    title = "Flow Lineage"
    if run_id:
        title = f"Flow Lineage · {run_id}"

    fig.update_layout(
        title=title,
        showlegend=False,
        hovermode="closest",
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-0.2, 1.3]),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        margin=dict(l=10, r=10, t=50, b=10),
        height=420,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


# ------------------------------------------------------------------
# Versioning / Window Comparison
# ------------------------------------------------------------------
def create_version_comparison_chart(
    comp_df: pd.DataFrame, win1_id: str, win2_id: str
) -> go.Figure:
    """
    Grouped bar chart comparing contributor weights between two windows.
    Expects columns: contributor, Window 1 Weight, Window 2 Weight
    """
    df = comp_df.copy()

    # Melt to long form for grouped bars
    melted = df.melt(
        id_vars="contributor",
        value_vars=["Window 1 Weight", "Window 2 Weight"],
        var_name="Window",
        value_name="Weight",
    )

    # Friendlier legend labels
    melted["Window"] = melted["Window"].replace({
        "Window 1 Weight": f"Window 1 ({win1_id})",
        "Window 2 Weight": f"Window 2 ({win2_id})",
    })

    fig = px.bar(
        melted,
        x="contributor",
        y="Weight",
        color="Window",
        barmode="group",
        title="Contributor Weight Comparison",
        labels={"contributor": "User", "Weight": "Attribution Weight"},
        color_discrete_sequence=["#636EFA", "#EF553B"],
    )
    fig.update_layout(
        margin=dict(l=20, r=20, t=50, b=20),
        height=360,
        xaxis_tickangle=-30,
        legend=dict(orientation="h", yanchor="bottom", y=-0.25),
    )
    return fig