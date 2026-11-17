from datetime import datetime
import pandas as pd
import streamlit as st
from components.charts import (
    create_attribution_donut_chart,
    create_backward_bar_chart,
    create_flow_network_chart,
    create_temporal_trend_chart,
    create_version_comparison_chart,
)
from components.page_setup import init_page
from components.api_client import get_runs, get_run, windows_to_df
from components.query_engine import QueryEngine

init_page("Query & Lineage")

st.title("Query & Lineage Explorer")
st.markdown(
    "Trace provenance, contributor weights, temporal trends, and flow lineage"
    " across attribution windows."
)

# ------------------------------------------------------------------
# Run selector (from API)
# ------------------------------------------------------------------
try:
    runs = get_runs()
    available_runs = [r["runId"] for r in runs] or ["No runs"]
except Exception as e:
    st.error(f"Could not reach the API: {e}")
    available_runs = ["No runs"]

latest_run = st.session_state.get("latest_run_id", None)
default_idx = 0
if latest_run and latest_run in available_runs:
    default_idx = available_runs.index(latest_run)

selected_run_id = st.selectbox(
    "Active Run ID",
    options=available_runs,
    index=default_idx,
    help="Select a pipeline run ID.",
)

# Load the windows for the selected run
window_df = pd.DataFrame()
if selected_run_id != "No runs":
    try:
        run_data = get_run(selected_run_id)
        window_df = windows_to_df(run_data)
    except Exception as e:
        st.error(f"Failed to load run: {e}")

engine = QueryEngine(window_df)   # pass the DataFrame

st.markdown("---")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Backward Query",
    "Forward Query",
    "Temporal Lineage",
    "Flow Lineage",
    "Versioning Query",
])

# ------------------------------------------------------------------
# Tab 1: Backward Query
# ------------------------------------------------------------------
with tab1:
    st.subheader("Backward Provenance Query")
    st.caption("Trace a window output back to its contributing users.")

    categories = engine.list_categories(selected_run_id)
    if not categories:
        st.info("No data available for this run.")
    else:
        selected_tag = st.selectbox("Filter by Hashtag", categories, key="bw_tag")
        output_ids = engine.list_output_ids(selected_tag, selected_run_id)

        if output_ids:
            selected_output_id = st.selectbox(
                "Select Output ID (Window)", output_ids, key="bw_output"
            )

            result = engine.get_backward_query(selected_output_id, selected_run_id)

            if isinstance(result, str):
                st.error(result)
            else:
                st.markdown(f"**Output ID: {result['output_id']}**")
                m1, m2 = st.columns(2)
                m1.metric("Total Count", result["output_count"])
                m2.metric("Window Start (ms)", result["window_start"])

                # Build contributor DataFrame for charts
                attr_df = pd.DataFrame([
                    {"contributor": k, "weight": v}
                    for k, v in result["weights"].items()
                ])

                if not attr_df.empty:
                    left, right = st.columns([1.3, 1])
                    with left:
                        fig_bar = create_backward_bar_chart(attr_df, selected_output_id)
                        st.plotly_chart(fig_bar, use_container_width=True)

                        fig_donut = create_attribution_donut_chart(
                            attr_df, title="Attribution Weight Share"
                        )
                        st.plotly_chart(fig_donut, use_container_width=True)

                    with right:
                        st.markdown("**Contributor Table**")
                        attr_df["attributed_count"] = (
                            attr_df["weight"] * result["output_count"]
                        ).round(2)
                        st.dataframe(attr_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No contributor data.")
        else:
            st.info("No windows found for this hashtag.")

# ------------------------------------------------------------------
# Tab 2: Forward Query
# ------------------------------------------------------------------
with tab2:
    st.subheader("Forward Impact Query")
    st.caption("See all windows influenced by a source user.")

    contributors = engine.list_contributors(selected_run_id)
    if contributors:
        source_id = st.selectbox("Select Source User", contributors, key="fw_user")
    else:
        source_id = st.text_input("Enter Source User ID", value="user_1", key="fw_text")

    if st.button("Run Forward Query", type="primary", key="fw_btn") and source_id:
        result = engine.get_forward_query(source_id, selected_run_id)

        if isinstance(result, str):
            st.info(result)
        else:
            st.success(f"Found {len(result)} window(s) influenced by {source_id}.")
            df = pd.DataFrame(result)
            if not df.empty:
                st.metric("Impacted Windows", len(df))
                st.metric(
                    "Total Attributed Volume",
                    round(df["attributed_count"].sum(), 2),
                )
                st.dataframe(df, use_container_width=True, hide_index=True)

# ------------------------------------------------------------------
# Tab 3: Temporal Lineage
# ------------------------------------------------------------------
with tab3:
    st.subheader("Temporal Lineage Query")
    st.caption("How a hashtag’s volume changes across windows.")

    categories = engine.list_categories(selected_run_id)
    if not categories:
        st.info("No hashtags available.")
    else:
        selected_tag = st.selectbox("Select Hashtag", categories, key="temp_tag")
        result = engine.get_temporal_lineage(selected_tag, selected_run_id)

        if isinstance(result, str):
            st.info(result)
        else:
            tag_df = result.copy()
            tag_df["window_label"] = tag_df["window_start"].apply(
                lambda ts: datetime.fromtimestamp(float(ts) / 1000.0).strftime("%H:%M:%S")
                if pd.notnull(ts) and float(ts) > 1e10
                else str(ts)
            )

            left, right = st.columns([1.3, 1])
            with left:
                fig = create_temporal_trend_chart(tag_df, selected_tag)
                st.plotly_chart(fig, use_container_width=True)
            with right:
                st.markdown("**Window Breakdown**")
                display_cols = [
                    c for c in ["output_id", "window_label", "output_count", "attribution_json"]
                    if c in tag_df.columns
                ]
                st.dataframe(tag_df[display_cols], use_container_width=True, hide_index=True)

# ------------------------------------------------------------------
# Tab 4: Flow Lineage
# ------------------------------------------------------------------
with tab4:
    st.subheader("Flow Lineage Query")
    st.caption("Provenance flow from users to hashtags.")

    result = engine.get_flow_lineage(selected_run_id, min_quantity=0.0)

    if isinstance(result, str):
        st.info(result)
    else:
        max_qty = float(result["total_quantity"].max()) if not result.empty else 1.0
        min_filter = st.slider(
            "Minimum Flow Quantity",
            min_value=0.0,
            max_value=max(1.0, max_qty),
            value=0.0,
            step=0.5,
        )
        filtered = result[result["total_quantity"] >= min_filter]

        left, right = st.columns([1.3, 1])
        with left:
            if not filtered.empty:
                fig = create_flow_network_chart(filtered, selected_run_id)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No flows match the filter.")
        with right:
            st.markdown("**Flow Table**")
            st.dataframe(
                filtered[["source_vertex", "target_vertex", "total_quantity"]],
                use_container_width=True,
                hide_index=True,
            )

# ------------------------------------------------------------------
# Tab 5: Versioning Query
# ------------------------------------------------------------------
with tab5:
    st.subheader("Versioning & Window Comparison")
    st.caption("Compare contributor weights between two windows.")

    categories = engine.list_categories(selected_run_id)
    if not categories:
        st.info("No hashtags available.")
    else:
        ver_tag = st.selectbox("Select Hashtag", categories, key="ver_tag")
        output_ids = engine.list_output_ids(ver_tag, selected_run_id)

        if len(output_ids) >= 1:
            c1, c2 = st.columns(2)
            with c1:
                win1 = st.selectbox("Window 1 (Baseline)", output_ids, index=0, key="ver_w1")
            with c2:
                default_w2 = 1 if len(output_ids) > 1 else 0
                win2 = st.selectbox(
                    "Window 2 (Comparison)", output_ids, index=default_w2, key="ver_w2"
                )

            result = engine.get_versioning_query(win1, win2, selected_run_id)

            if isinstance(result, str):
                st.error(result)
            else:
                m1, m2, m3 = st.columns(3)
                m1.metric(f"Window 1 ({win1})", result["window_1"]["output_count"])
                m2.metric(
                    f"Window 2 ({win2})",
                    result["window_2"]["output_count"],
                    delta=result["delta_count"],
                )
                m3.metric("Net Change", f"{result['delta_count']:+d}")

                comp_df = result["comparison"]
                left, right = st.columns([1.2, 1])
                with left:
                    if not comp_df.empty:
                        fig = create_version_comparison_chart(comp_df, win1, win2)
                        st.plotly_chart(fig, use_container_width=True)
                with right:
                    st.markdown("**Difference Table**")
                    st.dataframe(comp_df, use_container_width=True, hide_index=True)
        else:
            st.info("Not enough windows to compare.")