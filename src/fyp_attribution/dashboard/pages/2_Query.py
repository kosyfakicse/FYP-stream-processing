from datetime import datetime
import pandas as pd
import streamlit as st
from src.fyp_attribution.dashboard.components.charts import (
    create_attribution_donut_chart,
    create_backward_bar_chart,
    create_flow_sankey_diagram,
    create_temporal_trend_chart,
    create_version_comparison_chart,
)
from src.fyp_attribution.dashboard.components.page_setup import init_page
from src.fyp_attribution.dashboard.components.db_utils import (
    get_db,  # noqa: F401  (kept for parity / future use)
    load_run_data,
    get_available_run_ids,
)
from src.fyp_attribution.dashboard.components.parsers import parse_attribution_json

init_page("Query & Lineage")


# ==========================================
# Page Header & Global Context Bar
# ==========================================
st.title("Query & Lineage Explorer")
st.markdown(
    "Trace provenance, contributor weights, temporal trends, and flow lineage"
    " across attribution windows."
)

mock_store = st.session_state.get("mock_results", {})
available_runs = get_available_run_ids(mock_store)
latest_run = st.session_state.get("latest_run_id", None)

default_idx = 0
if latest_run and latest_run in available_runs:
    default_idx = available_runs.index(latest_run)

ctx_col1, ctx_col2 = st.columns([3, 1])
with ctx_col1:
    selected_run_id = st.selectbox(
        "Active Run ID",
        options=available_runs,
        index=default_idx,
        help="Select a pipeline run ID generated from Insert Data or stored in DuckDB.",
    )

window_df, flow_df, data_source = load_run_data(selected_run_id, mock_store)

with ctx_col2:
    st.metric("Data Source", data_source)

expected_window_cols = [
    "output_id",
    "category",
    "output_count",
    "attribution_json",
    "window_start",
]
if window_df.empty:
    window_df = pd.DataFrame(columns=expected_window_cols)

st.markdown("---")

# ==========================================
# Main Navigation Tabs
# ==========================================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Backward Query",
    "Forward Query",
    "Temporal Lineage",
    "Flow Lineage",
    "Versioning Query",
])

# ==========================================
# Tab 1: Backward Query
# ==========================================
with tab1:
    st.subheader("Backward Provenance Query")
    st.caption(
        "Trace window outputs backward to their contributing users, attribution"
        " weights, and source tweets across time."
    )

    available_tags = (
        window_df["category"].unique().tolist() if not window_df.empty else []
    )

    if available_tags:
        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            selected_tag_bw = st.selectbox(
                "Filter by Hashtag", options=available_tags, key="bw_tag_filter"
            )

        tag_windows_df = window_df[window_df["category"] == selected_tag_bw]
        output_options = tag_windows_df["output_id"].astype(str).tolist()

        with filter_col2:
            selected_output_id = st.selectbox(
                "Select Specific Output ID (Window)",
                options=output_options,
                key="bw_select_output",
            )

        st.markdown("---")

        st.markdown(f"**All Historical Windows for {selected_tag_bw}**")
        summary_cols = [
            c
            for c in [
                "output_id",
                "output_count",
                "window_start",
                "attribution_json",
            ]
            if c in tag_windows_df.columns
        ]
        st.dataframe(
            tag_windows_df[summary_cols], use_container_width=True, hide_index=True
        )

        st.markdown("---")

        if selected_output_id:
            matched = tag_windows_df[
                tag_windows_df["output_id"].astype(str) == str(selected_output_id)
            ]
            if not matched.empty:
                record = matched.iloc[0]
                total_cnt = int(record.get("output_count", 0))
                win_start = str(record.get("window_start", "N/A"))

                st.markdown(
                    f"**Detailed Breakdown for Output ID: {selected_output_id}**"
                )
                m1, m2 = st.columns(2)
                m1.metric("Total Count in Window", total_cnt)
                m2.metric("Window Start (ms)", win_start)

                attr_df = parse_attribution_json(
                    record.get("attribution_json", "{}")
                )

                left_col, right_col = st.columns([1.3, 1])

                with left_col:
                    if not attr_df.empty:
                        fig_bar = create_backward_bar_chart(
                            attr_df=attr_df,
                            output_id=selected_output_id,
                        )
                        st.plotly_chart(fig_bar, use_container_width=True)

                        fig_donut = create_attribution_donut_chart(
                            attr_df=attr_df,
                            title="Attribution Weight Share",
                        )
                        st.plotly_chart(fig_donut, use_container_width=True)
                    else:
                        st.info("No contributor breakdown available.")

                with right_col:
                    st.markdown("**Contributor Attribution Table**")
                    if not attr_df.empty:
                        attr_display = attr_df.copy()
                        attr_display["attributed_count"] = (
                            attr_display["weight"] * total_cnt
                        ).round(2)
                        st.dataframe(
                            attr_display, use_container_width=True, hide_index=True
                        )

                    source_events_map = mock_store.get(selected_run_id, {}).get(
                        "source_events", {}
                    )
                    if selected_output_id in source_events_map:
                        st.markdown("**Contributing Source Tweets**")
                        st.dataframe(
                            source_events_map[selected_output_id],
                            use_container_width=True,
                            hide_index=True,
                        )
    else:
        st.info("No output records available. Please run pipeline in Insert Data.")

# ==========================================
# Tab 2: Forward Query
# ==========================================
with tab2:
    st.subheader("Forward Impact Query")
    st.caption(
        "Trace a source user ID forward to see all window outputs influenced by"
        " their activity."
    )

    all_contributors = set()
    if not window_df.empty and "attribution_json" in window_df.columns:
        for raw_json in window_df["attribution_json"].dropna():
            parsed = parse_attribution_json(raw_json)
            all_contributors.update(parsed["contributor"].tolist())

    contributor_list = sorted(list(all_contributors))

    fw_col1, fw_col2 = st.columns([3, 1])
    with fw_col1:
        if contributor_list:
            source_id = st.selectbox(
                "Select Source User ID", options=contributor_list, key="fw_select"
            )
        else:
            source_id = st.text_input(
                "Enter Source User ID", value="user_1", key="fw_text"
            )
    with fw_col2:
        st.write("")
        fw_search = st.button(
            "Run Forward Query",
            type="primary",
            use_container_width=True,
            key="fw_btn",
        )

    if source_id and not window_df.empty:
        impacted_rows = []
        for _, row in window_df.iterrows():
            parsed = parse_attribution_json(row.get("attribution_json", "{}"))
            user_match = parsed[parsed["contributor"] == source_id]
            if not user_match.empty:
                weight = float(user_match.iloc[0]["weight"])
                total_cnt = int(row.get("output_count", 0))
                impacted_rows.append({
                    "Output ID": row.get("output_id"),
                    "Hashtag": row.get("category"),
                    "Total Count": total_cnt,
                    "User Weight": weight,
                    "Attributed Count": round(total_cnt * weight, 2),
                    "Time Window": row.get("window_start"),
                    "Attribution JSON": row.get("attribution_json"),
                })

        impacted_df = pd.DataFrame(impacted_rows)
        if not impacted_df.empty:
            st.success(
                f"Found {len(impacted_df)} window outputs influenced by {source_id}."
            )
            fm1, fm2 = st.columns(2)
            fm1.metric("Impacted Windows", len(impacted_df))
            fm2.metric(
                "Total Attributed Volume",
                round(impacted_df["Attributed Count"].sum(), 2),
            )
            st.dataframe(impacted_df, use_container_width=True, hide_index=True)
        else:
            st.info(f"No downstream window outputs attributed to {source_id}.")
    else:
        st.info("Select or enter a Source User ID to trace downstream outputs.")

# ==========================================
# Tab 3: Temporal Lineage Query
# ==========================================
with tab3:
    st.subheader("Temporal Lineage Query")
    st.caption(
        "Analyze how a specific hashtag's volume evolves across sequential time"
        " windows."
    )

    hashtags = (
        sorted(window_df["category"].dropna().unique().tolist())
        if not window_df.empty
        else []
    )
    if hashtags:
        selected_tag = st.selectbox(
            "Select a Hashtag", options=hashtags, key="temp_tag"
        )
        tag_df = window_df[window_df["category"] == selected_tag].sort_values(
            by="window_start"
        )

        tag_df = tag_df.copy()
        tag_df["window_label"] = tag_df["window_start"].apply(
            lambda ts: datetime.fromtimestamp(float(ts) / 1000.0).strftime(
                "%H:%M:%S"
            )
            if pd.notnull(ts) and float(ts) > 1e10
            else str(ts)
        )

        t_left, t_right = st.columns([1.3, 1])
        with t_left:
            fig_temp = create_temporal_trend_chart(
                tag_df=tag_df,
                selected_tag=selected_tag,
            )
            st.plotly_chart(fig_temp, use_container_width=True)

        with t_right:
            st.markdown("**Window Breakdown**")
            display_cols = [
                c
                for c in [
                    "output_id",
                    "window_label",
                    "output_count",
                    "attribution_json",
                ]
                if c in tag_df.columns
            ]
            st.dataframe(
                tag_df[display_cols], use_container_width=True, hide_index=True
            )
    else:
        st.info("No hashtags available in the selected run.")

# ==========================================
# Tab 4: Flow Lineage Query
# ==========================================
with tab4:
    st.subheader("Flow Lineage Query")
    st.caption(
        "Visualize provenance flow from source contributors to target hashtags."
    )

    if flow_df.empty and not window_df.empty:
        derived_flows = []
        for _, row in window_df.iterrows():
            tag = row.get("category")
            cnt = int(row.get("output_count", 0))
            parsed = parse_attribution_json(row.get("attribution_json", "{}"))
            for _, p_row in parsed.iterrows():
                derived_flows.append({
                    "source_vertex": p_row["contributor"],
                    "target_vertex": tag,
                    "total_quantity": round(cnt * float(p_row["weight"]), 2),
                })
        if derived_flows:
            flow_df = (
                pd.DataFrame(derived_flows)
                .groupby(["source_vertex", "target_vertex"], as_index=False)[
                    "total_quantity"
                ]
                .sum()
            )

    if not flow_df.empty:
        max_qty = float(flow_df["total_quantity"].max())
        min_filter = st.slider(
            "Minimum Flow Quantity Filter",
            min_value=0.0,
            max_value=max(1.0, max_qty),
            value=0.0,
            step=0.5,
        )
        filtered_flow = flow_df[flow_df["total_quantity"] >= min_filter]

        f_left, f_right = st.columns([1.3, 1])
        with f_left:
            if not filtered_flow.empty:
                fig_sankey = create_flow_sankey_diagram(
                    flow_df=filtered_flow,
                    run_id=selected_run_id,
                )
                st.plotly_chart(fig_sankey, use_container_width=True)
            else:
                st.info("No flow edges match the current filter threshold.")

        with f_right:
            st.markdown("**Flow Lineage Table**")
            display_flow_cols = [
                c
                for c in ["source_vertex", "target_vertex", "total_quantity"]
                if c in filtered_flow.columns
            ]
            st.dataframe(
                filtered_flow[display_flow_cols],
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("No flow lineage records available for the selected Run ID.")

# ==========================================
# Tab 5: Versioning Query
# ==========================================
with tab5:
    st.subheader("Versioning & Window Comparison Query")
    st.caption(
        "Compare contributor attribution and total volume between two different"
        " windows for the same hashtag."
    )

    v_hashtags = (
        sorted(window_df["category"].dropna().unique().tolist())
        if not window_df.empty
        else []
    )
    if v_hashtags:
        v_c1, v_c2, v_c3 = st.columns(3)
        with v_c1:
            ver_tag = st.selectbox("Select Hashtag", options=v_hashtags, key="ver_tag")

        ver_df = window_df[window_df["category"] == ver_tag].sort_values(
            by="window_start"
        )
        win_options = ver_df["output_id"].astype(str).tolist()

        if len(win_options) >= 1:
            with v_c2:
                win1_id = st.selectbox(
                    "Select Window 1 (Baseline)",
                    options=win_options,
                    index=0,
                    key="ver_w1",
                )
            with v_c3:
                default_w2 = 1 if len(win_options) > 1 else 0
                win2_id = st.selectbox(
                    "Select Window 2 (Comparison)",
                    options=win_options,
                    index=default_w2,
                    key="ver_w2",
                )

            row1 = ver_df[ver_df["output_id"].astype(str) == win1_id].iloc[0]
            row2 = ver_df[ver_df["output_id"].astype(str) == win2_id].iloc[0]

            cnt1 = int(row1.get("output_count", 0))
            cnt2 = int(row2.get("output_count", 0))
            delta_cnt = cnt2 - cnt1

            vm1, vm2, vm3 = st.columns(3)
            vm1.metric(f"Window 1 ({win1_id})", cnt1)
            vm2.metric(f"Window 2 ({win2_id})", cnt2, delta=delta_cnt)
            vm3.metric("Net Change", f"{delta_cnt:+d}")

            st.markdown("---")

            attr1 = parse_attribution_json(
                row1.get("attribution_json", "{}")
            ).rename(columns={"weight": "Window 1 Weight"})
            attr2 = parse_attribution_json(
                row2.get("attribution_json", "{}")
            ).rename(columns={"weight": "Window 2 Weight"})

            comp_df = pd.merge(attr1, attr2, on="contributor", how="outer").fillna(0.0)
            comp_df["Weight Diff"] = (
                comp_df["Window 2 Weight"] - comp_df["Window 1 Weight"]
            ).round(4)

            v_left, v_right = st.columns([1.2, 1])
            with v_left:
                if not comp_df.empty:
                    fig_ver = create_version_comparison_chart(
                        comp_df=comp_df,
                        win1_id=win1_id,
                        win2_id=win2_id,
                    )
                    st.plotly_chart(fig_ver, use_container_width=True)
                else:
                    st.info("No contributor weights to compare.")

            with v_right:
                st.markdown("**Difference Table**")
                st.dataframe(comp_df, use_container_width=True, hide_index=True)
        else:
            st.info("No windows available for comparison.")
    else:
        st.info("No hashtags available for versioning comparison.")