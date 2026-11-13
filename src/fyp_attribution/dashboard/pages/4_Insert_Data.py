from datetime import datetime
import json
import time
import uuid
import pandas as pd
import requests
import streamlit as st
from src.fyp_attribution.dashboard.components.page_setup import (
    init_page,
    BACKEND_API_URL,
)
from src.fyp_attribution.dashboard.components.data_utils import (
    validate_schema,
    generate_mock_tweets,
)

init_page("Insert Data")

# Initialize session state to persist dataset and run_id across pages
if "prepared_df" not in st.session_state:
    base_ts = int(time.time() * 1000)
    st.session_state.prepared_df = pd.DataFrame({
        "user_id": ["user_1", "user_3", "user_2"],
        "text": ["#UST #DATA", "#AI #DATA", "Who else uses #AI?"],
        "timestamp": [base_ts, base_ts + 1200, base_ts + 2400],
    })

if "latest_run_id" not in st.session_state:
    st.session_state.latest_run_id = None

if "pipeline_status" not in st.session_state:
    st.session_state.pipeline_status = None


# ==========================================
# Page Header
# ==========================================
st.title("Insert Data")
st.markdown(
    "Prepare tweet stream datasets and trigger the Flink / Java temporal"
    " attribution pipeline."
)

# ==========================================
# Top Section: Dataset Preparation
# ==========================================
st.subheader("1. Dataset Preparation")
tab1, tab2, tab3 = st.tabs([
    "Upload File",
    "System Generate with Parameters",
    "Full Custom Editor",
])

# Tab 1: Upload File
with tab1:
    col_dl, col_up = st.columns([1, 3])
    with col_dl:
        sample_csv = (
            "user_id,text,timestamp\n"
            "user_1,#UST #DATA,1793844000000\n"
            "user_3,#AI #DATA,1793844001200\n"
            "user_2,Who else uses #AI?,1793844002400\n"
        )
        st.download_button(
            label="Download Sample CSV",
            data=sample_csv,
            file_name="tweet_hashtag_sample.csv",
            mime="text/csv",
        )
    with col_up:
        uploaded_file = st.file_uploader(
            "Upload CSV file (Required columns: user_id, text, timestamp)",
            type=["csv"],
        )
        if uploaded_file is not None:
            try:
                df_uploaded = pd.read_csv(uploaded_file)
                is_valid, msg = validate_schema(df_uploaded)
                if is_valid:
                    st.success(msg)
                    st.session_state.prepared_df = df_uploaded
                else:
                    st.error(msg)
            except Exception as e:
                st.error(f"Failed to read CSV file: {e}")

# Tab 2: System Generate with Parameters
with tab2:
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        num_rows = st.number_input(
            "Number of Rows", min_value=5, max_value=10000, value=50, step=10
        )
    with c2:
        num_users = st.number_input(
            "Number of Users", min_value=1, max_value=500, value=3, step=1
        )
    with c3:
        raw_tags = st.text_input(
            "Target Hashtags (comma-separated)", value="#UST, #AI, #DATA"
        )
    with c4:
        interval_ms = st.number_input(
            "Time Interval (ms)", min_value=100, max_value=60000, value=1200, step=100
        )

    if st.button("Generate Dataset"):
        tag_list = [t.strip() for t in raw_tags.split(",") if t.strip()]
        if not tag_list:
            st.warning("Please enter at least one target hashtag.")
        else:
            st.session_state.prepared_df = generate_mock_tweets(
                num_rows, num_users, tag_list, interval_ms
            )
            st.success(f"Successfully generated {num_rows} rows.")

# Tab 3: Full Custom Editor
with tab3:
    st.caption(
        "Edit cell values directly, click '+' at the bottom to add rows, or"
        " select rows on the left to delete:"
    )
    edited_df = st.data_editor(
        st.session_state.prepared_df,
        num_rows="dynamic",
        use_container_width=True,
        key="custom_data_editor",
    )
    if st.button("Apply Changes"):
        is_valid, msg = validate_schema(edited_df)
        if is_valid:
            st.session_state.prepared_df = edited_df
            st.success("Dataset updated successfully.")
        else:
            st.error(msg)

st.markdown("---")

# ==========================================
# Middle Section: Dataset Preview & Validation
# ==========================================
st.subheader("2. Dataset Preview & Validation")
df_current = st.session_state.prepared_df

total_records = len(df_current)
unique_users = df_current["user_id"].nunique() if total_records > 0 else 0

if total_records > 1:
    span_ms = int(df_current["timestamp"].max() - df_current["timestamp"].min())
    time_span_str = f"{span_ms / 1000:.1f}s ({span_ms} ms)"
else:
    time_span_str = "0.0s"

col1, col2, col3 = st.columns(3)
col1.metric("Total Records", total_records)
col2.metric("Unique Users", unique_users)
col3.metric("Time Span", time_span_str)

st.dataframe(df_current, use_container_width=True, height=250)

st.markdown("---")

# ==========================================
# Bottom Section: Pipeline Execution & Run ID Tracking
# ==========================================
st.subheader("3. Pipeline Execution & Run ID Tracking")

col_mode, col_url, col_btn = st.columns([2, 2, 1.5])
with col_mode:
    run_mode = st.radio(
        "Run Mode",
        options=["Mock Mode", "Live Backend"],
        horizontal=True,
        help="Switch between local frontend simulation and live Java backend API.",
    )
with col_url:
    api_endpoint = st.text_input(
        "Backend Endpoint",
        value=BACKEND_API_URL,
        disabled=(run_mode == "Mock Mode"),
    )
with col_btn:
    st.write("")
    send_clicked = st.button(
        "Send & Run Pipeline", type="primary", use_container_width=True
    )

if send_clicked:
    is_valid, msg = validate_schema(df_current)
    if not is_valid:
        st.error(f"Validation Failed: {msg}")
    else:
        if run_mode == "Mock Mode":
            with st.status("Executing Mock Pipeline...", expanded=True) as status:
                st.write("Status: Submitted - Packaging dataset payload...")
                time.sleep(0.5)
                st.write("Status: Processing - Running temporal attribution batch...")
                time.sleep(0.7)

                mock_run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:4]}"
                st.session_state.latest_run_id = mock_run_id
                st.session_state.pipeline_status = "Completed"

                if "mock_results" not in st.session_state:
                    st.session_state.mock_results = {}

                # Extract hashtags and calculate multi-window mock attribution results
                exploded = df_current.assign(
                    hashtag=df_current["text"].str.findall(r"#\w+")
                ).explode("hashtag")
                exploded = exploded.dropna(subset=["hashtag"])

                mock_window_rows = []
                mock_flow_rows = []
                mock_source_events = {}

                mid_ts = (
                    int(df_current["timestamp"].median())
                    if not df_current.empty
                    else 0
                )

                for idx, (tag, group) in enumerate(
                    exploded.groupby("hashtag"), start=1
                ):
                    for w_idx, sub_group in enumerate(
                        [
                            group[group["timestamp"] <= mid_ts],
                            group[group["timestamp"] > mid_ts],
                        ],
                        start=1,
                    ):
                        if sub_group.empty:
                            continue
                        out_id = f"out_{mock_run_id[-4:]}_{idx}_w{w_idx}"
                        total_cnt = len(sub_group)
                        user_counts = sub_group["user_id"].value_counts(
                            normalize=True
                        )
                        attr_dict = {
                            u: round(float(w), 3) for u, w in user_counts.items()
                        }

                        win_start = int(sub_group["timestamp"].min())
                        mock_window_rows.append({
                            "output_id": out_id,
                            "run_id": mock_run_id,
                            "category": tag,
                            "output_count": total_cnt,
                            "attribution_json": json.dumps(attr_dict),
                            "window_start": win_start,
                        })

                        mock_source_events[out_id] = sub_group[
                            ["user_id", "text", "timestamp"]
                        ].copy()

                        for u, cnt in sub_group["user_id"].value_counts().items():
                            mock_flow_rows.append({
                                "run_id": mock_run_id,
                                "source_vertex": u,
                                "target_vertex": tag,
                                "total_quantity": int(cnt),
                            })

                st.session_state.mock_results[mock_run_id] = {
                    "window_outputs": pd.DataFrame(mock_window_rows),
                    "flow_lineage": pd.DataFrame(mock_flow_rows),
                    "source_events": mock_source_events,
                }

                status.update(
                    label="Status: Completed", state="complete", expanded=False
                )
        else:
            try:
                with st.spinner("Sending dataset to Java backend..."):
                    payload = df_current.to_dict(orient="records")
                    response = requests.post(
                        api_endpoint, json=payload, timeout=15
                    )
                    response.raise_for_status()
                    res_json = response.json()
                    st.session_state.latest_run_id = res_json.get(
                        "run_id", "unknown_run_id"
                    )
                    st.session_state.pipeline_status = res_json.get(
                        "status", "Submitted"
                    )
            except Exception as e:
                st.error(f"Backend API Error: {e}")

# Execution Result Display
if st.session_state.latest_run_id:
    st.success(
        "Pipeline Triggered Successfully. Assigned Run ID:"
        f" {st.session_state.latest_run_id}"
    )

    res_col1, res_col2 = st.columns([3, 1])
    with res_col1:
        st.info(
            f"Pipeline Status: {st.session_state.pipeline_status} - Use this Run ID"
            " in the Query page to filter attribution results."
        )
    with res_col2:
        if st.button("Go to Query Page", use_container_width=True):
            st.switch_page("pages/2_Query.py")