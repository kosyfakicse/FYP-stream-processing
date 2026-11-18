import re
import time
import pandas as pd
import requests
import streamlit as st
from components.page_setup import (
    init_page,
    BACKEND_API_URL,
)
from components.data_utils import (
    validate_schema,
    generate_mock_tweets,
)

init_page("Insert Data")

# Helper Function
def sanitize_csv_filename(raw_name: str) -> str:
    """Ensure every upload has a clean, backend-safe .csv filename."""
    if not raw_name or not raw_name.strip():
        return "streamlit_upload.csv"

    # Remove directory paths if any, strip whitespace
    base = raw_name.strip().split("/")[-1].split("\\")[-1]

    # Remove existing .csv extension (case-insensitive)
    base = re.sub(r"\.csv$", "", base, flags=re.IGNORECASE)

    # Replace spaces and unsafe characters with underscores
    clean_base = re.sub(r"[^a-zA-Z0-9_-]", "_", base).strip("_")
    if not clean_base:
        clean_base = "streamlit_upload"

    return f"{clean_base}.csv"


# ==========================================
# Initialize Session State
# ==========================================
if "prepared_df" not in st.session_state:
    base_ts = int(time.time() * 1000)
    st.session_state.prepared_df = pd.DataFrame({
        "user_id": ["user_1", "user_3", "user_2"],
        "text": ["#UST #DATA", "#AI #DATA", "Who else uses #AI?"],
        "timestamp": [base_ts, base_ts + 1200, base_ts + 2400],
    })

if "dataset_filename" not in st.session_state:
    st.session_state.dataset_filename = "streamlit_upload.csv"

if "latest_run_id" not in st.session_state:
    st.session_state.latest_run_id = None

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
            if not uploaded_file.name.lower().endswith(".csv"):
                st.error("Only .csv files are allowed. Please upload a valid CSV file.")
                if "prepared_df" in st.session_state:
                    del st.session_state.prepared_df
                st.stop()
            else:
                try:
                    df_uploaded = pd.read_csv(uploaded_file)
                    is_valid, msg = validate_schema(df_uploaded)

                    if is_valid:
                        clean_name = sanitize_csv_filename(uploaded_file.name)
                        st.session_state.prepared_df = df_uploaded
                        st.session_state.dataset_filename = clean_name
                        st.session_state.is_file_valid = True
                        st.success(f"{msg} (Dataset name: `{clean_name}`)")

                    else:
                        st.error(msg)
                        if "prepared_df" in st.session_state:
                            del st.session_state.prepared_df
                        st.stop()

                except Exception as e:
                    st.error(f"Failed to read CSV file: {e}")
                    if "prepared_df" in st.session_state:
                        del st.session_state.prepared_df
                    st.stop()

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
            st.session_state.dataset_filename = sanitize_csv_filename(
                f"generated_{num_rows}_rows.csv"
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
            st.session_state.dataset_filename = sanitize_csv_filename(
                st.session_state.dataset_filename
            )
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
# Bottom Section: Pipeline Execution
# ==========================================
st.subheader("3. Pipeline Execution")

safe_filename = sanitize_csv_filename(st.session_state.dataset_filename)
st.caption(f"Target upload filename: `{safe_filename}`")

if st.button("Send & Run Pipeline", type="primary"):
    is_valid, msg = validate_schema(df_current)
    if not is_valid:
        st.error(f"Validation Failed: {msg}")
    else:
        try:
            with st.spinner(f"Sending '{safe_filename}' to Java backend..."):
                # 1. Convert DataFrame to raw CSV bytes
                csv_bytes = df_current.to_csv(index=False).encode("utf-8")

                # 2. POST /datasets with raw CSV bytes and Content-Type: text/csv
                upload_resp = requests.post(
                    f"{BACKEND_API_URL}/datasets",
                    params={"name": safe_filename},
                    data=csv_bytes,
                    headers={"Content-Type": "text/csv"},
                    timeout=30,
                )
                upload_resp.raise_for_status()
                dataset_id = upload_resp.json()["datasetId"]

                # 3. POST /runs with the datasetId
                run_resp = requests.post(
                    f"{BACKEND_API_URL}/runs",
                    json={"datasetId": dataset_id},
                    timeout=120,
                )
                run_resp.raise_for_status()
                run_id = run_resp.json()["runId"]

                st.session_state.latest_run_id = run_id
                st.success(f"Pipeline finished. Run ID: {run_id}")

        except requests.exceptions.HTTPError as e:
            detail = e.response.text if e.response is not None else str(e)
            st.error(f"Backend API Error ({e.response.status_code}): {detail}")
        except Exception as e:
            st.error(f"Backend API Error: {e}")

# Show latest run id and navigation
if st.session_state.get("latest_run_id"):
    st.info(f"Latest Run ID: {st.session_state.latest_run_id}")
    if st.button("Go to Query Page"):
        st.switch_page("pages/2_Query.py")
