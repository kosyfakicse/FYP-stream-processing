import streamlit as st
from src.fyp_attribution.dashboard.components.page_setup import init_page
from src.fyp_attribution.dashboard.components.db_utils import get_db
from src.fyp_attribution.dashboard.components.charts import (
    create_trending_hashtags_chart,
)
from pathlib import Path

# Only clear mock store once when the server starts
if "mock_cleared" not in st.session_state:
    mock_file = Path("data/mock_results.pkl")
    if mock_file.exists():
        mock_file.unlink()
    st.session_state.mock_cleared = True

init_page("System Overview")

st.title("System Overview")
st.markdown("TIN provenance system — high-level view of attribution results.")

try:
    db = get_db()

    total_records = db.execute("SELECT COUNT(*) FROM window_outputs").fetchone()[0]
    unique_categories = db.execute(
        "SELECT COUNT(DISTINCT category) FROM window_outputs"
    ).fetchone()[0]

    # Most frequent hashtag
    top_tag_row = db.execute("""
        SELECT category, SUM(output_count) AS total
        FROM window_outputs
        GROUP BY category
        ORDER BY total DESC
        LIMIT 1
    """).fetchone()
    most_hashtag = top_tag_row[0] if top_tag_row else "N/A"

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Window Outputs", total_records)
    col2.metric("Most Hashtag", most_hashtag)
    col3.metric("Unique Hashtags", unique_categories)

    st.markdown("---")

    # Top 5 Trending Hashtags
    st.subheader("Top 5 Trending Hashtags")
    top_df = db.execute("""
        SELECT category, SUM(output_count) AS total_heat
        FROM window_outputs
        GROUP BY category
        ORDER BY total_heat DESC
        LIMIT 5
    """).fetchdf()

    if not top_df.empty:
        fig = create_trending_hashtags_chart(top_df)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available yet. Run the Flink pipeline to generate results!")

    st.markdown("---")

    # Latest 5 Processed Windows
    st.subheader("Latest 5 Processed Windows")
    latest_df = db.execute("""
        SELECT
            category AS Hashtag,
            output_count AS Count,
            attribution_json AS Attribution,
            window_start AS "Time Window"
        FROM window_outputs
        ORDER BY window_start DESC
        LIMIT 5
    """).fetchdf()

    if not latest_df.empty:
        st.dataframe(latest_df, use_container_width=True, hide_index=True)
    else:
        st.info("No recent records found.")

except Exception as e:
    st.error(
        f"Could not load overview data. Database might be empty or locked.\n\nError: {e}"
    )