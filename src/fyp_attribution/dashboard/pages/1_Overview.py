import streamlit as st
from src.fyp_attribution.dashboard.components.page_setup import init_page
from src.fyp_attribution.dashboard.components.db_utils import get_db
from src.fyp_attribution.dashboard.components.charts import (
    create_trending_hashtags_chart,
)

init_page("System Overview")

# Display the system overview
st.title("System Overview")
st.markdown("TIN provenance system.")

try:
    db = get_db()
    # Get the total number of records and unique categories from the database
    total_records = db.execute("SELECT COUNT(*) FROM window_outputs").fetchone()[0]
    unique_categories = db.execute(
        "SELECT COUNT(DISTINCT category) FROM window_outputs"
    ).fetchone()[0]

    # Display the metrics in three columns
    col1, col2, col3 = st.columns(3)
    col1.metric(
        "Total Window Outputs",
        total_records,
        help="Total number of aggregation results stored.",
    )
    col2.metric(
        "Unique Hashtags",
        unique_categories,
        help="Number of distinct categories/hashtags tracked.",
    )
    col3.metric("Database Status", "Online", "Healthy")

    st.markdown("---")

    # Display the top 5 trending hashtags based on their frequency
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

    # Display the latest 5 window outputs in a table
    st.subheader("Latest 5 Records")
    latest_df = db.execute("""
        SELECT output_id, category, output_count, attribution_json, window_start 
        FROM window_outputs 
        ORDER BY window_start DESC 
        LIMIT 5
    """).fetchdf()

    if not latest_df.empty:
        st.dataframe(latest_df, use_container_width=True)
    else:
        st.info("No recent records found.")

except Exception as e:
    st.error(
        f"Could not load overview data. Database might be empty or locked. Error: {e}"
    )