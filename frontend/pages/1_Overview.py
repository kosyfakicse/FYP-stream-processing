import streamlit as st
from components.page_setup import init_page
from components.api_client import get_runs, get_run, windows_to_df
from components.charts import create_trending_hashtags_chart

init_page("System Overview")
st.title("System Overview")
st.markdown("TIN provenance system — high-level view of attribution results.")

try:
    runs = get_runs()
    if not runs:
        st.info("No runs yet. Upload a dataset in Insert Data.")
    else:
        latest = next((r for r in runs if r.get("finishedAt")), runs[0])
        run_data = get_run(latest["runId"])
        df = windows_to_df(run_data)

        if df.empty:
            st.info("Run has no window outputs yet.")
        else:
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Window Outputs", len(df))
            col2.metric("Most Hashtag", df.loc[df["output_count"].idxmax(), "category"])
            col3.metric("Unique Hashtags", df["category"].nunique())

            st.subheader("Top 5 Trending Hashtags")
            top = (
                df.groupby("category")["output_count"]
                .sum()
                .reset_index()
                .rename(columns={"output_count": "total_heat"})
                .sort_values("total_heat", ascending=False)
                .head(5)
            )
            fig = create_trending_hashtags_chart(top)
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Latest 5 Processed Windows")
            st.dataframe(df.head(5), use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Could not reach the API. Is the backend running?\n\n{e}")