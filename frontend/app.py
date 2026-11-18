import streamlit as st
from components.page_setup import init_page

init_page("TIN Provenance Dashboard")

st.title("TIN Provenance Dashboard")
st.markdown("Select a page from the sidebar.")

st.info("Go to **Overview** to see the latest attribution results.")