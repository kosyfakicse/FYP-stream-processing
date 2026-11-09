import streamlit as st
from src.fyp_attribution.dashboard.components.sidebar import render_sidebar

# Set page configuration
st.set_page_config(page_title="FYP Provenance", layout="wide")

# Call the function to render the sidebar
render_sidebar()

st.title("TIN Data Provenance System")
st.markdown("""
Welcome to the **How-Much Provenance** visualization dashboard.

Use the sidebar on the left to navigate to **Backward Attribution**.
""")
