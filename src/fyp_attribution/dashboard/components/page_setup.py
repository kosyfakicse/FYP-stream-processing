import os
import streamlit as st
from dotenv import load_dotenv
from src.fyp_attribution.dashboard.components.sidebar import render_sidebar

load_dotenv()

DB_PATH = os.getenv("DUCKDB_PATH", "data/attribution.duckdb")
BACKEND_API_URL = os.getenv(
    "BACKEND_API_URL", "http://localhost:8080/api/pipeline/run"
)


def init_page(title: str, layout: str = "wide") -> None:
    """Configure the Streamlit page and render the shared sidebar.

    Must be called before any other Streamlit command on the page.
    """
    st.set_page_config(page_title=title, layout=layout)
    render_sidebar()