import streamlit as st
import os

from loguru import logger
from dotenv import load_dotenv
from src.fyp_attribution.storage.duckdb_adapter import DuckDB

# Load environment variables from .env file
load_dotenv()

# Define the path to the DuckDB database, defaulting to "data/attribution.duckdb" if not set in the environment
DB_PATH = os.getenv("DUCKDB_PATH", "data/attribution.duckdb")


def inject_css():
    st.markdown("""
        <style>
            /* Enlarge all link text in the sidebar */
            [data-testid="stSidebar"] a,
            [data-testid="stSidebar"] a * {
                font-size: 1.2rem !important;
                font-weight: 700 !important; 
                color: #ffffff !important; /* Pure white */
                margin: 0 !important;
            }
            /* Style links as buttons */
            [data-testid="stSidebar"] a[data-testid="stPageLink"] {
                padding: 12px 15px !important;
                margin-bottom: 10px !important;
                border-radius: 8px !important; 
                background-color: rgba(255, 255, 255, 0.05) !important;
                display: block !important;
                text-decoration: none !important;
            }
            /* Hover effect */
            [data-testid="stSidebar"] a[data-testid="stPageLink"]:hover {
                background-color: rgba(255, 255, 255, 0.15) !important;
                transform: translateY(-2px) !important;
            }
        </style>
    """, unsafe_allow_html=True)
    
@st.cache_resource
def check_db_connection() -> bool:
    """Check if the database connection is successful"""
    try:
        db = DuckDB(db_path=DB_PATH, read_only=True)
        # Perform a simple query to check if the database is accessible
        db.execute("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def render_sidebar():
    inject_css()

    # Sidebar content
    with st.sidebar:
        # 1. Add a navigation menu to the sidebar
        st.markdown("### Navigation")
        st.page_link("pages/1_Overview.py", label="Overview")

        st.markdown("---")

        # 2. Add a title and description to the sidebar
        st.markdown("### System Status")
        if check_db_connection():
            st.success("Database: Online")
        else:
            st.error(f"Database: Offline")

        st.markdown("---")

        # 3. Add a section for additional information or links
        st.markdown("### System Info")
        st.markdown("Developed by Leo & Vincent")
