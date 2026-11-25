import streamlit as st
import os
import requests
from dotenv import load_dotenv

load_dotenv()
BACKEND_API_URL = os.getenv("BACKEND_API_URL", "http://localhost:8000")

def inject_css():
    st.markdown("""
        <style>
            [data-testid="stSidebar"] a,
            [data-testid="stSidebar"] a * {
                font-size: 1.05rem !important;
                font-weight: 600 !important;
                color: #ffffff !important;
                margin: 0 !important;
            }
            [data-testid="stSidebar"] a[data-testid="stPageLink"] {
                padding: 10px 14px !important;
                margin-bottom: 6px !important;
                border-radius: 8px !important;
                background-color: rgba(255, 255, 255, 0.05) !important;
                display: block !important;
                text-decoration: none !important;
            }
            [data-testid="stSidebar"] a[data-testid="stPageLink"]:hover {
                background-color: rgba(255, 255, 255, 0.12) !important;
            }
            [data-testid="stSidebar"] h3 {
                font-size: 0.8rem !important;
                font-weight: 700 !important;
                letter-spacing: 0.05em !important;
                color: #94a3b8 !important;
                margin: 1.2rem 0 0.5rem 0 !important;
                text-transform: uppercase !important;
            }
        </style>
    """, unsafe_allow_html=True)


def check_api_connection() -> bool:
    try:
        resp = requests.get(f"{BACKEND_API_URL}/runs", timeout=3)
        return resp.status_code == 200
    except Exception:
        return False


def render_sidebar():
    inject_css()

    with st.sidebar:
        # Section 1: Database Status
        st.markdown("### Navigation")
        st.page_link("pages/1_Overview.py", label="Overview")
        st.page_link("pages/2_Query.py", label="Query")
        st.page_link("pages/3_TIN_Network.py", label="TIN Network")
        st.page_link("pages/4_Insert_Data.py", label="Insert Data")

        st.markdown("---")

        # Section 2: Navigation
        st.markdown("### API Status")
        if check_api_connection():
            st.success("Online")
        else:
            st.error("Offline")

        st.markdown("---")

        # Section 4: About / System Info
        st.markdown("### About / System Info")
        st.caption("TIN Data Provenance System")
        st.caption("Developed by Leo & Vincent")