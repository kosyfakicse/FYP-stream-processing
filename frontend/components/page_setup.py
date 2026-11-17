import os
import streamlit as st
from dotenv import load_dotenv
from components.sidebar import render_sidebar


load_dotenv()

BACKEND_API_URL = os.getenv("BACKEND_API_URL", "http://localhost:8000")

def init_page(title: str, layout: str = "wide") -> None:
    st.set_page_config(page_title=title, layout=layout)
    render_sidebar()