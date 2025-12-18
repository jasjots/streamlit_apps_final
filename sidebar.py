import streamlit as st
from pathlib import Path
import base64

ICON_PATH = Path("assets/icons")

def load_icon(path):
    return base64.b64encode(Path(path).read_bytes()).decode()

def nav_button(icon, label, key, page):
    clicked = st.button("", key=key)
    if clicked:
        st.switch_page(page)

    icon_base64 = load_icon(ICON_PATH / icon)

    st.markdown(
        f"""
        <div class="icon-wrapper">
            <img src="data:image/svg+xml;base64,{icon_base64}" class="sidebar-icon-img"/>
        </div>
        <div class="sidebar-label">{label}</div>
        """,
        unsafe_allow_html=True
    )

def render_sidebar():
    with st.sidebar:
        nav_button("home.svg", "Home", "home", "3_Home.py")
        nav_button("admin.svg", "Admin", "admin", "pages/1_Admin.py")
        nav_button("settings.svg", "Adhoc Run", "adhoc", "pages/2_Adhoc_Run.py")
        nav_button("error.svg", "Error Details", "error", "pages/4_Error_Details_Weekly.py")
        nav_button("history.svg", "Job History", "history", "pages/5_Job_History.py")
        nav_button("calendar.svg", "Monthly Job Details", "monthly", "pages/6_Monthly_Job_Details.py")
        nav_button("settings.svg", "OP Status", "op", "pages/7_OP_Status_Update.py")
