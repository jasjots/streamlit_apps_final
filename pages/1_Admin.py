import streamlit as st
import pandas as pd
from spcs_helpers.connection import session


# Get Snowpark session
session = session()

st.set_page_config(
    page_title="RBAC Admin Page",
    layout="wide",
    initial_sidebar_state="expanded", 
)

def load_css(path):
    with open(path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css("CSS/sidebar.css")
load_css("CSS/admin.css")

from sidebar import render_sidebar
render_sidebar()

spinner_placeholder=st.empty()
with st.spinner ('Loading, please wait...'):
    #st.session_state.spinner_text='Loading dataframe and charts...'
    #spinner_placeholder.markdown ("<p style='color: #5D6A85;'>Loading dataframe and charts...</p>", unsafe_allow_html=True) 
    cl1, cl2, cl3 = st.columns ([4,1,1])
    with cl3:
        st.image("assets/logo_cloudeqs.png", width=200)




with st.container(border=False):
    st.markdown(
        """
        <div>
            <h1 style="font-family: Inter, sans-serif; font-size: 25px; text-align: left;">
                RBC Admin Page 
            </h1>
        </div>
        """, unsafe_allow_html=True
    )


# --- Helper Functions ---

def load_users():
    df = session.table("EDW_LAB_DEV.OBSERVABILITY.RBC_ADMIN_USERS").to_pandas()
    return df

def load_roles():
    session.use_database("EDW_LAB_PROD")
    session.use_schema("STREAMLITAPPS")
    df = session.table("EDW_LAB_PROD.STREAMLITAPPS.GRANTS_TO_USERS_V").select("ROLE").distinct().to_pandas()
    roles = df["ROLE"].dropna().unique().tolist()
    roles.sort()
    return roles

def load_usernames_by_role(role):
    session.use_database("EDW_LAB_PROD")
    session.use_schema("STREAMLITAPPS")
    df = session.table("EDW_LAB_PROD.STREAMLITAPPS.GRANTS_TO_USERS_V").filter(f"ROLE = '{role}'").select("GRANTEE_NAME").distinct().to_pandas()
    usernames = df["GRANTEE_NAME"].dropna().unique().tolist()
    usernames.sort()
    return usernames

def add_user(username, role, access):
    session.sql(
        f"""
        INSERT INTO EDW_LAB_DEV.OBSERVABILITY.RBC_ADMIN_USERS (username, role, access)
        VALUES ('{username}', '{role}', '{access}')
        """
    ).collect()

def update_user(old_username, new_role, new_access):
    session.sql(
        f"""
        UPDATE EDW_LAB_DEV.OBSERVABILITY.RBC_ADMIN_USERS
        SET role = '{new_role}', access = '{new_access}'
        WHERE username = '{old_username}'
        """
    ).collect()

def delete_user(username):
    session.sql(
        f"""
        DELETE FROM EDW_LAB_DEV.OBSERVABILITY.RBC_ADMIN_USERS
        WHERE username = '{username}'
        """
    ).collect()


# --- UI ---



# st.title("🔐 RBC Admin Page")
with st.container(border=True):

    st.markdown(
        """
        <h1 style="font-family: Inter, sans-serif; font-size: 18px; text-align: left;">
            Add New User
        </h1>
        """,
        unsafe_allow_html=True
    )

    roles = load_roles()
    selected_role = st.selectbox("Select a Role", options=roles)

    if selected_role:
        usernames = load_usernames_by_role(selected_role)
        selected_username = st.selectbox("Select a Username", options=usernames)
    else:
        selected_username = None

    access_level = st.radio(
        "Select Access Level",
        options=["Admin", "Viewer"],
        horizontal=True
    )

    if st.button("Add User"):
        if selected_username and selected_role and access_level:
            add_user(selected_username, selected_role, access_level)
            st.success(
                f"✅ User **{selected_username}** added with role **{selected_role}**."
            )
        else:
            st.error("❌ Please select all fields.")


# Show existing users
st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">',unsafe_allow_html=True)
with st.container(border=True):

    st.markdown(
        """
        <h1 style="font-family: Inter, sans-serif; font-size: 18px; text-align: left;">
            Existing Admin Users
        </h1>
        """,
        unsafe_allow_html=True
    )
    

    df_users = load_users()

    if df_users.empty:
        st.info("No admin users found.")
    else:
        for index, row in df_users.iterrows():

            st.markdown('<div class="user-row-card">', unsafe_allow_html=True)

            col1, col2, col3, col4, col5 = st.columns([2.2, 2, 2, 1, 1])

            with col1:
                st.markdown(
                    f"<p class='user-text'>{row['USERNAME']}</p>",
                    unsafe_allow_html=True
                )

            with col2:
                new_role = st.selectbox(
                    f"Role_{index}",
                    roles,
                    index=roles.index(row["ROLE"]) if row["ROLE"] in roles else 0
                )

            with col3:
                new_access = st.selectbox(
                    f"Access_{index}",
                    ["Admin", "Viewer"],
                    index=["Admin", "Viewer"].index(row["ACCESS"])
                )

            with col4:
                if st.button("💾 Save", key=f"save_{row['USERNAME']}"):
                    update_user(row["USERNAME"], new_role, new_access)
                    st.success(f"✅ Updated {row['USERNAME']}")
                    st.rerun()

            with col5:
                if st.button("🗑️ Delete", key=f"delete_{row['USERNAME']}"):
                    delete_user(row["USERNAME"])
                    st.warning(f"🗑️ Deleted {row['USERNAME']}")
                    st.rerun()

            st.markdown('</div>', unsafe_allow_html=True)
