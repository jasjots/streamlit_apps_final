import streamlit as st
import os
from spcs_helpers.connection import session
import logging
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta, timezone
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import plotly.express as px
import io
import time
import requests
import json
import pytz

from pathlib import Path
import sys
from streamlit.web.server.websocket_headers import _get_websocket_headers
from constants import CONSTANTS

st.set_page_config(
    page_title="Ad-Hoc DBT Job Runner", 
    page_icon="", 
    layout="wide",
    initial_sidebar_state="expanded"
)

snowflake_session = session()

def load_css(path):
    with open(path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css("CSS/sidebar.css")
load_css("CSS/admin.css")

from sidebar import render_sidebar
render_sidebar()

# spinner_placeholder=st.empty()
# with st.spinner ('Loading, please wait...'):
#     #st.session_state.spinner_text='Loading dataframe and charts...'
#     #spinner_placeholder.markdown ("<p style='color: #5D6A85;'>Loading dataframe and charts...</p>", unsafe_allow_html=True) 
#     cl1, cl2, cl3 = st.columns ([4,1,1])
#     with cl3:
#         st.image("assets/logo_cloudeqs.png", width=200)

tab_dbt, tab_matilion = st.tabs(["ADHOC RUNS DBT","ADHOC RUNS MATILION"])


# with st.container(border=False):
#     st.markdown(
#         """
#         <div>
#             <h1 style="font-family: Inter, sans-serif; font-size: 25px; text-align: left;">
#                 RBC Admin Page 
#             </h1>
#         </div>
#         """, unsafe_allow_html=True
#     )
# Set page configuration for Streamlit
# st.set_page_config(
#     page_title="Adhoc Runs DBT",
#     layout="wide",
#     initial_sidebar_state="expanded",
# )

with tab_dbt :
    procedure_db, procedure_schema = "EDW_LAB_DEV", "OBSERVABILITY"

    # def connect_to_snowflake():
    #     """Connect to Snowflake using the spcs_helpers.session() method."""
    #     try:
    #         return spcs_helpers.session()  # Ensure this is defined properly in spcs_helpers
    #     except Exception as e:
    #         st.error(f"Failed to connect to Snowflake: {e}")
    #         logging.error(f"Failed to connect to Snowflake: {e}")
    #         return None

    # # Initialize session with Snowflake
    # snowflake_session = connect_to_snowflake()

    email_id = st.context.headers.get("Sf-Context-Current-User") or "Visitor"
    user_token = st.context.headers.get("Sf-Context-Current-User-Token") or ""
    # st.sidebar.header(f"Hi! {email_id}")
    # st.sidebar.text_area("User Token", user_token, height=100)
    session = session(user_token)
    #email_id = session.sql("SELECT CURRENT_USER();").to_pandas().iloc[0,0]
    # st.sidebar.text(f"What is on you mind today {email_id}?")
    REQUIRED_ROLE = session.sql("SELECT CURRENT_ROLE();").to_pandas().iloc[0,0]
    # st.sidebar.text(f"Role:{REQUIRED_ROLE}")
 

    # try:
    #     if os.path.isfile(".snowflake/session/token"):
    #         email_id = st._context.headers.get("sf-Context-Current-User-Email", "").lower()
    #        # st.write("if",email_id)
    #     else:
    #         email_id = snowflake_session.get_current_user().replace('"', "").lower()
    #         #st.write("else",email_id)
    # except Exception as e:
    #     st.error(f"Unable to retrieve user email: {e}")
    #     st.stop()


    def get_user_role(email_id,required_role):
        """Fetch the user's role from the Snowflake RBAC table based on their email."""

        query = f"""
        SELECT ACCESS
        FROM EDW_LAB_DEV.OBSERVABILITY.rbc_admin_users
        WHERE LOWER(USERNAME) = LOWER('{email_id}')
        AND ROLE = '{required_role}';
        """
        try:
            # Execute query
            result = snowflake_session.sql(query).to_pandas()

            # Log query result for debugging
            logging.info(f"Query executed: {query}")
            logging.info(f"Query result: {result}")

            # Check if role exists
            if not result.empty:
                return result["ACCESS"].iloc[0] # Return the appropriate access admin
            else:
                return None  # No matching role found
        except Exception as e:
            st.error(f"❌ Error fetching role: {str(e)}")
            logging.error(f"Error fetching role from Snowflake: {e}")
            return None

    def execute_snowflake_function_and_get_trigger_id(function_name, job_id):
        query = f"""
        SELECT {function_name}('{job_id}');
        """
        try:
            result = snowflake_session.sql(query).collect()
            if result:
                trigger_id = result[0][0]  # Directly access the integer
                return trigger_id
            return None
        except Exception as e:
            st.error(f"❌ Error executing function: {str(e)}")
            logging.error(f"Error executing Snowflake function: {e}")
            return None


    def save_job_action_comment(task_history_id, source_type, action, email_id, comment):
        """
        Save job action details, including user comments, into the Snowflake table 'jobs_comments'.
        """

        # Escape the comment to prevent SQL injection issues
        comment_message = comment.replace("'", "''")
        insert_query = f"""
        INSERT INTO {procedure_db}.{procedure_schema}.jobs_comments (
            run_id,
            source_type,
            comments,
            reviewed,
            rerun,
            resolved,
            closed_flag,
        
            reviewer_name,
            comment_timestamp
        )
        VALUES (
            '{task_history_id}',        -- Task history ID (trigger_id)
            '{source_type}',            -- Source type (string)
            '{comment_message}',        -- Comment text (escaped string)
            FALSE,                      -- Reviewed (boolean)
            TRUE,                       -- Rerun flag (boolean)
            FALSE,                      -- Resolved flag (boolean)
            FALSE,                      -- Closed flag (boolean)
            '{email_id}',               -- Created by (email ID)
            CURRENT_TIMESTAMP           -- Timestamp (current time)
        );
        """
        try:
            snowflake_session.sql(insert_query).collect()
            return "Comment saved successfully!"
        except Exception as e:
            st.error(f"❌ Error saving comment in Snowflake: {str(e)}")
            logging.error(f"Error saving comment in Snowflake: {e}")
            return None

    def trigger_dbt_job(dbt_account_id: str, dbt_job_id: str, dbt_commands: str, user_comments: str) -> str:
        """
        Trigger the DBT job execution with user-defined steps using 'steps_override'.

        Parameters:
        - account_id (str): DBT Cloud account ID
        - job_id (str): Specific DBT job ID to trigger
        - dbt_commands (str): Comma-separated DBT commands
        - user_comments (str): Comments about the Ad-hoc run
        """
        # Parse DBT commands
        try:
            steps_override = [command.strip() for command in dbt_commands.split(",") if command.strip()]
            if not steps_override:
                raise ValueError("No valid DBT commands provided.")
        except Exception as e:
            st.error(f"❌ Error parsing DBT commands: {str(e)}")
            return None

        # DBT API endpoint and request payload
        url = f"https://cloud.getdbt.com/api/v2/accounts/{dbt_account_id}/jobs/{dbt_job_id}/run/"
        payload = {
            "cause": "Triggered via Streamlit (Ad-Hoc Run)",
            "steps_override": steps_override
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {CONSTANTS.DBT_TOKEN}"
        }

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            response_data = response.json()
            run_id = response_data.get("data", {}).get("id")

            if run_id:
                st.success(f"✅ Ad-Hoc DBT job triggered successfully! Run ID: {run_id}")
                save_comment_to_snowflake(run_id, user_comments, success_flag=True)
                return run_id
            else:
                st.error("⚠️ Unable to retrieve Run ID from the response.")
                save_comment_to_snowflake(None, user_comments, success_flag=False)
                return None

        except Exception as e:
            st.error(f"❌ Failed to trigger the Ad-Hoc job: {str(e)}")
            save_comment_to_snowflake(None, user_comments, success_flag=False)
            return None


    def save_comment_to_snowflake(run_id, user_comments, success_flag):
        """
        Save DBT job execution comments into the Snowflake database for Ad-Hoc actions.
        """
        # Escape user comments to prevent SQL injection
        comments_escaped = user_comments.replace("'", "''")

        insert_query = f"""
        INSERT INTO {procedure_db}.{procedure_schema}.jobs_comments (
            run_id,
        """
        # Escape the comment to avoid SQL injection
        comments_escaped = user_comments.replace("'", "''")

        insert_query = f"""
        INSERT INTO {procedure_db}.{procedure_schema}.jobs_comments (
            run_id,
            source_type,
            comments,
            adhoc_run,
            reviewer_name,
            REVIEWED,
            comment_timestamp
        )
        VALUES (
            '{run_id}',
            'Ad-Hoc Run',
            '{comments_escaped}',
            {success_flag},
            '{email_id}',
            TRUE,
            CURRENT_TIMESTAMP
        );
        """
        try:
            snowflake_session.sql(insert_query).collect()
            st.success("✅ Job comments saved successfully!")
        except Exception as e:
            st.error(f"❌ Failed to save job comments: {str(e)}")
            logging.error(f"Failed to save job comments: {e}")


    def adhoc_cs_page():
        """Ad-Hoc Runs functionality specific to CS Analytics Mart."""

        # CS-specific DBT Project
        DBT_ACCOUNT_ID = 179022  # Replace with your DBT Cloud Account ID
        DBT_PROJECT_ID = 276054  # Replace with CS-specific DBT Project ID
        DBT_JOB_ID = "931307"    # Replace with CS-specific DBT Job ID
        REQUIRED_ACCESS = ["Admin"]  # Access values to check

        # Get the current role of the logged-in user
        required_role = session.sql("SELECT CURRENT_ROLE();").to_pandas().iloc[0, 0]  # returns a string

        # Fetch user's access using current role
        user_access = get_user_role(email_id, required_role)

        # Check if user has correct access
        user_has_access = user_access in REQUIRED_ACCESS

        if user_has_access:
            # st.success(f"✅ Access granted to Ad-Hoc Runs. Welcome, {email_id}!")
            # st.markdown("<p style='color: green;'>You have the necessary permission to execute the Ad-Hoc Jobs for CS Analytics Mart.</p>", unsafe_allow_html=True)
            
            col1, col2 = st.columns(2, gap="large")
            with col1:
                st.header("Scheduled Jobs in the mart")
            # try:
                            # Fetch jobs for CS Project using Snowflake
                dbt_jobs = snowflake_session.sql(
                            
                f"""
                SELECT name AS job_name, id AS job_id, execute_steps
                FROM {procedure_db}.{procedure_schema}.DBT_SCHEDULES
                WHERE project_id = {DBT_PROJECT_ID}
                AND environment_id = '228384'
                """
                ).collect()
            

                dbt_job_logs= snowflake_session.sql(f"""
                        select b.job_id as Job_ID,c.name as Job_Name,a.execute_completed_at as LAST_RUN ,status,next_run as NEXT_RUN from EDW_LAB_DEV.OBSERVABILITY.DBT_RUN_RESULTS as a, 
                        EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS as b, EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES as c 
                        where a.invocation_id=b.invocation_id and b.job_id=c.id            
                """).collect()

                jobs_df = pd.DataFrame(dbt_jobs)
                job_options = [row["JOB_NAME"] for _, row in jobs_df.iterrows()]
                execute_steps_mapping = {
                    row["JOB_NAME"]: row["EXECUTE_STEPS"] for _, row in jobs_df.iterrows()
                }


                logs_df = pd.DataFrame(dbt_job_logs)
            # logs_options= [row["Job_Name"] for _, row in logs_df.iterrows()]
                #detail_steps_mapping = {
                ##   row["Job_Name"]: row["Last_Run"] for _, row in logs_df.iterrows()
                #}


                # Dropdown for selecting jobs
                selected_job_name = st.selectbox(
                    "Select a Specific Scheduled Job to Execute:",
                    options=list(job_options),
                    help="Choose a Customer Success Analytics Mart job for execution."
                )

                
                selected_job_id = jobs_df.loc[jobs_df["JOB_NAME"] == selected_job_name, "JOB_ID"].values[0]
                #selected_job_id1 = logs_df.loc[jobs_df["JOB_NAME"] == selected_job_name, "JOB_ID"].values[0]
                
                if selected_job_name:
                    execute_steps = execute_steps_mapping.get(selected_job_name, "No execute steps available for this job")
                    #execute_steps2 = detail_steps_mapping.get(selected_job_name, "No execute steps available for this job")
                    selected_row = logs_df[logs_df["JOB_NAME"] == selected_job_name].iloc[0]

                    selected_job_id = selected_row["JOB_ID"]
                    last_run = selected_row["LAST_RUN"]
                    status = selected_row["STATUS"]
                    next = selected_row["NEXT_RUN"]

                    job_details = f"""
                    Job ID   : {selected_job_id}
                    Job Name : {selected_job_name}
                    Last Run : {last_run}
                    Status    : {status}
                    Next Run : {next}
                    """
                    st.text_area(
                        label="Execute Steps (Read-Only):",
                        value=execute_steps,
                        help="Steps configured for this job",
                        disabled=True  # Make the text area read-only
                    )
                    st.text_area(
                        label="Job Details (Read-Only):",
                        value=job_details.strip(),
                        help="The unique Job ID",
                        disabled=True
                    )

                

                # Add a text area for comments
                scheduled_comments = st.text_area(
                    "Add Comments for Scheduled Job (Required):",
                    help="Provide comments for this scheduled job execution."
                )

                # Dropdown for action options (Run, Rerun)
                run_option = st.selectbox(
                    "Choose a Job Action on the Selected Scheduled Job:",
                    options=["Select Job Actions", "Run Scheduled Job", "Rerun From Failure"],
                    help="Run a new job or retry the same."
                )
                # Ensure comments are mandatory
                if not scheduled_comments.strip():
                    st.warning("⚠️ Comments are required to execute or rerun a job.")
                else:
                    # Execute job based on user's selected action
                    if run_option == "Run Scheduled Job":
                        # if "confirm_run" not in st.session_state:
                        #     st.session_state.confirm_run = False
                        # if not st.session_state.confirm_run:
                        if st.button("Run Job"):
                                # st.session_state.confirm_run = True
                                # st.warning("⚠️ Are you sure you want to run this job?")
                                # st.info("Click 'Confirm Run' to proceed or refresh to cancel.")
                                # st.button("Confirm Run")
                        # else:
                            # if st.button("Confirm Run"):
                                trigger_id = execute_snowflake_function_and_get_trigger_id(
                                    f"{procedure_db}.{procedure_schema}.DBT_RUN_JOB_API", selected_job_id
                                )
                                if trigger_id:
                                    save_job_action_comment(trigger_id, "Scheduled Job", "RUN ENTIRE JOB", email_id, scheduled_comments)
                                    st.markdown(
                                        f"""
                                        <p style="color: green; font-weight: bold;">
                                        ✅ Job Execution Triggered: <a href="https://cloud.getdbt.com/deploy/{DBT_ACCOUNT_ID}/projects/{DBT_PROJECT_ID}/runs/" target="_blank">View Run</a>
                                        </p>
                                        """,
                                        unsafe_allow_html=True,
                                    )

                    elif run_option == "Rerun From Failure":
                        if st.button("Rerun Job"):
                            trigger_id = execute_snowflake_function_and_get_trigger_id(
                                f"{procedure_db}.{procedure_schema}.DBT_RETRY_JOB_API", selected_job_id
                            )
                            if trigger_id:
                                save_job_action_comment(trigger_id, "Scheduled Job", "RERUN FROM FAILURE", email_id, scheduled_comments)
                                st.markdown(
                                    f"""
                                    <p style="color: green; font-weight: bold;">
                                    🔁 Rerun Triggered Successfully: <a href="https://cloud.getdbt.com/deploy/{DBT_ACCOUNT_ID}/projects/{DBT_PROJECT_ID}/runs/" target="_blank">View Run</a>
                                    </p>
                                    """,
                                    unsafe_allow_html=True,
                                )

                    elif run_option == "Select Job Actions":
                        st.warning("⚠️ Please select a valid Action for the job before proceeding.")

                # except Exception as e:
                #     st.error(f"❌ Failed to load scheduled job options for CS Analytics Mart: {e}")
                with col2:
                    # Section 2: Ad-Hoc Run for CS Analytics Mart
                    st.header("Perform Ad-Hoc Runs in the Mart")
                    adhoc_commands = st.text_area(
                        "Enter DBT commands (comma-separated):",
                        placeholder="e.g., dbt build --select model.name, dbt test --select tag:demo",
                        help="Provide a list of DBT commands for execution."
                    )
                    adhoc_comments = st.text_area("Add Comments for This Ad-Hoc Run (Required):")

                    if adhoc_comments.strip():
                        
                        if st.button("Run Ad-Hoc Job"):
                            # Execute the DBT job with entered commands and comments
                            run_id = trigger_dbt_job(DBT_ACCOUNT_ID, DBT_JOB_ID, adhoc_commands, adhoc_comments)

                            if run_id:
                                st.markdown(
                                    f"""<p style='color: green; font-weight: bold;'>✅ Job Successfully Triggered <a href='https://cloud.getdbt.com/deploy/{DBT_ACCOUNT_ID}/projects/{DBT_PROJECT_ID}/runs/{run_id}' target='_blank'>View Run Details</a></p>""",
                                    unsafe_allow_html=True,
                                )
                    else:
                            st.warning("⚠️ Comments are required to execute the Ad-Hoc Run.")

        else:
            st.error("❌ Access denied for Mart Ad-Hoc Runs.")
            st.markdown(
                f"""<p style='color: red;'>Your email ({email_id}) does not have the permissions required to access this module.</p>""",
                unsafe_allow_html=True,
            )
        st.markdown("---")
    #     st.markdown(
    #     """
    #     <div style="text-align: center; font-size: 0.9em; color: #888;">
    #         Built with ❤️ using Streamlit and DBT Cloud API by <strong>Cloudeqs</strong>.<br>
    #         Copyright © 2025.
    #     </div>
    #     """,
    #     unsafe_allow_html=True,
    # )
    st.markdown(
            """
            <div style="text-align: center; padding: 15px; background-color: #f4f4fa; border-radius: 10px; margin-bottom: 20px;">
                <h1 style="color: #333;">Ad-Hoc DBT Job Runner</h1>
                <p style="color: #666; font-size: 1.2em;">Dynamically update DBT job steps and execute them effortlessly</p>
            </div>
            """,
            unsafe_allow_html=True,
    )

    adhoc_cs_page()

# with tab_matilion:
#     # Add .../streamlit_github (project root) to sys.path
#     PROJECT_ROOT = Path(__file__).resolve().parents[2]
#     if str(PROJECT_ROOT) not in sys.path:
#         sys.path.insert(0, str(PROJECT_ROOT))

#     from matillion_api import (
#         list_projects, list_environments, list_published_pipelines,
#         execute_pipeline, get_execution_status, cancel_execution, recent_executions
#     )

#     # -------------------------
#     # UI
#     # -------------------------
#     st.title("Run & Monitor Matillion Pipelines")

#     with st.sidebar:
#        #st.logo("observability_app/src/assets/logo_cloudeqs.png", size="large")
#        # st.logo("./assets/logo_cloudeqs.png", size="large")
#        # st.header("Select Context")

#        projects = list_projects()
#        if not projects:
#            st.stop()

#        # proj_names = [f"{p.get('name')} ({p.get('id')})" for p in projects]
#        # proj_idx = st.selectbox("Project", options=range(len(projects)), format_func=lambda i: proj_names[i])
#        # project = projects[proj_idx]
#        # project_id = project["id"]

#        # envs = list_environments(project_id)
#        # if not envs:
#        #     st.warning("No environments found in this project.")
#        #     st.stop()

#        # filtered = [r for r in envs ]
#        # sorted_env = sorted(filtered, key=lambda r: r.get("name", "").lower())
#        # env_names = [e["name"] for e in sorted_env]
#        # env_name = st.selectbox("Environment", env_names)

#        # pipelines = list_published_pipelines(project_id, env_name)
#        # if not pipelines:
#        #     st.warning("No published pipelines found in this project.")
#        #     st.stop()

#        # filtered = [r for r in pipelines ]
#        # sorted_results = sorted(filtered, key=lambda r: r.get("name", "").lower())
#        # pipe_names = [p['name'] for p in sorted_results]
#        # pipeline_name = st.selectbox("Pipeline", pipe_names, index=0)

#        # exec_tag = st.text_input("Execution Tag (optional)")
#        # autorun = st.toggle("Auto-monitor after starting", value=True)
#        # run_btn = st.button("Run Pipeline", type="primary", use_container_width=True)

#    # st.subheader("Execute & Monitor")

#    # if run_btn:
#    #     try:
#    #         with st.status("Submitting run request…", state="running") as s:
#    #             resp = execute_pipeline(project_id, env_name, pipeline_name, exec_tag or None)
#    #             peid = resp.get("pipelineExecutionId")
#    #             if not peid:
#    #                 s.update(label="No execution ID returned.", state="error")
#    #                 st.stop()
#    #             s.update(label=f"Started execution: {peid}", state="complete")
#    #             st.session_state.last_exec_id = peid
#    #     except requests.HTTPError as e:
#    #         st.error(f"Failed to start pipeline: {e.response.text if e.response is not None else e}")
#    #     except Exception as e:
#    #         st.error(f"Failed to start pipeline: {e}")

#    # peid = st.session_state.get("last_exec_id")
#    # peid_input = st.text_input("Pipeline Execution ID (monitor any ID)", value=peid or "", placeholder="1398aa31-af57-4a6a-9752-27c2e8556c3f")

#    # c1, c2, c3 = st.columns([1, 1, 1])
#    # with c1:
#    #     start_monitor = st.button("Start monitoring", use_container_width=True)
#    # with c2:
#    #     stop_monitor = st.button("Stop monitoring", use_container_width=True, disabled=True)
#    # with c3:
#    #     cancel_btn = st.button("Cancel execution", use_container_width=True, disabled=not peid_input)

#    # if cancel_btn and peid_input:
#    #     try:
#    #         cancel_execution(project_id, peid_input)
#    #         st.success("Cancellation requested.")
#    #     except requests.HTTPError as e:
#    #         st.error(f"Cancel failed: {e.response.text if e.response is not None else e}")
#    #     except Exception as e:
#    #         st.error(f"Cancel failed: {e}")

#    # if (autorun and run_btn) or start_monitor:
#    #     if not peid_input:
#    #         st.warning("Enter a Pipeline Execution ID to monitor.")
#    #     else:
#    #         ph_status = st.empty()
#    #         ph_meta = st.empty()
#    #         ph_progress = st.progress(0, text="Waiting for status…")

#    #         TERMINAL = {"SUCCESS", "FAILED", "TERMINATED"}
#    #         max_seconds = 5 * 60
#    #         poll_every = 3
#    #         waited = 0

#    #         while waited <= max_seconds:
#    #             try:
#    #                 data = get_execution_status(project_id, peid_input)
#    #                 result = (data or {}).get("result", {})
#    #                 status = result.get("status", "UNKNOWN")
#    #                 started = result.get("startedAt")
#    #                 finished = result.get("finishedAt")
#    #                 message = result.get("message")

#    #                 pct = min(99, int((waited / max_seconds) * 100)) if status not in TERMINAL else 100
#    #                 ph_progress.progress(pct, text=f"Status: {status}")

#    #                 ph_status.info(f"**Status:** {status}")
#    #                 meta_lines = []
#    #                 if started: meta_lines.append(f"**Started:** {started}")
#    #                 if finished: meta_lines.append(f"**Finished:** {finished}")
#    #                 if message: meta_lines.append(f"**Message:** {message}")
#    #                 ph_meta.markdown("\n\n".join(meta_lines) if meta_lines else "_No extra details yet…_")

#    #                 if status in TERMINAL:
#    #                     if status == "SUCCESS":
#    #                         st.success("Pipeline completed successfully ✅")
#    #                     elif status == "FAILED":
#    #                         st.error("Pipeline failed ❌")
#    #                     else:
#    #                         st.warning("Pipeline terminated ⚠️")
#    #                     ph_progress.progress(100, text=f"Status: {status}")
#    #                     break

#    #                 time.sleep(poll_every)
#    #                 waited += poll_every
#    #             except requests.HTTPError as e:
#    #                 st.error(f"Status check failed: {e.response.text if e.response is not None else e}")
#    #                 break
#    #             except Exception as e:
#    #                 st.error(f"Status check failed: {e}")
#    #                 break

#    #         if waited > max_seconds:
#    #             st.warning("Stopped monitoring after 5 minutes. Click **Start monitoring** to continue polling.")

#    # st.subheader("Recent Executions")
#    # results = recent_executions(project_id, pipeline_name, days=7, limit=200)
#    # if results:
#    #     table = [{
#    #         "pipelineName": r.get("pipelineName"),
#    #         "status": r.get("status"),
#    #         "trigger": r.get("trigger"),
#    #         "startedAt": r.get("startedAt"),
#    #         "finishedAt": r.get("finishedAt"),
#    #         "pipelineExecutionId": r.get("pipelineExecutionId"),
#    #     } for r in results]
#    #     st.dataframe(table, use_container_width=True, height=420)
#    # else:
#    #     st.info("No executions found in the last 7 days.")

    # st.caption("Note: This page uses Matillion DPC public API endpoints to list published pipelines, execute them, poll status, and cancel runs.")



