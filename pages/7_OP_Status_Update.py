import streamlit as st
from spcs_helpers.connection import session
import pandas as pd
import plotly.express as px
import json
import time
from datetime import datetime, date, timedelta, timezone
import os
import snowflake.connector

session = session()

st.set_page_config(
    page_title="Job Logs Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

def load_css(path):
    with open(path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css("CSS/sidebar.css")
load_css("CSS/op_status.css")


from sidebar import render_sidebar
render_sidebar()

# Data loading functions

def get_dbt_api_failed_jobs(param, time_input_start, time_input_end):
    """Query for DBT API failed jobs."""
    time_input_start = str(time_input_start).replace('+00:00', '')
    time_input_end = str(time_input_end).replace('+00:00', '')

    dbt_session = session
    dbt_api = f"""
    select EDW_LAB_DEV.OBSERVABILITY.FAILED_DBT_JOBS_API({param}, '{time_input_start}', '{time_input_end}');
    """
    dbt_df = dbt_session.sql(dbt_api).to_pandas()
    response = dbt_df.iloc[0, 0]
    data = json.loads(response)
    rows = []
    for df in data:
        row = (df.get("id"),
               df.get("trigger_id"),
               df.get("environment_id"),
               df.get("account_id"),
               df.get("project_id"),
               df.get("job_definition_id"),
               df.get("status"),
               df.get("job_id"),
               df.get("href"),
               df.get("status_message"),
               df.get("created_at"),
               df.get("updated_at"),
               df.get("started_at"),
               df.get("finished_at"),
               df.get("last_heartbeat_at"),
               df.get("should_start_at"),
               df.get("status_humanized").upper(),
               df.get("trigger"),
               df.get("in_progress"),
               df.get("is_complete"),
               df.get("is_error"),
               df.get("is_failed"),
               df.get("duration"),
               df.get("queued_duration"),
               df.get("run_duration")
               )
        rows.append(row)
    columns = ['TASK_HISTORY_ID', 'trigger_id', 'environment_id', 'account_id', 'project_id', 'job_definition_id', 'status',
               'job_id', 'LINKS', 'ERROR_MESSAGE', 'created_at', 'updated_at',
               'START_TIME', 'END_TIME', 'last_heartbeat_at', 'should_start_at', 'STATUS',
               'trigger', 'in_progress', 'is_complete', 'is_error', 'is_failed', 'duration',
               'queued_duration', 'run_duration']
    dbt_data = pd.DataFrame(rows, columns=columns)
    dbt_data['START_TIME'] = pd.to_datetime(dbt_data['START_TIME'])
    dbt_data['END_TIME'] = pd.to_datetime(dbt_data['END_TIME'])
    dbt_data = dbt_data.query("environment_id in [314701,282110, 218564]")
    dbt_data['TASK_HISTORY_ID'] = dbt_data['TASK_HISTORY_ID'].astype(int)

    query_schedule = '''
    SELECT DISTINCT
        ST.ID AS "job_id",
        upper (ST.NAME) AS schedule_name, ST.execute_steps AS JOB_TAG_NAME,
        '' AS TRIGGER_BY,
        di.project_name,
        'DBT' AS SOURCE_TYPE
    FROM EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES ST
    LEFT JOIN EDW_LAB_DEV.OBSERVABILITY.DBT_PROJECTS di
    ON di.project_id = ST.project_id;
    '''
    schedule_data = dbt_session.sql(query_schedule)
    schedule_df = schedule_data.to_pandas()
    merged_df = pd.merge(dbt_data, schedule_df, on=['job_id'], how='left')
    merged_df['TOTAL_RUNTIME_MINUTES'] = (pd.to_datetime(merged_df['END_TIME']) - pd.to_datetime(merged_df['START_TIME'])).dt.total_seconds() / 60
    merged_df['START_TIME'] = pd.to_datetime(merged_df['START_TIME']).dt.tz_localize(None)
    merged_df['END_TIME'] = pd.to_datetime(merged_df['END_TIME']).dt.tz_localize(None)
    return merged_df


def get_matillion_failed_dataframe():
    """Query for failed Matillion jobs."""
    query = """
    with cte_mat_temp as(
        SELECT upper(sd.name) as SCHEDULE_NAME, upper (rh.job_name) JOB_TAG_NAME, DAYOFWEEK(rh.START_TIME_PST),
            rh.START_TIME_PST AS START_TIME, rh.END_TIME_PST AS END_TIME, TIMESTAMPDIFF('minute',START_TIME, END_TIME) AS TOTAL_RUNTIME_MINUTES,
            upper(rh.STATE) as STATUS, rh.MESSAGE as ERROR_MESSAGE,
            rh.TASK_HISTORY_ID, rh. PROJECT_NAME,
            'MATILLION' AS SOURCE_TYPE,
            null as LINKS, null as TRIGGER_BY
        FROM
            EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY rh
        JOIN
            EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS sd
            ON rh. PROJECT_NAME = sd.PROJECT AND upper (rh.STATE)='FAILED' and
            rh.JOB_NAME = sd.JOB_NAME
            and run_date=(select max(run_date) from EDW_LAB_DEV.OBSERVABILITY.matillion_schedules_details)
        WHERE
            enabled=true and
            rh.JOBTYPE in ('SCHEDULE_ORCHESTRATION', 'QUEUE_ORCHESTRATION') and rh.PROJECT_NAME='ZSCALER_BI_DWH' and
            END_TIME >= DATEADD(hour, -168, CURRENT_TIMESTAMP) ORDER BY END_TIME DESC
    ),
    mat_final_cte as(
        select * from cte_mat_temp where lower (SCHEDULE_NAME) not like '%weekend%' and lower (SCHEDULE_NAME) not like '%weekday%'
        union
        select * from cte_mat_temp where lower (SCHEDULE_NAME) like '%weekend%' and DAYOFWEEK (START_TIME) in (0,6)
        union
        select * from cte_mat_temp where lower (SCHEDULE_NAME) like '%weekday%' and (DAYOFWEEK (START_TIME) in (1,2,3,4,5))
    )
    SELECT
        SCHEDULE_NAME, JOB_TAG_NAME, START_TIME, END_TIME, TOTAL_RUNTIME_MINUTES,
        TASK_HISTORY_ID, STATUS, ERROR_MESSAGE,
        PROJECT_NAME, 'MATILLION' AS SOURCE_TYPE,
        null as LINKS, null as TRIGGER_BY
    FROM mat_final_cte;
    """
    id_df = session.sql(query)
    task_df = id_df.to_pandas()
    return task_df


def get_matillion_running_queue():
    """Query for running/queued Matillion jobs."""
    mat_session = session
    time_query = f"""
    SELECT
    CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', MAX(END_TIME_PST))
    FROM EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY where END_TIME_PST is not null
    """
    time_data = mat_session.sql(time_query).to_pandas() 
    timestamp_str = str(time_data.iloc[0,0])
    
    # Handle None or invalid timestamp
    if timestamp_str == 'None' or ' ' not in timestamp_str:
        return pd.DataFrame()
    date_str, time_str = timestamp_str.split(' ')
    time_str = time_str.split('.')[0]
    time_str = time_str[:5]

    mat_api = f"""
    select EDW_LAB_DEV.OBSERVABILITY.MATILLION_JOBS_API('{date_str}', '{time_str}');
    """
    mat_df = mat_session.sql(mat_api).to_pandas()
    resp = mat_df.iloc[0, 0]
    data = json.loads(resp)
    body_data = data
    rows = []
    for df in body_data:
        row = (df.get("id"),
               df.get("projectName"),
               df.get("state"),
               df.get("jobName"),
               df.get("startTime"),
               df.get("endTime")
               )
        rows.append(row)

    columns = ['TASK_HISTORY_ID', 'PROJECT_NAME',
               'STATUS', 'SCHEDULE_NAME',
               'START_TIME', 'END_TIME']
    mat_data = pd.DataFrame(rows, columns=columns)
    mat_data['SCHEDULE_NAME'] = mat_data['SCHEDULE_NAME'].str.upper()
    mat_data['JOB_TAG_NAME'] = mat_data['SCHEDULE_NAME']
    mat_data.loc[mat_data['STATUS'].isin(['QUEUED']), 'START_TIME'] = None
    mat_data.loc[mat_data['STATUS'].isin(['RUNNING', 'QUEUED']), 'END_TIME'] = None
    mat_data['SOURCE_TYPE'] = 'MATILLION'
    return mat_data[mat_data['STATUS'] == 'FAILED']


def get_dbt_failed_data():
    """Query for failed DBT jobs."""
    dbt_session = session
    query_failed = '''
        SELECT distinct cast(fj.id as number) as TASK_HISTORY_ID,upper(st.NAME) AS schedule_name, st.execute_steps AS JOB_TAG_NAME,
        fj.started_at as start_time,
        fj.finished_at as end_time,
        'FAILED' as STATUS,
        fj.status_message as error_message, di.project_name,
        'DBT' AS SOURCE_TYPE,
        fj.href as LINKS, fj."TRIGGER" as trigger_by
    FROM EDW_LAB_DEV.OBSERVABILITY.DBT_FAILED_JOBS fj
    left join EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES st on fj.job_id=st.id
    LEFT JOIN EDW_LAB_DEV.OBSERVABILITY.DBT_PROJECTS di
    ON di.project_id= st.project_id;
    '''
    failed_data = dbt_session.sql(query_failed)
    failed_df = failed_data.to_pandas()
    failed_df['START_TIME'] = pd.to_datetime(failed_df['START_TIME'])
    failed_df['END_TIME'] = pd.to_datetime(failed_df['END_TIME'])
    failed_df['TOTAL_RUNTIME_MINUTES'] = (pd.to_datetime(failed_df['END_TIME']) - pd.to_datetime(failed_df['START_TIME'])).dt.total_seconds() / 60

    query_comments = '''
    SELECT
        cc.run_id AS TASK_HISTORY_ID,
        cc.reviewed,
        cc.comments,
        cc.comment_timestamp
    FROM EDW_LAB_DEV.OBSERVABILITY. JOBS_COMMENTS cc
    '''
    comments_data = dbt_session.sql(query_comments)
    comments_df = comments_data.to_pandas()

    failed_df['TASK_HISTORY_ID'] = failed_df['TASK_HISTORY_ID'].astype(int)
    comments_df['TASK_HISTORY_ID'] = pd.to_numeric(comments_df['TASK_HISTORY_ID'], errors='coerce').astype('Int64')
    comments_df = comments_df.dropna(subset=['TASK_HISTORY_ID'])
    comments_df['TASK_HISTORY_ID'] = comments_df['TASK_HISTORY_ID'].astype(int)

    failed_df = pd.merge(failed_df, comments_df, on='TASK_HISTORY_ID', how='left')

    columns_to_display = ['SCHEDULE_NAME', 'JOB_TAG_NAME', 'START_TIME', 'END_TIME', 'TOTAL_RUNTIME_MINUTES', 'TASK_HISTORY_ID', 'STATUS', 'ERROR_MESSAGE',
                          'PROJECT_NAME', 'SOURCE_TYPE', 'LINKS',
                          'TRIGGER_BY', 'COMMENTS', 'REVIEWED']
    final_fail_df = failed_df[columns_to_display]
    return final_fail_df


def fetch_all_op_status_data():
    """Master loader - combines all operational status data sources."""
    # DBT data
    gmt_end_time = datetime.now(timezone.utc)
    gmt_start_time = datetime.now(timezone.utc) - timedelta(hours=24)
    dbt_api_task_ids = get_dbt_api_failed_jobs(20, gmt_start_time, gmt_end_time)
    dbt_table_task_ids = get_dbt_failed_data()
    
    # Matillion data
    mat_table_task_df = get_matillion_failed_dataframe()
    mat_api_task_ids = get_matillion_running_queue()
    
    return dbt_api_task_ids, dbt_table_task_ids, mat_table_task_df, mat_api_task_ids

# spinner_placeholder=st.empty()
# with st.spinner ('Loading, please wait...'):
#     #st.session_state.spinner_text='Loading dataframe and charts...'
#     #spinner_placeholder.markdown ("<p style='color: #5D6A85;'>Loading dataframe and charts...</p>", unsafe_allow_html=True) 
#     cl1, cl2, cl3 = st.columns ([4,1,1])
#     with cl3:
#         st.image("assets/logo_cloudeqs.png", width=200)



# with st.container(border=False):
#     st.markdown(
#         """
#         <div>
#             <h1 class="hover-effect">
#                 OP Status Update
#             </h1>
#         </div>
#         """, unsafe_allow_html=True
#     )

# col1, col2 = st.columns([1, 3])  # adjust the ratio as needed
# with col1:
#     source_type = st.selectbox("Select Source", ["MATILLION", "DBT"])
@st.fragment
def render_op_status_fragment():
    """Fragment UI with session state for operational status updates."""
    
    # Initialize session state for filters
    if 'op_status_filters' not in st.session_state:
        st.session_state.op_status_filters = {
            'source_type': 'MATILLION',
            'task_history_id': None,
            'comments': '',
            'reviewed': False
        }
    
    def _update_source():
        st.session_state.op_status_filters['source_type'] = st.session_state.source_select
    
    # Header
    # with st.container(border=False):
    #     st.markdown(
    #         """
    #         <div>
    #             <h1 style="font-family: Inter, sans-serif; font-size: 22px; text-align: left;">
    #                 OP Status Update
    #             </h1>
    #         </div>
    #         """, unsafe_allow_html=True
    #     )
    
    # Load all data once (cached)
    dbt_api_task_ids, dbt_table_task_ids, mat_table_task_df, mat_api_task_ids = fetch_all_op_status_data()
    
    # Source selector
    st.selectbox("Select Source", ["MATILLION", "DBT"], key='source_select', on_change=_update_source)
    source_type = st.session_state.op_status_filters['source_type']
    
    # Process data based on source type
    with st.form(key="task_form"):
        if source_type == "MATILLION":
            matillion_df = pd.concat([mat_table_task_df, mat_api_task_ids], ignore_index=True)
            matillion_df['TASK_HISTORY_ID'] = matillion_df['TASK_HISTORY_ID'].astype(int)
            # Only drop duplicates by TASK_HISTORY_ID to keep all jobs with same name but different IDs
            matillion_df = matillion_df.drop_duplicates(subset=['TASK_HISTORY_ID'], keep='first')
            matillion_df = matillion_df.sort_values(by='TASK_HISTORY_ID', ascending=False)
            mat_task_id_with_name = [f"{task_id} - {job_tag}" for task_id, job_tag in zip(matillion_df['TASK_HISTORY_ID'], matillion_df['JOB_TAG_NAME'])]
            task_hist_id = mat_task_id_with_name
        else:
            dbt_df = pd.concat([dbt_table_task_ids, dbt_api_task_ids], ignore_index=True)
            dbt_df['TASK_HISTORY_ID'] = dbt_df['TASK_HISTORY_ID'].astype(int)
            # Only drop duplicates by TASK_HISTORY_ID to keep all jobs with same name but different IDs
            dbt_df = dbt_df.drop_duplicates(subset=['TASK_HISTORY_ID'], keep='first')
            dbt_df = dbt_df.sort_values(by='TASK_HISTORY_ID', ascending=False)
            dbt_task_id_with_name = [f"{task_id} - {schedule_name}" for task_id, schedule_name in zip(dbt_df['TASK_HISTORY_ID'], dbt_df['SCHEDULE_NAME'])]
            task_hist_id = dbt_task_id_with_name
        
        if len(task_hist_id) > 0:
            task_history_id_with_name = st.selectbox("Select Task History ID", task_hist_id, key='task_select_box')
            task_history_id = int(task_history_id_with_name.split(' - ')[0])
            comments = st.text_input("Enter Comments")
            reviewed = st.checkbox("Reviewed")
        else:
            st.warning('No Data Available')
        
        submit_button = st.form_submit_button(label="Save")

    # Handle form submission
    # Get user email from Snowflake session (current logged-in user)
    try:
        user_mail = session.sql("SELECT CURRENT_USER()").to_pandas().iloc[0, 0]
    except:
        user_mail = "observabilityservice@example.com"
    
    if submit_button and len(task_hist_id) > 0:
        comments_escaped = comments.replace("'", "''")
        reviewed_val = "TRUE" if reviewed else "FALSE"
        merge_query = f"""
        insert into EDW_LAB_DEV.OBSERVABILITY.JOBS_COMMENTS  (
        run_id,
        source_type,
        comments,
        reviewed,
        reviewer_name,
        comment_timestamp
    )
        values (
        '{task_history_id}',
        '{source_type}',
        '{comments_escaped}',
        {reviewed_val},
        '{user_mail}',
         current_timestamp
    );
    """
        data = session.sql(merge_query)
        st.write(data)
        st.success("Data merged successfully!")


# Main page rendering
st.markdown("""<style>div[data-testid="stAppViewContainer"] { padding: 0; } .stAppViewContainer > div { margin: 0; } .stAppViewContainer > div > div { margin: 0; }</style>""", unsafe_allow_html=True)

# Logo
# cl1, cl2, cl3 = st.columns([4, 1, 1])
# with cl3:
#     st.image("assets/logo_cloudeqs.png", width=200)

# Render the operational status fragment
render_op_status_fragment()
