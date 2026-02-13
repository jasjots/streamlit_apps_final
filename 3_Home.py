import streamlit as st
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
from spcs_helpers.connection import session
from snowflake.snowpark.context import get_active_session

session = session()
# Set page configuration for Streamlit
st.set_page_config(
    page_title="Job Logs Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

def load_css(path):
    with open(path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css("CSS/sidebar.css")
load_css("CSS/app.css")

from sidebar import render_sidebar
render_sidebar()


# Function to convert UTC time to PST


def utc_to_pst(utc_time):
    utc_zone = pytz.utc
    pst_zone = pytz.timezone('US/Pacific')
    
    # Convert the string to a datetime object
    utc_time = datetime.strptime(utc_time, "%Y-%m-%d %H:%M:%S.%f%z")
    
    # Replace the timezone with UTC
    utc_time = utc_time.astimezone(utc_zone)
    
    # Convert to PST
    pst_time = utc_time.astimezone(pst_zone)
    
    return pst_time

# Function to get DBT schedules API response from Snowflake
def dbt_schedules_api():
    sch_session = session
    schedule_api = """
    select EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES_API();
    """
    sch_df = sch_session.sql(schedule_api).to_pandas()
    response = sch_df.iloc[0, 0]
    if response is not None:
        data = json.loads(response)
        rows = []
        for df in data:
            if df is not None:
                row= (df.get("name"), df.get("next_run"),
                df.get("job_type"), df.get("id"),
                df.get("project_id"),
                df.get("environment_id"),
                df.get("execute_steps")
            )
            rows.append(row)
        columns = ['NAME', 'NEXT_RUN', 'JOB_TYPE', 'ID', 'PROJECT_ID', 'ENVIRONMENT_ID', 'EXECUTE_STEPS'] 
        schedule_data= pd.DataFrame (rows, columns=columns)
        schedule_data['SOURCE_TYPE'] = 'DBT'
    
        sch_arr=schedule_data.values.tolist()
        df1 = sch_session.create_dataframe (sch_arr,schema=schedule_data.columns.tolist())
        df1.write.save_as_table("EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES", mode="overwrite")

def convert_to_pst (epoch_time):
    utc_time = datetime.fromtimestamp (epoch_time / 1000, tz=pytz.utc)
    pst_timezone = pytz.timezone ('America/Los_Angeles')
    pst_time = utc_time.astimezone (pst_timezone)
    return pst_time.strftime('%Y-%m-%d %H:%M:%S.%f')

def dbt_running_run_and_queue (param):
    dbt_session = session

    dbt_api=f"""
    select EDW_LAB_DEV.OBSERVABILITY.DBT_JOBS_API({param});
    """
    dbt_df = dbt_session.sql (dbt_api).to_pandas () 
    response = dbt_df.iloc[0,0]
    data=json.loads(response)
    rows = []
    for df in data["data"]:
        row= (df.get("id"), 
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
        df.get("status_humanized").upper (),
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
    columns=['TASK_HISTORY_ID', 'trigger_id', 'environment_id', 'account_id', 'project_id', 'job_definition_id', 'status', 'job_id', 'LINKS', 'ERROR_MESSAGE', 'created_at', 'updated_at',
    'START_TIME', 'END_TIME', 'last_heartbeat_at', 'should_start_at', 'STATUS',
    'trigger', 'in_progress', 'is_complete', 'is_error', 'is_failed', 'duration', 'queued_duration', 'run_duration']
    dbt_data=pd.DataFrame (rows, columns=columns)

    dbt_data['START_TIME'] = pd.to_datetime (dbt_data['START_TIME'], utc=True) 
    dbt_data['END_TIME'] = pd.to_datetime (dbt_data['END_TIME'], utc=True)

    pst_timezone =pytz.timezone('America/Los_Angeles')
    # Convert to PST
    dbt_data['START_TIME'] = dbt_data['START_TIME'].dt.tz_convert(pst_timezone)
    dbt_data['END_TIME'] = dbt_data['END_TIME'].dt.tz_convert(pst_timezone)


    query_schedule='''
    WITH cte_tag AS (
        SELECT DISTINCT
            project_name,
            project_id,
        FROM EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS
    )
    SELECT DISTINCT
        ST.ID AS "job_id",
        upper (ST.NAME) AS schedule_name,
        st.execute_steps as job_tag_name,
        di.project_name,
        '-' as trigger_by,
        'DBT' AS SOURCE_TYPE
    FROM EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES ST
    LEFT JOIN cte_tag di
    ON CAST(di.project_id AS VARCHAR)= CAST(ST.project_id AS VARCHAR);
    '''
    schedule_data=dbt_session.sql(query_schedule) 
    schedule_df=schedule_data.to_pandas()
    merged_df = pd.merge(dbt_data, schedule_df, on=['job_id'], how='left')

    #st.dataframe (merged_df)
    jobs_count = dbt_data[(dbt_data['status'] ==param)].shape[0]

    #st.write(f"Total number of jobs: {jobs_count}")
    return jobs_count, merged_df

def dbt_failed_jobs (param, time_input_start, time_input_end): 
    time_input_start = str(time_input_start).replace('+00:00','') 
    time_input_end = str(time_input_end).replace('+00:00','') 
    # st.write(time_input_start)
    # st.write(time_input_end)

    dbt_session = session
    dbt_api=f"""
        select EDW_LAB_DEV.OBSERVABILITY.FAILED_DBT_JOBS_API('{param}', '{time_input_start}', '{time_input_end}');
    """
    dbt_df = dbt_session.sql(dbt_api).to_pandas()
    response = dbt_df.iloc[0,0]
    if len(response) > 0:
        data=json.loads(response)
        rows=[]
        for df in data:
            row= (df.get("id"),
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
        columns=['TASK_HISTORY_ID', 'trigger_id', 'environment_id', 'account_id', 'project_id', 'job_definition_id', 'status', 
                'job_id', 'LINKS', 'ERROR_MESSAGE', 'created_at', 'updated_at',
                'START_TIME', 'END_TIME', 'last_heartbeat_at', 'should_start_at', 'STATUS',
                'trigger', 'in_progress', 'is_complete', 'is_error', 'is_failed', 'duration', 
                'queued_duration', 'run_duration']
        dbt_data=pd.DataFrame(rows, columns=columns)
    
        dbt_data['START_TIME'] = pd.to_datetime(dbt_data['START_TIME']) 
        dbt_data['END_TIME'] = pd.to_datetime(dbt_data['END_TIME']) 
        dbt_data=dbt_data.query("environment_id in [314701,282110, 218564]")
    
        dbt_data['TASK_HISTORY_ID'] = dbt_data['TASK_HISTORY_ID'].astype(int)
        query_schedule='''
        SELECT DISTINCT
            ST.ID AS "job_id",
            upper (ST.NAME) AS schedule_name, ST.execute_steps AS job_tag_name, '' AS TRIGGER_BY,
            di.project_name,
            'DBT' AS SOURCE_TYPE
        FROM EDW_LAB_DEV.OBSERVABILITY.DBT_PROJECTS di
        LEFT JOIN EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES ST
        ON di.project_id = ST.project_id;
        '''
        schedule_data=dbt_session.sql(query_schedule)
        schedule_df=schedule_data.to_pandas()
        #st.dataframe (schedule_df)
        merged_df = pd.merge(dbt_data, schedule_df, on=['job_id'], how='left') 
        #st.dataframe (merged_df)
        merged_df[ 'TOTAL RUNTIME_MINUTES'] = (merged_df ['END_TIME'] - merged_df ['START_TIME']).dt.total_seconds() / 60
    
        query_comments = '''
            SELECT
                --cast (cc.run_id as number) AS TASK_HISTORY_ID,
                cc.run_id AS TASK_HISTORY_ID,
                cc.reviewed,
                CC.REVIEWER_NAME,
                LISTAGG(cc.comments, ', ') AS COMMENTS
            FROM EDW_LAB_DEV.OBSERVABILITY.JOBS_COMMENTS cc
            GROUP BY ALL
            '''
        comments_data = dbt_session.sql(query_comments) 
        comments_df = comments_data.to_pandas()
    
        # Merge merged_df with comments data on TASK_HISTORY_ID
        merged_df = pd.merge (merged_df, comments_df, on='TASK_HISTORY_ID', how='left') 
        #st.dataframe (merged_df)
        jobs_count = dbt_data[(dbt_data['status'] == param)].shape[0]
    
        return jobs_count, merged_df
    else:
        st.warning('No data available')
    

def mat_running_run_and_queue():
 
    mat_session = session
    time_query=f"""
    SELECT
    CONVERT_TIMEZONE ('America/Los_Angeles', 'UTC', MAX (END_TIME_PST))
    FROM EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY where END_TIME_PST is not null
    """
    time_data = mat_session.sql(time_query).to_pandas()
    timestamp_str = str(time_data.iloc[0,0])
    date_str, time_str = timestamp_str.split(' ')
    # Further split the time to remove milliseconds
    time_str = time_str.split('.')[0]
    # Extract hours and minutes
    time_str= time_str[:5]
    print(date_str,time_str)
    #st.write(date_str,'-', time_str)
    mat_api=f"""
        select EDW_LAB_DEV.OBSERVABILITY.MATILLION_JOBS_API('{date_str}', '{time_str}');
    """
    mat_df = mat_session.sql (mat_api).to_pandas()
    resp = mat_df.iloc[0,0]
    #clean_res=resp.replace("'", '"')
    data=json.loads(resp)
    body_data=data
    rows = []
    for df in body_data:
        row= (df.get("id"),
        df.get("projectName"),
        df.get("state"),
        df.get("jobName"),
        df.get("startTime"),
        df.get("endTime")
        )
        rows.append(row)
    columns=['TASK_HISTORY_ID', 'PROJECT_NAME',
             'STATUS','SCHEDULE_NAME',
             'START_TIME', 'END_TIME']
    mat_data=pd.DataFrame(rows, columns=columns)
    # st.write("test")
    # st.write(mat_data["STATUS"].value_counts())
    mat_data['TASK_HISTORY_ID'] = mat_data['TASK_HISTORY_ID'].astype(int)
    mat_data['SCHEDULE_NAME'] = mat_data['SCHEDULE_NAME'].str.upper()
    mat_data['JOB_TAG_NAME']=mat_data['SCHEDULE_NAME']
 
 
    mat_data['START_TIME'] = mat_data['START_TIME'].apply(convert_to_pst)
    mat_data['END_TIME'] = mat_data['END_TIME'].apply(convert_to_pst)
    mat_data.loc[mat_data['STATUS'].isin(['QUEUED']), 'START_TIME'] = None
    mat_data.loc[mat_data['STATUS'].isin(['RUNNING', 'QUEUED']), 'END_TIME'] = None
 
    mat_data['TOTAL_RUNTIME_MINUTES'] = (
        pd.to_datetime(mat_data['END_TIME']) - pd.to_datetime(mat_data['START_TIME'])
    ).dt.total_seconds() // 60
    # Create LINKS column
 
    mat_data['SOURCE_TYPE']='MATILLION'
 
    # query_schedule = '''
    # SELECT
    #pd.to_datetime (mat_data['START_TIME'])
    #distinct upper (sd.name) as schedule_name, upper (sd.job_name) as job_name,
    #max (run_date) as run_date
    # FROM EDW_LAB_DEV.OBSERVABILITY.MATILLION SCHEDULES_DETAILS sd
    # where run_date=CURRENT_DATE
    # group by schedule_name, job_name
    #'''
    # schedule_data = mat_session.sql (query_schedule)
    # schedule_df = schedule_data.to_pandas ()
    # # Merge merged_df with comments data on TASK_HISTORY_ID
    #mat_data = pd.merge (mat_data, schedule_df, on='JOB_NAME', how='left')
    query_comments = '''
            SELECT
                --cast (cc.run_id as number) AS TASK_HISTORY_ID,
                cc.run_id AS TASK_HISTORY_ID,
                cc.reviewed,
                CC.REVIEWER_NAME,
                LISTAGG(cc.comments, ', ') AS COMMENTS
            FROM EDW_LAB_DEV.OBSERVABILITY.JOBS_COMMENTS cc
            GROUP BY ALL
            '''
    comments_data = mat_session.sql(query_comments)
    comments_df = comments_data.to_pandas()
    comments_df['TASK_HISTORY_ID'] = (
    comments_df['TASK_HISTORY_ID']
    .replace('None', None)
    .astype('Int64')
    )
        # Merge merged_df with comments data on TASK_HISTORY_ID
    mat_data = pd.merge (mat_data, comments_df, on='TASK_HISTORY_ID', how='left')
    # mat_data = pd.concat (mat_data, comments_df, on='TASK_HISTORY_ID', how='left')
    return mat_data

def fetch_schedule_status_data():
    """Loader for schedule status data (fresh SQL on each reload)."""
    query = """
    with cte_dbt_status as (
        select listagg (distinct
        CASE
            WHEN contains (lower (rr.status), 'error') THEN 'error' 
            ELSE 'success'
        END
        ,',') status, listagg (distinct
        CASE
            WHEN contains (lower (rr.status), 'error') THEN message 
            ELSE ''
        END
        ,',') message, job_run_id, listagg (distinct di.SELECTED,',') AS JOB_TAG_NAME
        from EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS di inner join
        EDW_LAB_DEV.OBSERVABILITY.DBT_RUN_RESULTS rr on di.invocation_id = rr.invocation_id
        where di.env='prod' and di.cause_category in ('scheduled', 'other') and rr.resource_type in ('model', 'test', 'snapshot', 'seed') and 
                CONVERT_TIMEZONE ('UTC', 'America/Los_Angeles', di. RUN_COMPLETED_AT) >= DATEADD (hour, -24, CURRENT_TIMESTAMP) 
                group by job_run_id
    ),
    cte_mat_temp as (
        SELECT 
            sd.name as SCHEDULE_NAME, 
            DAYOFWEEK (rh.START_TIME_PST), 
            upper (rh.job_name) JOB_TAG_NAME,
            rh.START_TIME_PST AS START_TIME, 
            rh.END_TIME_PST AS END_TIME, 
            TIMESTAMPDIFF ('minute', START_TIME, END_TIME) AS TOTAL_RUNTIME_MINUTES, 
            upper (rh.STATE) as STATUS, 
            rh.MESSAGE as ERROR_MESSAGE,--'-' as TAG,
            rh.TASK_HISTORY_ID, rh.PROJECT_NAME,
            'MATILLION' AS SOURCE_TYPE,
            null as LINKS, null as TRIGGER_BY
            FROM EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY rh
            JOIN EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS_STG sd
            ON rh.PROJECT_NAME = sd.PROJECT AND
            rh.JOB_NAME = sd.JOB_NAME
            and run_date= (select max (run_date) from EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS) 
            WHERE enabled=true and
                rh.JOBTYPE in ('SCHEDULE ORCHESTRATION', 'RUN_ORCHESTRATION')  
                and END_TIME >= DATEADD (hour, -24, CURRENT_TIMESTAMP) ORDER BY END_TIME DESC
    ),
    mat_final_cte as (
        select * from cte_mat_temp where lower (SCHEDULE_NAME) not like '%weekend%' and lower (SCHEDULE_NAME) not like '%weekday%' 
        union
        select * from cte_mat_temp where lower (SCHEDULE_NAME) like '%weekend%' and DAYOFWEEK (START_TIME) in (0,6)
        union
        select * from cte_mat_temp where lower (SCHEDULE_NAME) like '%weekday%' and (DAYOFWEEK (START_TIME) in (1,2,3,4,5) )
    ),
    combined_union as (
        select UPPER (st.name) AS SCHEDULE_NAME, JOB_TAG_NAME,
        CONVERT_TIMEZONE ('UTC', 'America/Los_Angeles', min (di. RUN_STARTED_AT)) AS START_TIME,
        CONVERT_TIMEZONE ('UTC', 'America/Los_Angeles', max (di. RUN_COMPLETED_AT)) AS END_TIME, 
        TIMESTAMPDIFF('minute', START_TIME, END_TIME) AS TOTAL_RUNTIME_MINUTES, 
        di.job_run_id as TASK_HISTORY_ID, 
        upper(ds.STATUS) as STATUS, 
        ds.message as ERROR_MESSAGE,  
        di.project_name, 
        'DBT' AS SOURCE_TYPE,
        di.job_url as LINKS, 
        di.cause AS TRIGGER_BY
        FROM EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES st
        inner join EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS di
        on st.id=di.job_id inner join cte_dbt_status ds on di.job_run_id = ds.job_run_id
        WHERE --st.project_id='270340' and st.environment_id=218564 and
        di.RUN_COMPLETED_AT >= DATEADD (hour, -24, CURRENT_TIMESTAMP)
        group by di.job_run_id, JOB_TAG_NAME, di.project_name, st.name, STATUS, message, SOURCE_TYPE, LINKS, TRIGGER_BY 
        UNION ALL
        SELECT upper(SCHEDULE_NAME) SCHEDULE_NAME, JOB_TAG_NAME, START_TIME, END_TIME, TOTAL_RUNTIME_MINUTES,
            TASK_HISTORY_ID, STATUS, ERROR_MESSAGE,
            PROJECT_NAME, 'MATILLION' AS SOURCE_TYPE,
            null as LINKS, null as TRIGGER_BY
            FROM mat_final_cte
    )
    select
            distinct cd.*,
            cc.REVIEWED,
            CC.REVIEWER_NAME,
            LISTAGG(cc.comments, ', ') AS COMMENTS
    FROM
            combined_union cd
    LEFT JOIN
            EDW_LAB_DEV.OBSERVABILITY.JOBS_COMMENTS cc
    ON
            cd.TASK_HISTORY_ID = cc.run_id
    AND
            cd.SOURCE_TYPE= upper(cc.source_type) 

    GROUP BY ALL
    ORDER BY
            cd.END_TIME DESC;
    """
    
    created_dataframe = session.sql(query)
    df = created_dataframe.to_pandas()
    mat_api_df = mat_running_run_and_queue()
    df = pd.concat([df, mat_api_df], ignore_index=True)
    
    successful_jobs = df[df['STATUS'] == 'SUCCESS'].shape[0]
    failed_jobs = df[df['STATUS'] == 'FAILED'].shape[0]
    cancelled_jobs = df[df['STATUS'] == 'CANCELLED'].shape[0]
    run_count = df[df['STATUS'] == 'RUNNING'].shape[0]
    queue_count = df[df['STATUS'] == 'QUEUED'].shape[0]
    
    running_jobs, run_df = dbt_running_run_and_queue(3)
    queued_jobs, queue_df = dbt_running_run_and_queue(1)
    
    gmt_end_time = datetime.now(timezone.utc)
    gmt_start_time = datetime.now(timezone.utc) - timedelta(hours=24)
    failed_jobs_dbt, fail_df = dbt_failed_jobs(20, gmt_start_time, gmt_end_time)
    
    running_jobs += run_count
    queued_jobs += queue_count
    failed_jobs += failed_jobs_dbt
    
    return df, run_df, queue_df, fail_df, successful_jobs, failed_jobs, cancelled_jobs, running_jobs, queued_jobs


@st.fragment
def render_schedule_status_fragment():
    """Fragment: Schedule Status Overview - uses cached data, no filters."""
    # Load cached data once
    if 'schedule_status_data' not in st.session_state:
        with st.spinner('Loading schedule status...'):
            st.session_state['schedule_status_data'] = fetch_schedule_status_data()
    
    cached = st.session_state['schedule_status_data']
    df, run_df, queue_df, fail_df, successful_jobs, failed_jobs, cancelled_jobs, running_jobs, queued_jobs = cached
    
    with st.container(border=False):
        # header_col1,divider, header_col2 = st.columns([1,0.05, 1])
        st.markdown(
            f"""
        <div class="dashboard-box">
             <div>
                 <h1 class="hover-effect">
                    Schedule Status Overview (24hrs)
                 </h1>
            </div>
            <div class="status-card-container">
                <div class="status-card status-success"><p>Success</p><h2 id="success">{successful_jobs}</h2></div>
                <div class="status-card status-failed"><p>Failed</p><h2 id="failed">{failed_jobs}</h2></div>
                <div class="status-card status-cancelled"><p>Cancelled</p><h2 id="cancelled">{cancelled_jobs}</h2></div>
                <div class="status-card status-running"><p>Running</p><h2 id="running">{running_jobs}</h2></div>
                <div class="status-card status-queue"><p>Queue</p><h2 id="queue">{queued_jobs}</h2></div>
            </div>
        </div>
        """,
            unsafe_allow_html=True,
        )
        st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">',unsafe_allow_html=True)
    
    return df, run_df, queue_df, fail_df


def fetch_jobs_status_data(df_input, run_df_input, queue_df_input, fail_df_input):
    """Assembly of combined job dataframe (fresh on each reload)."""
    return df_input, run_df_input, queue_df_input, fail_df_input


@st.fragment
def render_jobs_status_fragment(df, run_df, queue_df, fail_df):
    """Fragment: Jobs Status with session_state filters - only filtered table re-renders on change."""
    # Initialize session state for filters
    if 'jobs_filters' not in st.session_state:
        st.session_state['jobs_filters'] = {
            'schedule': 'ALL',
            'source_type': 'ALL',
            'status': 'ALL',
        }
    
    schedules_options = ["ALL"] + list(df["SCHEDULE_NAME"].unique())
    source_type_options = ["ALL"] + list(df["SOURCE_TYPE"].unique())
    status_options = ["ALL","SUCCESS","FAILED","CANCELLED","RUNNING","QUEUED"]
    
    with st.container(border=False):
        st.markdown(
            """
            <div>
                 <div>
                    <h1 class="hover-effect">
                      Jobs Status
                 </h1>
                 </div>
            
            </div>
            """,
            unsafe_allow_html=True,
        )
        
        # Callback for filter changes
        def _update_jobs_filters():
            st.session_state['jobs_filters'] = {
                'schedule': 'ALL' if st.session_state.get('jobs_schedule_sel') == 'Schedule' else st.session_state.get('jobs_schedule_sel', 'ALL'),
                'source_type': 'ALL' if st.session_state.get('jobs_source_sel') == 'Source Type' else st.session_state.get('jobs_source_sel', 'ALL'),
                'status': 'ALL' if st.session_state.get('jobs_status_sel') == 'Status' else st.session_state.get('jobs_status_sel', 'ALL'),
            }

        filter_col1, filter_col2 = st.columns([2, 2])

        with filter_col1:
            f1, f2, f3 = st.columns(3)
            with f1:
                schedules_options_placeholder = ["Schedule"] + schedules_options[1:]
                st.selectbox("", options=schedules_options_placeholder, index=0, key='jobs_schedule_sel', on_change=_update_jobs_filters)
            with f2:
                source_type_options_placeholder = ["Source Type"] + source_type_options[1:]
                st.selectbox("", options=source_type_options_placeholder, index=0, key='jobs_source_sel', on_change=_update_jobs_filters)
            with f3:
                status_options_placeholder = ["Status"] + status_options[1:]
                st.selectbox("", options=status_options_placeholder, index=0, key='jobs_status_sel', on_change=_update_jobs_filters)

        # Use filters from session state
        schedule_filter_val = st.session_state['jobs_filters']['schedule']
        source_type_filter_val = st.session_state['jobs_filters']['source_type']
        status_filter_val = st.session_state['jobs_filters']['status']

        columns_to_insert = ["PROJECT_NAME","SCHEDULE_NAME","START_TIME","END_TIME","ERROR_MESSAGE","STATUS","SOURCE_TYPE","LINKS","JOB_TAG_NAME","TRIGGER_BY","TASK_HISTORY_ID"]
        fail_col_to_insert = columns_to_insert + ["REVIEWED","REVIEWER_NAME","COMMENTS"]

        filtered_merged_df = run_df[columns_to_insert]
        filtered_queue_df = queue_df[columns_to_insert]
        final_fail_df = fail_df[fail_col_to_insert]
        concatenated_df_temp = pd.concat([filtered_merged_df, filtered_queue_df], ignore_index=True)
        concatenated_df = pd.concat([final_fail_df, concatenated_df_temp], ignore_index=True)
        
        df_combined = pd.concat([df, concatenated_df], ignore_index=True)
        df_combined.loc[df_combined["STATUS"].str.contains("FAILEDIERROR", case=False), "STATUS"] = "FAILED"
        df_combined["LINKS"] = df_combined.apply(lambda row: f"https://matillion-prod.corp.zscaler.com/#ZSCALER_BI/ZSCALER_BI_DWH/default/{row['SCHEDULE_NAME']}/run/{row['TASK_HISTORY_ID']}" if row["SOURCE_TYPE"] == "MATILLION" else row["LINKS"], axis=1)

        # Apply filters locally (no SQL re-execution)
        if status_filter_val == "FAILED":
            df_filtered = df_combined[((df_combined["SOURCE_TYPE"] == source_type_filter_val)| (source_type_filter_val == "ALL")) & ((df_combined["SCHEDULE_NAME"] == schedule_filter_val)| (schedule_filter_val == "ALL")) & ((df_combined["STATUS"] == "FAILED")|(status_filter_val == "ALL"))]
        else:
            df_filtered = df_combined[((df_combined["SOURCE_TYPE"] == source_type_filter_val) | (source_type_filter_val == "ALL")) & ((df_combined["SCHEDULE_NAME"] == schedule_filter_val) | (schedule_filter_val == "ALL")) & ((df_combined["STATUS"] == status_filter_val) | (status_filter_val == "ALL"))]

        df_filtered = df_filtered.copy()
        # Remove timezone info and convert to datetime
        df_filtered["START_TIME"] = pd.to_datetime(df_filtered["START_TIME"], utc=True).dt.tz_localize(None).dt.strftime("%B %d, %Y at %I:%M %p")
        df_filtered["END_TIME"] = pd.to_datetime(df_filtered["END_TIME"], utc=True).dt.tz_localize(None).dt.strftime("%B %d, %Y at %I:%M %p")

        column_config1 = {
            "PROJECT_NAME": st.column_config.Column(" PROJECT", help="PROJECT NAME", width="medium"),
            "SCHEDULE_NAME": st.column_config.Column(" SCHEDULE", help="SCHEDULE NAME", width="medium"),
            "JOB_TAG_NAME": st.column_config.Column(" JOB/TAG ", help="JOB/TAG NAME", width="medium"),
            "START_TIME": st.column_config.Column(" START TIME (PST)", help="START TIME (PST)", width="medium"),
            "END_TIME": st.column_config.Column(" END TIME (PST)", help="END TIME (PST)", width="medium"),
            "ERROR_MESSAGE": st.column_config.Column(" ERROR MESSAGE", help="ERROR MESSAGE", width="medium"),
            "TOTAL_RUNTIME_MINUTES": st.column_config.Column(" TOTAL RUNTIME (MIN)", help="TOTAL RUNTIME (MINS)", width="medium"),
            "TASK_HISTORY_ID": st.column_config.Column(" TASK HISTORY ID", help="TASK HISTORY ID", width="medium"),
            "TRIGGER_BY": st.column_config.Column(" TRIGGERED BY", help="TRIGGER BY", width="medium"),
            "SOURCE_TYPE": st.column_config.Column(" SOURCE TYPE", help="SOURCE TYPE", width="medium"),
            "LINKS": st.column_config.LinkColumn(" LINKS", help="LINKS", width="medium"),
            "REVIEWER_NAME": st.column_config.Column(" REVIEWER ", help="REVIEWER NAME", width="medium"),
        }

        def color_status(val):
            color_map = {"SUCCESS": "#5b85fb","FAILED": "#f26271","CANCELLED": "#feb746","RUNNING": "#61d7a1","QUEUED": "#fee8c6"}
            return f'background-color: {color_map.get(val, "white")}; font-weight:bold; border-radius: 5px; font-family: Inter, sans-serif;'

        if "TOTAL_RUNTIME_MINUTES" in df_filtered.columns:
            df_filtered["TOTAL_RUNTIME_MINUTES"] = df_filtered["TOTAL_RUNTIME_MINUTES"].astype(float).fillna(0).astype(int)

        df_styled = df_filtered.style.applymap(color_status, subset=["STATUS"]).set_table_styles([
            {"selector": "th","props": [("background-color", "#7fcbf0"),("color", "#000"),("font-size", "12px"),("font-family", "Inter, sans-serif"),("text-align", "center"),],},
            {"selector": "td","props": [("background-color", "#7fcbf0"),("font-family", "Inter, sans-serif"),("font-size", "13px"),("color", "#000"),],},
        ])

        st.dataframe(df_styled, column_config=column_config1, use_container_width=True, hide_index=True)
        st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">',unsafe_allow_html=True)
    
    return df_combined


def fetch_longest_execution_data(df_input):
    """Assembly of success jobs data (fresh on each reload)."""
    return df_input[df_input["STATUS"] == "SUCCESS"].copy()


@st.fragment
def render_longest_execution_fragment(df):
    """Fragment: Longest Execution Time Jobs - uses session_state for top_n filter."""
    # Initialize session state for filter
    if 'longest_exec_top_n' not in st.session_state:
        st.session_state['longest_exec_top_n'] = 5

    with st.container(border=False):
        st.markdown(
            """
            <div>
                <div>
                    <h1 class="hover-effect">
                    Longest Execution Time Jobs
                </h1>
                </div>
            
            </div>
            """,
            unsafe_allow_html=True,
        )
        
        # Callback for top_n changes
        def _update_longest_exec():
            st.session_state['longest_exec_top_n'] = st.session_state.get('longest_exec_sel', 5)
        
        df_success = df[df["STATUS"] == "SUCCESS"].copy()
        df_success["TOTAL_RUNTIME_MINUTES"] = pd.to_numeric(df_success["TOTAL_RUNTIME_MINUTES"], errors="coerce")

        col1, col2 = st.columns([2, 4])
        with col1:
            top_n = st.selectbox(
                "Select number of top jobs to display",
                options=[1, 5, 10, 15, 20, 25, 30, 40, 50],
                index=1,
                key='longest_exec_sel',
                on_change=_update_longest_exec
            )

        # Use filter from session state (re-render with new top_n)
        top_n_val = st.session_state.get('longest_exec_top_n', 5)
        top_runs = df_success.sort_values("TOTAL_RUNTIME_MINUTES", ascending=False).head(top_n_val).copy()
        top_runs["RANK"] = range(1, len(top_runs) + 1)
        top_runs["LABEL"] = top_runs["RANK"].astype(str) + ". " + top_runs["SCHEDULE_NAME"].astype(str).str.slice(0, 25) + "..."

        fig = px.bar(top_runs, x="LABEL", y="TOTAL_RUNTIME_MINUTES", color="SOURCE_TYPE", text="TOTAL_RUNTIME_MINUTES",
                     hover_data={"RANK": True, "SCHEDULE_NAME": True, "JOB_TAG_NAME": True, "SOURCE_TYPE": True, "TOTAL_RUNTIME_MINUTES": ":.2f",},)
        fig.update_traces(textposition="outside", texttemplate="%{text:.2f} min", marker_line=dict(width=1.2, color="darkgrey"), hovertemplate="<b>Rank:</b> %{customdata[0]}<br><b>Schedule:</b> %{customdata[1]}<br><b>Source:</b> %{customdata[3]}<br><b>Job/Tag:</b> %{customdata[2]}<br><b>Runtime:</b> %{y:.2f} min<br><extra></extra>")
        fig.update_layout(hovermode="x unified", hoverlabel=dict(bgcolor="white", font_size=12, font_family="'Inter', sans-serif"), margin=dict(l=40, r=40, t=70, b=200), xaxis_title="Job Runs", yaxis_title="Runtime (Minutes)", xaxis=dict(tickangle=45, tickfont=dict(size=10, color="Black"),), plot_bgcolor="rgba(245,246,250,1)", paper_bgcolor="rgba(245,246,250,1)", bargap=0.3,)

        st.plotly_chart(fig, use_container_width=True)
        st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">', unsafe_allow_html=True)


def create_df_charts():


    # Call the three independent fragments
    df, run_df, queue_df, fail_df = render_schedule_status_fragment()
    df = render_jobs_status_fragment(df, run_df, queue_df, fail_df)
    render_longest_execution_fragment(df)


def jobs_status_data():
    task_query = """
        SELECT distinct * FROM EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_DETAILS;
    """
    session = get_active_session()
    created_task_dataframe = session.sql(task_query)
    df = created_task_dataframe.to_pandas()
    jobs_options = ["ALL"] + list(df["JOB_NAME"].unique())
    # source_type_options= ['ALL']+ list (df ['SOURCE_TYPE'].unique())
    state_options = [
        "ALL",
        "SUCCESS",
        "FAILED",
        "CANCELLED",
        "RUNNING",
        "QUEUED",
    ]  # + [status for status in df['STATUS'].unique() if status != 'ERROR']

    with st.container(border=False):
        st.markdown(
            """
            <h1 class="hover-effect">
               Jobs Status Data
            </h1>
            <hr style="border: 1px solid black; width: 0%; margin-left: auto; margin-right: auto; margin-top: -10px; margin-bottom: 5px;">
            """,
            unsafe_allow_html=True,
        )

        filter_col1, filter_col2 = st.columns([2, 2])

        with filter_col1:
            f1, f2 = st.columns([1, 1])
            with f1:
                jobs_options = ["Job"] + list(df["JOB_NAME"].unique())
                jobs_filter = st.selectbox("", options=jobs_options, index=0)
            with f2:
                state_options = [
                    "Status",
                    "ALL",
                    "SUCCESS",
                    "FAILED",
                    "CANCELLED",
                    "RUNNING",
                    "QUEUED",
                ]
                state_filter = st.selectbox("", options=state_options, index=0)
                    
                
        column_config_jobs = {
            "taskID": st.column_config.Column(
                "TASK ID", help="PROJECT NAME", width="medium"
            ),
            "SCHEDULE_NAME": st.column_config.Column(
                "SCHEDULE NAME", help="SCHEDULE NAME", width="medium"
            ),
            "JOB_TAG_NAME": st.column_config.Column(
                "JOB/TAG NAME", help="JOB/TAG NAME", width="medium"
            ),
            "START_TIME": st.column_config.Column(
                "START TIME (PST)", help="START TIME (PST)", width="medium"
            ),
            "END_TIME": st.column_config.Column(
                "END TIME (PST)", help="END TIME (PST)", width="medium"
            ),
            "ERROR_MESSAGE": st.column_config.Column(
                "ERROR MESSAGE", help="ERROR MESSAGE", width="medium"
            ),
            # "TOTAL_RUNTIME_MINUTES": st.column_config.Column(
            #     "TOTAL RUNTIME (MIN)",
            #     help="TOTAL RUNTIME (MINS)",
            #     width="medium"
            # # ),
            # "TASK_HISTORY_ID": st.column_config.Column(
            #     "TASK HISTORY ID",
            #     help="TASK HISTORY ID",
            #     width="medium"
            # ),
            # "TRIGGER_BY": st.column_config.Column(
            #     "TRIGGER BY",
            #     help="TRIGGER BY",
            #     width="medium"
            # ),
            "TYPE": st.column_config.Column(
                "SOURCE TYPE", help="SOURCE TYPE", width="medium"
            ),
            # "LINKS": st.column_config.LinkColumn (
            #     "LINKS",
            #     help="LINKS",
            #     width="medium",
            # )
        }

        def color_survived(val):
            if val == "SUCCESS":
                color = "#5b85fb"
            elif val == "FAILED":
                color = "#f26271"
            elif val == "CANCELLED":
                color = "#feb746"
            elif val == "RUNNING":
                color = "#61d7a1"
            elif val == "QUEUED":
                color = "#fee8c6"
            else:
                color = "white"
            return f"background-color: {color}"

        df = df.style.applymap(color_survived, subset=["STATE"])
        st.dataframe(df, column_config=column_config_jobs, hide_index=True)
        # edited_df

        st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">',unsafe_allow_html=True)
        # st.markdown("", unsafe_allow_html=True)
        

def fetch_schedule_lag_data():
    """Loader for schedule lag comparison data (fresh on each reload)."""
    sch_lag_query='''

    SELECT
        subquery.SCHEDULE_NAME,
        rs.ENQUEUED_TIME_PST AS EXPECTED_START_TIME,
        rs.START_TIME_PST AS ACTUAL_START_TIME,
        DATEDIFF ('minute', rs.ENQUEUED_TIME_PST, rs.START_TIME_PST) AS LAG_TIME_MINUTES, 
        'MATILLION' AS SOURCE_TYPE,
        rs.PROJECT_NAME
    FROM
        (SELECT DISTINCT upper (sd.NAME) AS SCHEDULE_NAME, sd. PROJECT 
         FROM EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS sd
         WHERE sd.ENABLED = 'TRUE') subquery
    JOIN
        EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY rs 
        ON rs.PROJECT_NAME = subquery.PROJECT 
        AND rs.JOB_NAME= subquery.SCHEDULE_NAME
    WHERE
        rs.STATE = 'SUCCESS' and LAG_TIME_MINUTES>1
        AND rs.START_TIME_PST >= DATEADD (hour, -24, CURRENT_TIMESTAMP)
    UNION ALL

    SELECT
        DISTINCT
        upper(sdt.NAME) AS SCHEDULE_NAME,
        rr.COMPILE_STARTED_AT AS EXPECTED_START_TIME, 
        rr.EXECUTE_STARTED_AT AS ACTUAL_START_TIME,
        DATEDIFF('minute', COMPILE_STARTED_AT, EXECUTE_STARTED_AT) AS LAG_TIME_MINUTES, 
        UPPER(sdt.SOURCE_TYPE) SOURCE_TYPE,
        di.PROJECT_NAME,
    FROM 
        EDW_LAB_DEV.OBSERVABILITY.DBT_RUN_RESULTS rr
    inner join 
        EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS di
    on 
        di.invocation_id=rr.invocation_id
    inner join 
        EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES sdt
    ON 
        sdt.id=di.job_id
    WHERE 
        sdt.project_id='270340' and sdt.environment_id=218564 and LAG_TIME_MINUTES>1 and
        di.env='prod' and di. cause_category='scheduled' and rr.resource_type in ('model', 'snapshot', 'seed') and 
        EXECUTE_STARTED_AT >= DATEADD (hour, -24, CURRENT_TIMESTAMP)
    ORDER BY
        PROJECT_NAME, ACTUAL_START_TIME DESC;
    '''

    return session.sql(sch_lag_query).to_pandas()


@st.fragment
def schedule_lag_df():
    """Fragment: Schedule Lag Comparison - uses session_state for source_type filter."""
    # Initialize session state for filter
    if 'lag_source_type' not in st.session_state:
        st.session_state['lag_source_type'] = 'ALL'
    
    # Load cached data once
    if 'schedule_lag_data' not in st.session_state:
        with st.spinner('Loading schedule lag data...'):
            st.session_state['schedule_lag_data'] = fetch_schedule_lag_data()
    
    schedule_df = st.session_state['schedule_lag_data']
    
    with st.container(border=False):
        st.markdown(
            """
            <h1 class="hover-effect">
               Schedule Lag Comparison (Current vs Previous Run)
            </h1>
            """,
            unsafe_allow_html=True,
        )
        
        # Callback for filter changes
        def _update_lag_filter():
            st.session_state['lag_source_type'] = st.session_state.get('lag_source_sel', 'ALL')
        
        filter_col1, filter_col2 = st.columns([2, 2])
        with filter_col1:
            source_type_filter = st.selectbox(
                "Source Type", ["ALL", "Matillion", "DBT"], key='lag_source_sel', on_change=_update_lag_filter
            )

        # Use filter from session state (apply locally)
        source_type_val = st.session_state.get('lag_source_type', 'ALL')
        filter_sch = schedule_df[(schedule_df["SOURCE_TYPE"] == source_type_val) | (source_type_val == "ALL")]
        
        column_config1 = {
            "SCHEDULE_NAME": st.column_config.Column("SCHEDULE NAME", help="SCHEDULE NAME", width="medium"),
            "EXPECTED_START_TIME": st.column_config.Column("EXPECTED START TIME (PST)", help="EXPECTED START TIME (PST)", width="medium"),
            "ACTUAL_START_TIME": st.column_config.Column("ACTUAL START TIME (PST)", help="ACTUAL START TIME (PST)", width="medium"),
            "LAG_TIME_MINUTES": st.column_config.Column("LAG TIME MINUTES", help="LAG TIME MINUTES", width="medium"),
            "SOURCE_TYPE": st.column_config.Column("SOURCE TYPE", help="SOURCE TYPE", width="medium"),
            "PROJECT_NAME": st.column_config.Column("PROJECT NAME", help="PROJECT NAME", width="medium"),
        }
        st.dataframe(filter_sch, hide_index=True, column_config=column_config1, use_container_width=True)
        st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">',unsafe_allow_html=True)
def fetch_long_run_data():
    """Loader for long run comparison data (fresh on each reload)."""
    avg7_lag_query="""
         WITH ScheduleRunData AS (
            SELECT
                rh. PROJECT_NAME,
                sd.name as SCHEDULE_NAME,
                rh. JOB_NAME,
                rh.START_TIME_PST,
                rh.END_TIME_PST,
                TIMESTAMPDIFF('second', rh. START_TIME_PST, rh. END_TIME_PST) AS RUN_DURATION_SECONDS, 
                ROW_NUMBER() OVER (PARTITION BY SCHEDULE_NAME ORDER BY rh. END_TIME_PST DESC) AS RUN_ORDER
            FROM
                EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY rh
            JOIN
                EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS sd
            ON
                rh.PROJECT_NAME = sd.PROJECT
                AND rh.JOB_NAME = sd.JOB_NAME 
            WHERE
                sd.ENABLED = 'TRUE' and (day_of_week=true or days_of_month is not null) and rh. STATE='SUCCESS' and rh.JOBTYPE = 'SCHEDULE ORCHESTRATION' and 
                rh.START_TIME_PST IS NOT NULL 
                AND rh.END_TIME_PST IS NOT NULL
        ),
        
        LastSevenRuns AS (
            SELECT
                PROJECT_NAME,
                SCHEDULE_NAME,
                AVG (RUN_DURATION_SECONDS)/60.0 AS AVG_LAST_7_RUNS_MINUTES
            FROM
                ScheduleRunData
            WHERE
                RUN_ORDER <= 7
            GROUP BY
                PROJECT_NAME, SCHEDULE_NAME
        ),

        LatestRun AS (
            SELECT
                PROJECT_NAME,
                SCHEDULE_NAME,
                RUN_DURATION_SECONDS/60.0 AS LATEST_RUN_MINUTES
            FROM
                ScheduleRunData
            WHERE
                RUN_ORDER = 1
        ),

        DBTScheduleRunData AS (
            SELECT
                sdt.SOURCE_TYPE,
                di.PROJECT_NAME,
                sdt.NAME AS SCHEDULE_NAME,
                rr.COMPILE_STARTED_AT AS EXPECTED_START_TIME,
                rr.EXECUTE_STARTED_AT AS ACTUAL_START_TIME,
                DATEDIFF ('minute', rr.COMPILE_STARTED_AT, rr.EXECUTE_STARTED_AT) AS RUN_DURATION_MINUTES,
                ROW_NUMBER() OVER (PARTITION BY di.PROJECT_NAME, sdt.NAME ORDER BY rr.EXECUTE_STARTED_AT DESC) AS RUN_ORDER
            FROM
                EDW_LAB_DEV.OBSERVABILITY.DBT_RUN_RESULTS rr
            INNER JOIN
                EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS di
            ON di.invocation_id = rr.invocation_id
            INNER JOIN
                EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES sdt
            ON sdt.id = di.job_id
            WHERE
                sdt.project_id = '270340'
                AND sdt.environment_id = 218564
                AND di.env= 'prod'
                AND di.cause_category= 'scheduled'
                AND rr.resource_type IN ('model', 'snapshot', 'seed')
        ),

        DBTLastSevenRuns AS (
            SELECT
                PROJECT_NAME,
                SCHEDULE_NAME,
                AVG(RUN_DURATION_MINUTES) AS AVG_LAST_7_RUNS_MINUTES
            FROM
                DBTScheduleRunData
            WHERE
                RUN_ORDER <= 7
            GROUP BY
                PROJECT_NAME, SCHEDULE_NAME
        ),

        DBTLatestRun AS (
            SELECT
                PROJECT_NAME,
                SCHEDULE_NAME,
                RUN_DURATION_MINUTES AS LATEST_RUN_MINUTES
            FROM
                DBTScheduleRunData
            WHERE
                RUN_ORDER = 1 and LATEST_RUN_MINUTES is not NULL
        )

    SELECT
        upper(lr.SCHEDULE_NAME) SCHEDULE_NAME,
        sr.AVG_LAST_7_RUNS_MINUTES,
        lr.LATEST_RUN_MINUTES,
        lr.LATEST_RUN_MINUTES - sr. AVG_LAST_7_RUNS_MINUTES AS TIME_DIFFERENCE_MINUTES, 
        'MATILLION' as SOURCE_TYPE,
        lr.PROJECT_NAME,
    FROM
        LatestRun lr
    JOIN
        LastSevenRuns sr
    ON
        lr. PROJECT_NAME = sr. PROJECT_NAME
        AND lr.SCHEDULE_NAME = sr.SCHEDULE_NAME
    HAVING TIME_DIFFERENCE_MINUTES >=5

    UNION ALL

    SELECT
        upper (sr.SCHEDULE_NAME) SCHEDULE_NAME,
        sr.AVG_LAST_7_RUNS_MINUTES,
        lr.LATEST_RUN_MINUTES,
        (lr.LATEST_RUN_MINUTES - sr.AVG_LAST_7_RUNS_MINUTES) AS TIME_DIFFERENCE_MINUTES, 
        'DBT' as SOURCE_TYPE,
        sr.PROJECT_NAME,
    FROM
        DBTLastSevenRuns sr
    JOIN
        DBTLatestRun lr
    ON
        sr.PROJECT_NAME = lr.PROJECT_NAME
        AND sr.SCHEDULE_NAME = lr.SCHEDULE_NAME 
    HAVING TIME_DIFFERENCE_MINUTES >=5
    ORDER BY
        PROJECT_NAME, SCHEDULE_NAME;
    """
    return session.sql(avg7_lag_query).to_pandas()


@st.fragment
def avg_schedule_lag():
    """Fragment: Long Run Comparison - no filters, cached data."""
    # Load cached data once
    if 'long_run_data' not in st.session_state:
        with st.spinner('Loading long run comparison data...'):
            st.session_state['long_run_data'] = fetch_long_run_data()
    
    avg7_schedule_df = st.session_state['long_run_data']

    with st.container(border=False):
        st.markdown(
            """
            <h1 class="hover-effect">
               Long Run Comparison (Current vs Previous Run)
            </h1>
            """,
            unsafe_allow_html=True,
        )
        filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([2, 2, 2, 2])
        with filter_col4:
            st.markdown(
                """
                <div style="text-align: right;">
                    <p style="color: blue; font-size: 14px;font-family: 'Inter', sans-serif; margin: 0;"> NOTE: 5 min Buffer
                    </p> 
                </div>
                """,
                unsafe_allow_html=True,
            )

        column_config1 = {
            "SCHEDULE_NAME": st.column_config.Column("SCHEDULE NAME", help="SCHEDULE NAME", width="medium"),
            "AVG_LAST_7_RUNS_MINUTES": st.column_config.Column("AVG LAST 7 RUNS MINUTES", help="AVG LAST 7 RUNS MINUTES", width="medium"),
            "LATEST_RUN_MINUTES": st.column_config.Column("LATEST RUN MINUTES", help="LATEST RUN MINUTES", width="medium"),
            "TIME_DIFFERENCE_MINUTES": st.column_config.Column("TIME DIFFERENCE MINUTES", help="TIME DIFFERENCE MINUTES", width="medium"),
            "SOURCE_TYPE": st.column_config.Column("SOURCE TYPE", help="SOURCE TYPE", width="medium"),
            "PROJECT_NAME": st.column_config.Column("PROJECT NAME", help="PROJECT NAME", width="medium"),
        }
        st.dataframe(avg7_schedule_df, column_config=column_config1, use_container_width=True, hide_index=True)
        st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">',unsafe_allow_html=True)

def fetch_upcoming_idle_data():
    """Fetch upcoming schedules (Matillion + DBT) and idle time, without filtering timestamps."""

    import pandas as pd

    # --- Matillion schedules ---
    try:
        mat_df = session.sql("""
            SELECT
                NAME AS SCHEDULE_NAME,
                NULL AS RUN_TIME,
                'MATILLION' AS SOURCE
            FROM EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS_STG
        """).to_pandas()
    except Exception as e:
        st.error(f"Error fetching Matillion schedules: {e}")
        mat_df = pd.DataFrame(columns=["SCHEDULE_NAME", "RUN_TIME", "SOURCE"])

    # --- DBT schedules ---
    try:
        dbt_df = session.sql("""
            SELECT
                NAME AS SCHEDULE_NAME,
                NULL AS RUN_TIME,
                'DBT' AS SOURCE
            FROM EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES
        """).to_pandas()
    except Exception as e:
        st.error(f"Error fetching DBT schedules: {e}")
        dbt_df = pd.DataFrame(columns=["SCHEDULE_NAME", "RUN_TIME", "SOURCE"])

    # --- Combine both ---
    combined_df = pd.concat([mat_df, dbt_df], ignore_index=True)

    # --- Idle time ---
    try:
        idle_df = session.sql("""
            SELECT
                Last_end_time AS FROM_DATETIME,
                next_start_time AS TO_DATETIME,
                ROUND(TIMESTAMPDIFF('minute', Last_end_time, next_start_time), 2) AS IDLE_TIME_MINUTES
            FROM (
                SELECT
                    END_TIME_PST AS last_end_time,
                    LEAD(END_TIME_PST) OVER (ORDER BY END_TIME_PST) AS next_start_time,
                    TIMESTAMPDIFF('second', END_TIME_PST, LEAD(END_TIME_PST) OVER (ORDER BY END_TIME_PST)) AS idle_seconds
                FROM EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY
                WHERE END_TIME_PST >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
                    AND STATE='SUCCESS'
            )
            WHERE next_start_time IS NOT NULL
            ORDER BY Last_end_time DESC
        """).to_pandas()
    except Exception as e:
        st.error(f"Error fetching idle time: {e}")
        idle_df = pd.DataFrame(columns=["FROM_DATETIME", "TO_DATETIME", "IDLE_TIME_MINUTES"])

    return combined_df, idle_df


# def debug_schedule_tables():
#     """Debug function to show what's actually in the schedule tables."""
#     with st.expander("🔍 DEBUG: Matillion & DBT Schedule Tables"):
#         col1, col2 = st.columns(2)
        
#         with col1:
#             st.subheader("Matillion Schedules Details (Raw)")
#             try:
#                 mat_debug = '''
#                 SELECT 
#                     name, 
#                     enabled, 
#                     run_date,
#                     hour,
#                     minute,
#                     day_of_week,
#                     days_of_month,
#                     PROJECT
#                 FROM EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS_STG
#                 ORDER BY name ASC
#                 LIMIT 20;
#                 '''
#                 mat_df = session.sql(mat_debug).to_pandas()
#                 st.write(f"**Total records: {len(mat_df)}**")
#                 st.dataframe(mat_df, use_container_width=True)
#             except Exception as e:
#                 st.error(f"Error: {str(e)}")
        
#         with col2:
#             st.subheader("DBT Schedules (Raw)")
#             try:
#                 dbt_debug = '''
#                 SELECT 
#                     NAME,
#                     NEXT_RUN,
#                     JOB_TYPE,
#                     SOURCE_TYPE,
#                     PROJECT_ID
#                 FROM EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES
#                 ORDER BY NAME ASC
#                 LIMIT 20;
#                 '''
#                 dbt_df = session.sql(dbt_debug).to_pandas()
#                 st.write(f"**Total records: {len(dbt_df)}**")
#                 st.dataframe(dbt_df, use_container_width=True)
#             except Exception as e:
#                 st.error(f"Error: {str(e)}")
        
#         # Show what the flattened query returns
#         st.subheader("Matillion - Flattened (from hour split)")
#         try:
#             flatten_test = '''
#             SELECT
#                 sd.name AS SCHEDULE_NAME,
#                 sd.run_date,
#                 sd.minute,
#                 sd.hour,
#                 C.value::int AS RUN_HOUR,
#                 TO_TIMESTAMP_LTZ(sd.run_date ||' '||LPAD(C.value, 2, '0') ||':' || LPAD(sd.minute, 2, '0') || ':00', 'YYYY-MM-DD HH24:MI:SS') AS START_RUN_TIME_UTC,
#                 CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP_LTZ(sd.run_date ||' '||LPAD(C.value, 2, '0') ||':' || LPAD(sd.minute, 2, '0') || ':00', 'YYYY-MM-DD HH24:MI:SS')) AS START_RUN_TIME
#             FROM
#                 EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS_TEMP sd,
#                 LATERAL FLATTEN(input => SPLIT(sd.hour, ',')) C
#             WHERE
#                 sd.enabled = 'TRUE'
#             ORDER BY sd.name
#             LIMIT 50;
#             '''
#             flatten_df = session.sql(flatten_test).to_pandas()
#             st.write(f"**Total flattened records: {len(flatten_df)}**")
#             st.write(f"**Current timestamp (for comparison):** {pd.Timestamp.now(tz='UTC').tz_convert('America/Los_Angeles')}")
#             st.dataframe(flatten_df, use_container_width=True)
#         except Exception as e:
#             st.error(f"Error: {str(e)}")


@st.fragment
def upcoming_sch_idle_time():

    # st.write("DEBUG: fragment executed")

    if "combined_schedules" not in st.session_state:
        with st.spinner("Loading upcoming schedules..."):
            # Matillion: fetch schedules with calculation logic
            matillion_sql = """
            SELECT
                upper(NAME) AS SCHEDULE_NAME,
                ENABLED,
                HOUR,
                MINUTE,
                RUN_DATE
            FROM EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS_STG
            """

            matillion_df = session.sql(matillion_sql).to_pandas()
            
            # Calculate NEXT_RUN_TIME in Python
            def calculate_next_run(row):
                try:
                    # Check if enabled and has valid hour/minute
                    if str(row['ENABLED']).lower() != 'true':
                        return None
                    
                    # Try to extract hour and minute (handle comma-separated values)
                    hour_str = str(row['HOUR']).strip()
                    minute_str = str(row['MINUTE']).strip()
                    
                    # If multiple values (comma-separated), take the first one
                    if ',' in hour_str:
                        hour_str = hour_str.split(',')[0].strip()
                    if ',' in minute_str:
                        minute_str = minute_str.split(',')[0].strip()
                    
                    # Try to convert to numbers
                    try:
                        hour = int(hour_str)
                        minute = int(minute_str)
                    except (ValueError, TypeError):
                        return None
                    
                    # Validate ranges
                    if not (0 <= hour <= 23 and 0 <= minute <= 59):
                        return None
                    
                    # Calculate next run time
                    from datetime import datetime, timedelta
                    now = datetime.now()
                    next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    
                    # If that time has already passed today, schedule for tomorrow
                    if next_run <= now:
                        next_run = next_run + timedelta(days=1)
                    
                    return next_run
                except Exception:
                    return None
            
            matillion_df['RUN_TIME'] = matillion_df.apply(calculate_next_run, axis=1)
            matillion_df['SOURCE'] = 'MATILLION'
            
            # Keep only necessary columns
            combined_df = matillion_df[['SCHEDULE_NAME', 'RUN_TIME', 'SOURCE']].copy()
            combined_df = combined_df.sort_values("SCHEDULE_NAME")

            st.session_state["combined_schedules"] = combined_df

    combined_df = st.session_state["combined_schedules"]

    st.markdown(
        """
        <div style="text-align: left;">
            <h1 class="hover-effect">
                Upcoming Schedules (Matillion)
            </h1>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if combined_df.empty:
        st.info("No upcoming schedules found.")
        return

    # Filter out schedules with no run time
    combined_df_filtered = combined_df.dropna(subset=['RUN_TIME'])
    
    if combined_df_filtered.empty:
        st.warning("No schedules with valid run times found.")
        return

    column_config = {
        "SCHEDULE_NAME": st.column_config.Column(
            "SCHEDULE NAME", width="medium"
        ),
        "RUN_TIME": st.column_config.Column(
            "NEXT RUN TIME (PST)", width="medium"
        ),
        "SOURCE": st.column_config.Column(
            "SOURCE", width="small"
        ),
    }

    st.dataframe(
        combined_df_filtered,
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
    )


# @st.fragment
# def debug_matillion_history():
#     """Debug fragment: Show last 30 entries from RUN_HISTORY_SUMMARY"""
#     with st.container(border=True):
#         st.markdown(
#             """
#             <h2 style="color: #f26271;">
#                 🔍 DEBUG: Matillion Run History (Last 30 Entries)
#             </h2>
#             """,
#             unsafe_allow_html=True,
#         )
        
#         try:
#             debug_query = """
#             SELECT 
#                 TASK_HISTORY_ID,
#                 PROJECT_NAME,
#                 JOB_NAME,
#                 STATE,
#                 MESSAGE,
#                 START_TIME_PST,
#                 END_TIME_PST,
#                 ENQUEUED_TIME_PST,
#                 JOBTYPE
                
#             FROM EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY
#             ORDER BY START_TIME_PST DESC
#             LIMIT 30
#             """
            
#             debug_df = session.sql(debug_query).to_pandas()

#             if debug_df.empty:
#                 st.warning("No data in RUN_HISTORY_SUMMARY")
#             else:
#                 st.write(f"**Total records: {len(debug_df)}**")
#                 st.dataframe(debug_df, use_container_width=True, hide_index=True)
                
#         except Exception as e:
#             st.error(f"Error fetching debug data: {str(e)}")


#main
#main
# spinner_placeholder=st.empty()
# with st.container(border=False):
#     cl1, cl2, cl3 = st.columns ([4,1,1])
#     with cl3:
#         st.image("assets/logo_cloudeqs.png", width=200)


dbt_schedules_api()
create_df_charts ()
# jobs_status_data()
# spinner_placeholder.markdown ("<p style='color: #5D6A85;'>Loading lagging schedule data...</p>", unsafe_allow_html=True) 
schedule_lag_df ()
# spinner_placeholder.markdown ("<p style='color: #5D6A85;'>Loading matillion idle time data...</p>", unsafe_allow_html=True) 
avg_schedule_lag()
upcoming_sch_idle_time()
# debug_matillion_history()
# debug_schedule_tables()
# spinner_placeholder.markdown ("")
