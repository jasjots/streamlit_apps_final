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
    comments_df['TASK_HISTORY_ID'] = comments_df['TASK_HISTORY_ID'].astype(int)
        # Merge merged_df with comments data on TASK_HISTORY_ID
    mat_data = pd.merge (mat_data, comments_df, on='TASK_HISTORY_ID', how='left')
    # mat_data = pd.concat (mat_data, comments_df, on='TASK_HISTORY_ID', how='left')
    return mat_data

def create_df_charts(): 
    query ="""
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
        where UPPER (status) != 'ERROR' and di.env='prod' and di.cause_category in ('scheduled', 'other') and rr.resource_type in ('model', 'test', 'snapshot', 'seed') and 
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
            rh. "message" as ERROR_MESSAGE,--'-' as TAG,
            rh.TASK_HISTORY_ID, rh.PROJECT_NAME,
            'MATILLION' AS SOURCE_TYPE,
            null as LINKS, null as TRIGGER_BY
            FROM EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY rh
            JOIN EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS sd
            ON rh.PROJECT_NAME = sd.PROJECT AND
            rh.JOB_NAME = sd.JOB_NAME
            and run_date= (select max (run_date) from EDW_LAB_DEV.OBSERVABILITY.matillion_schedules_details) 
            WHERE enabled=true and
                rh.type in ('SCHEDULE ORCHESTRATION', 'QUEUE_ORCHESTRATION') --and rh.PROJECT_NAME = 'ZSCALER_BI_DWH' 
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
    JOIN
            EDW_LAB_DEV.OBSERVABILITY.JOBS_COMMENTS cc
    ON
            cd.TASK_HISTORY_ID = cc.run_id
    AND
            cd.SOURCE_TYPE= upper(cc.source_type) 

    GROUP BY ALL
    ORDER BY
            cd.END_TIME DESC;
    """

    task_query = """
        SELECT distinct * FROM EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_DETAILS;
    """
   # session = session

    created_dataframe = session.sql(query) 
    created_task_dataframe = session.sql(task_query)
    df=created_dataframe.to_pandas() 
    df_task = created_task_dataframe.to_pandas()
    mat_api_df=mat_running_run_and_queue()
    df = pd.concat([df, mat_api_df], ignore_index=True)
    # Calculate the number of successful and failed jobs in the left half
    successful_jobs = df [df['STATUS'] == 'SUCCESS'].shape[0]
    failed_jobs = df [df['STATUS']=='FAILED'].shape[0]
    # st.write(df [df ['STATUS'].str.contains("FAILED | ERROR", case=False)])
    cancelled_jobs = df [df['STATUS'] == 'CANCELLED'].shape[0]
    run_count=df[(df['STATUS'] == 'RUNNING')].shape[0]
    queue_count = df [(df['STATUS'] == 'QUEUED')].shape[0]

     # Calculate the number of successful and failed jobs in the left half
    successful_task_jobs = df_task [df_task['STATE'] == 'SUCCESS'].shape[0]
    failed_task_jobs = df_task [df_task['STATE']=='FAILED'].shape[0]
    # st.write(df [df ['STATUS'].str.contains("FAILED | ERROR", case=False)])
    cancelled_task_jobs = df_task [df_task['STATE'] == 'CANCELLED'].shape[0]
    run_task_count=df_task[(df_task['STATE'] == 'RUNNING')].shape[0]
    queue_task_count = df_task [(df_task['STATE'] == 'QUEUED')].shape[0]
    color_mapping = {
        'SUCCESS': '#16A34A', # Blue
        'FAILED': '#DC2626', # Red
        'CANCELLED': '#feb746', # orange
        'RUNNING': '#2563EB',# green
        'QUEUE': '#F59E0B'
    }
    running_jobs, run_df=dbt_running_run_and_queue(3)
    queued_jobs, queue_df=dbt_running_run_and_queue (1)

    local_time = time.localtime()
    gmt_end_time = datetime.now(timezone.utc)
    gmt_start_time = datetime.now(timezone.utc) - timedelta(hours=24)

    failed_jobs_dbt, fail_df= dbt_failed_jobs (20, gmt_start_time, gmt_end_time)
    # st.dataframe (fail_df) 
    running_jobs+=run_count 
    queued_jobs+=queue_count 
    failed_jobs+=failed_jobs_dbt

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
            <div style="display:flex; flex-wrap:wrap; justify-content:left;">
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

        # with st.container(border=False):
        #     # header_col1,divider, header_col2 = st.columns([1,0.05, 1])
        #     st.markdown(f"""
        #     <div class="dashboard-box">
        #          <div>
        #              <h1 style="color:Black;font-family:'Inter', sans-serif; font-size: 20px; margin: auto; text-align: center;">
        #                  Jobs Status Overview (24hrs)
        #              </h1>
        #         </div>
        #         <div style="display:flex; flex-wrap:wrap;">
        #              <div class="status-card"><p>Success</p><h2>{successful_task_jobs}</h2></div>
        #              <div class="status-card"><p>Failed</p><h2>{failed_task_jobs}</h2></div>
        #              <div class="status-card"><p>Cancelled</p><h2>{cancelled_task_jobs}</h2></div>
        #              <div class="status-card"><p>Running</p><h2>{run_task_count}</h2></div>
        #              <div class="status-card"><p>Queue</p><h2>{queue_task_count}</h2></div>
        #         </div>
        #     </div>
        #     """, unsafe_allow_html=True)

        st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">',unsafe_allow_html=True)


    schedules_options = ["ALL"] + list(df["SCHEDULE_NAME"].unique())
    source_type_options = ["ALL"] + list(df["SOURCE_TYPE"].unique())
    status_options = [
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

        

        filter_col1, filter_col2 = st.columns([2, 2])

        with filter_col1:
            f1, f2, f3 = st.columns(3)
            with f1:
                schedules_options_placeholder = ["Schedule"] + schedules_options[1:]  # assuming schedules_options[0] is 'ALL'
                schedule_filter = st.selectbox("", options=schedules_options_placeholder, index=0
                )
            with f2:
                source_type_options_placeholder = ["Source Type"] + source_type_options[1:]
                source_type_filter = st.selectbox("", options=source_type_options_placeholder, index=0
                )
            with f3:
                status_options_placeholder = ["Status"] + status_options[1:]
                status_filter = st.selectbox("", options=status_options_placeholder, index=0
                )

        

        # insert RUNNING AND QUEUE
        columns_to_insert = [
            "PROJECT_NAME",
            "SCHEDULE_NAME",
            "START_TIME",
            "END_TIME",
            "ERROR_MESSAGE",
            "STATUS",
            "SOURCE_TYPE",
            "LINKS",
            "JOB_TAG_NAME",
            "TRIGGER_BY",
            "TASK_HISTORY_ID",
        ]
        # fail_col_to_insert = ['PROJECT_NAME', 'SCHEDULE_NAME', 'START_TIME', 'END_TIME', 'TOTAL RUNTIME MINUTES', 'ERROR_MESSAGE', 'STATUS', 'SOURCE_TYPE', 'LINKS', 'JOB_TAG_NAME', 'TRIGGER_BY', 'TASK_HISTORY_ID', 'REVIEWED','REVIEWER_NAME','COMMENTS']
        fail_col_to_insert = columns_to_insert + [
            "REVIEWED",
            "REVIEWER_NAME",
            "COMMENTS",
        ]

        filtered_merged_df = run_df[columns_to_insert]
        filtered_queue_df = queue_df[columns_to_insert]
        final_fail_df = fail_df[fail_col_to_insert]

        concatenated_df_temp = pd.concat(
            [filtered_merged_df, filtered_queue_df], ignore_index=True
        )
        concatenated_df = pd.concat(
            [final_fail_df, concatenated_df_temp], ignore_index=True
        )

        df = pd.concat([df, concatenated_df], ignore_index=True)

        df.loc[
            df["STATUS"].str.contains("FAILEDIERROR", case=False), "STATUS"
        ] = "FAILED"
        # st.dataframe(df)

        df["LINKS"] = df.apply(
            lambda row: f"https://matillion-prod.corp.zscaler.com/#ZSCALER_BI/ZSCALER_BI_DWH/default/{row['SCHEDULE_NAME']}/run/{row['TASK_HISTORY_ID']}"
            if row["SOURCE_TYPE"] == "MATILLION"
            else row["LINKS"],
            axis=1,
        )

        schedule_filter_val = (
            "ALL" if schedule_filter == "Schedule" else schedule_filter
        )
        source_type_filter_val = (
            "ALL" if source_type_filter == "Source Type" else source_type_filter
        )
        status_filter_val = (
            "ALL" if status_filter == "Status" else status_filter
        )

  
        if status_filter_val == "FAILED":
            df_filtered = df[
                ((df["SOURCE_TYPE"] == source_type_filter_val)| (source_type_filter_val == "ALL"))
                & ((df["SCHEDULE_NAME"] == schedule_filter_val)| (schedule_filter_val == "ALL"))
                & ((df["STATUS"] == "FAILED")|(status_filter_val == "ALL"))
            ]
        else:
            df_filtered = df[
                ((df["SOURCE_TYPE"] == source_type_filter_val) | (source_type_filter_val == "ALL"))
                & ((df["SCHEDULE_NAME"] == schedule_filter_val) | (schedule_filter_val == "ALL"))
                & ((df["STATUS"] == status_filter_val) | (status_filter_val == "ALL"))
            ]

        df_filtered["START_TIME"] = pd.to_datetime(df_filtered["START_TIME"])
        df_filtered["START_TIME"] = df_filtered["START_TIME"].dt.strftime(
            "%B %d, %Y at %I:%M %p"
        )
        df_filtered["END_TIME"] = pd.to_datetime(df_filtered["END_TIME"])
        df_filtered["END_TIME"] = df_filtered["END_TIME"].dt.strftime(
            "%B %d, %Y at %I:%M %p"
        )
        # df_filtered = df_filtered.sort_values (by= 'START_TIME', ascending=False)

        column_config1 = {
            "PROJECT_NAME": st.column_config.Column(
                " PROJECT", help="PROJECT NAME", width="medium"
            ),
            "SCHEDULE_NAME": st.column_config.Column(
                " SCHEDULE", help="SCHEDULE NAME", width="medium"
            ),
            "JOB_TAG_NAME": st.column_config.Column(
                " JOB/TAG ", help="JOB/TAG NAME", width="medium"
            ),
            "START_TIME": st.column_config.Column(
                " START TIME (PST)", help="START TIME (PST)", width="medium"
            ),
            "END_TIME": st.column_config.Column(
                " END TIME (PST)", help="END TIME (PST)", width="medium"
            ),
            "ERROR_MESSAGE": st.column_config.Column(
                " ERROR MESSAGE", help="ERROR MESSAGE", width="medium"
            ),
            "TOTAL_RUNTIME_MINUTES": st.column_config.Column(
                " TOTAL RUNTIME (MIN)", help="TOTAL RUNTIME (MINS)", width="medium"
            ),
            "TASK_HISTORY_ID": st.column_config.Column(
                " TASK HISTORY ID", help="TASK HISTORY ID", width="medium"
            ),
            "TRIGGER_BY": st.column_config.Column(
                " TRIGGERED BY", help="TRIGGER BY", width="medium"
            ),
            "SOURCE_TYPE": st.column_config.Column(
                " SOURCE TYPE", help="SOURCE TYPE", width="medium"
            ),
            "LINKS": st.column_config.LinkColumn(
                " LINKS",
                help="LINKS",
                width="medium",
            ),
            "REVIEWER_NAME": st.column_config.Column(
                " REVIEWER ",
                help="REVIEWER NAME",
                width="medium",
            ),
        }

        def color_status(val):
            color_map = {
                "SUCCESS": "#5b85fb",
                "FAILED": "#f26271",
                "CANCELLED": "#feb746",
                "RUNNING": "#61d7a1",
                "QUEUED": "#fee8c6",
            }
            return f'background-color: {color_map.get(val, "white")}; font-weight:bold; border-radius: 5px; font-family: Inter, sans-serif;'

        if "TOTAL_RUNTIME_MINUTES" in df_filtered.columns:
            df_filtered["TOTAL_RUNTIME_MINUTES"] = (
                df_filtered["TOTAL_RUNTIME_MINUTES"].astype(float).fillna(0).astype(int)
            )

            

        # === Style for Table ===
        # df_styled = df_filtered.style.applymap(color_status, subset=['STATUS'])
        df_styled = df_filtered.style.applymap(
            color_status, subset=["STATUS"]
        ).set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#7fcbf0"),  # Dark blue-gray header
                        ("color", "#000"),
                        ("font-size", "12px"),
                        ("font-family", "Inter, sans-serif"),
                        # ("font-weight", "bold"),
                        ("text-align", "center"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [
                        ("background-color", "#7fcbf0"),
                        ("font-family", "Inter, sans-serif"),
                        ("font-size", "13px"),
                        ("color", "#000"),
                    ],
                },
            ]
        )

        st.dataframe(
            df_styled,
            column_config=column_config1,
            use_container_width=True,
            hide_index=True,
        )

        st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">',unsafe_allow_html=True)
        # edited_df = st.data_editor (df_filtered, column_config = column_config1,key= 'TASK_HISTORY_ID', use_container_width=True, hide_index=True)
    
    
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
        df_success = df[df["STATUS"] == "SUCCESS"].copy()
        df_success["TOTAL_RUNTIME_MINUTES"] = pd.to_numeric(df_success["TOTAL_RUNTIME_MINUTES"], errors="coerce")

        col1, col2 = st.columns([2, 4])
        with col1:
            top_n = st.selectbox(
                "Select number of top jobs to display",
                options=[1, 5, 10, 15, 20, 25, 30, 40, 50],
                index=1
            )

        top_runs = df_success.sort_values("TOTAL_RUNTIME_MINUTES", ascending=False).head(top_n).copy()
        top_runs["RANK"] = range(1, len(top_runs) + 1)
        top_runs["LABEL"] = (
            top_runs["RANK"].astype(str) + ". " +
            top_runs["SCHEDULE_NAME"].astype(str).str.slice(0, 25) + "..."
        )

        # ------------ VERTICAL BAR CHART ------------------
        fig = px.bar(
            top_runs,
            x="LABEL",
            y="TOTAL_RUNTIME_MINUTES",
            color="SOURCE_TYPE",
            text="TOTAL_RUNTIME_MINUTES",
            hover_data={
                "RANK": True,
                "SCHEDULE_NAME": True,
                "JOB_TAG_NAME": True,
                "SOURCE_TYPE": True,
                "TOTAL_RUNTIME_MINUTES": ":.2f",
            },
        )

        fig.update_traces(
            textposition="outside",
            texttemplate="%{text:.2f} min",
            marker_line=dict(width=1.2, color="darkgrey"),
            hovertemplate="<b>Rank:</b> %{customdata[0]}<br>" +
                        "<b>Schedule:</b> %{customdata[1]}<br>" +
                        "<b>Source:</b> %{customdata[3]}<br>" +
                        "<b>Job/Tag:</b> %{customdata[2]}<br>" +
                        "<b>Runtime:</b> %{y:.2f} min<br>" +
                        "<extra></extra>"
        )

        fig.update_layout(
            hovermode="x unified",
            hoverlabel=dict(
                bgcolor="white",
                font_size=12,
                font_family="'Inter', sans-serif"
            ),
            
            margin=dict(l=40, r=40, t=70, b=200),
            xaxis_title="Job Runs",
            yaxis_title="Runtime (Minutes)",
            xaxis=dict(
                tickangle=45,
                tickfont=dict(size=10, color="Black"),
            ),
            plot_bgcolor="rgba(245,246,250,1)",
            paper_bgcolor="rgba(245,246,250,1)",
            bargap=0.3,
        )

        st.plotly_chart(fig, use_container_width=True)

        st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">', 
                    unsafe_allow_html=True)


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
        

def schedule_lag_df():
    #session = session
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

    schedule_df =session.sql(sch_lag_query) 
    schedule_df=schedule_df.to_pandas()
    with st.container(border=False):
        st.markdown(
            """
            <h1 class="hover-effect">
               Schedule Lag Comparison (Current vs Previous Run)
            </h1>
            """,
            unsafe_allow_html=True,
        )
        filter_col1, filter_col2 = st.columns([2, 2])
        with filter_col1:
            source_type_filter = st.selectbox(
                "Source Type", ["ALL", "Matillion", "DBT"]
            )

        # Filter the DataFrame based on selected Source Type
        filter_sch = schedule_df[
            (
                (schedule_df["SOURCE_TYPE"] == source_type_filter)
                | (source_type_filter == "ALL")
            )
        ]
        # Display the filtered DataFrame below the bar chart
        column_config1 = {
            "SCHEDULE_NAME": st.column_config.Column(
                "SCHEDULE NAME", help="SCHEDULE NAME", width="medium"
            ),
            "EXPECTED_START_TIME": st.column_config.Column(
                "EXPECTED START TIME (PST)",
                help="EXPECTED START TIME (PST)",
                width="medium",
            ),
            "ACTUAL_START_TIME": st.column_config.Column(
                "ACTUAL START TIME (PST)",
                help="ACTUAL START TIME (PST)",
                width="medium",
            ),
            "LAG_TIME_MINUTES": st.column_config.Column(
                "LAG TIME MINUTES", help="LAG TIME MINUTES", width="medium"
            ),
            "SOURCE_TYPE": st.column_config.Column(
                "SOURCE TYPE", help="SOURCE TYPE", width="medium"
            ),
            "PROJECT_NAME": st.column_config.Column(
                "PROJECT NAME", help="PROJECT NAME", width="medium"
            ),
        }
        st.dataframe(
            filter_sch,
            hide_index=True,
            column_config=column_config1,
            use_container_width=True,
        )

        st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">',unsafe_allow_html=True)
        # st.markdown("", unsafe_allow_html=True)
        
def avg_schedule_lag():
    #session = session
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
                sd.ENABLED = 'TRUE' and (day_of_week=true or days_of_month is not null) and rh. STATE='SUCCESS' and rh.type = 'SCHEDULE ORCHESTRATION' and 
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
    avg7_schedule_df = session.sql(avg7_lag_query) 
    avg7_schedule_df= avg7_schedule_df.to_pandas ()

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
        "SCHEDULE_NAME": st.column_config.Column(
            "SCHEDULE NAME", help="SCHEDULE NAME", width="medium"
        ),
        "AVG_LAST_7_RUNS_MINUTES": st.column_config.Column(
            "AVG LAST 7 RUNS MINUTES", help="AVG LAST 7 RUNS MINUTES", width="medium"
        ),
        "LATEST_RUN_MINUTES": st.column_config.Column(
            "LATEST RUN MINUTES", help="LATEST RUN MINUTES", width="medium"
        ),
        "TIME_DIFFERENCE_MINUTES": st.column_config.Column(
            "TIME DIFFERENCE MINUTES", help="TIME DIFFERENCE MINUTES", width="medium"
        ),
        "SOURCE_TYPE": st.column_config.Column(
            "SOURCE TYPE", help="SOURCE TYPE", width="medium"
        ),
        "PROJECT_NAME": st.column_config.Column(
            "PROJECT NAME", help="PROJECT NAME", width="medium"
        ),
    }
    st.dataframe(
        avg7_schedule_df,
        column_config=column_config1,
        use_container_width=True,
        hide_index=True,
    )

    st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">',unsafe_allow_html=True)
def upcoming_sch_idle_time():
    #session= session

    up_sch='''
   WITH FlattenedSchedules AS (
        SELECT
            sd.name AS SCHEDULE_NAME,
            sd.run_date,
            sd.minute,
            C.value::int AS RUN_HOUR,
            TO_TIMESTAMP_LTZ (sd.run_date ||' '||LPAD (C.value, 2, '0') ||':' || LPAD (sd.minute, 2, '0') || ':00', 'YYYY-MM-DD HH24:MI:SS') AS START_RUN_TIME
        FROM
            EDW_LAB_DEV.OBSERVABILITY.matillion_schedules_details sd,
            LATERAL FLATTEN (input => split (sd. hour, ',')) C
        WHERE
            sd.enabled = true
            AND (sd.day_of_week =true or sd.days_of_month is not null)
            AND sd.run_date = (SELECT MAX(run_date) FROM EDW_LAB_DEV.OBSERVABILITY.matillion_schedules_details)
    )

    SELECT
        upper (SCHEDULE_NAME) SCHEDULE_NAME,
        START_RUN_TIME,
        'MATILLION' AS SOURCE_TYPE
    FROM
        FlattenedSchedules
    WHERE
        START_RUN_TIME > CURRENT_TIMESTAMP()
        AND DATE(START_RUN_TIME) <= CURRENT_DATE+1 --AND DATE (START_RUN_TIME) <= CURRENT_DATE+1
        -- START_RUN_TIME >= CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP)

    UNION ALL
    SELECT
        upper (NAME) AS SCHEDULE_NAME,
        CONVERT_TIMEZONE ('UTC', 'America/Los_Angeles', NEXT_RUN) AS START_RUN_TIME,
        SOURCE_TYPE
    FROM
        EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES
    WHERE
        CONVERT_TIMEZONE ('UTC', 'America/Los_Angeles', NEXT_RUN) > CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())
        AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', NEXT_RUN) <= CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', DATEADD (hour, 24, CURRENT_TIMESTAMP())) 
        AND JOB_TYPE = 'scheduled'
    ORDER by start_run_time asc;
    '''

    upcoming_schedules_df=session.sql(up_sch)
    upcoming_schedules_df=upcoming_schedules_df.to_pandas()

    with st.container(border=False):
        st.markdown(
            """
                <div style="text-align: left;">
                    <h1 class="hover-effect">
                        Upcoming Schedules
                    </h1>
                </div>
                """,
            unsafe_allow_html=True,
        )
        column_config = {
            "SCHEDULE_NAME": st.column_config.Column(
                "SCHEDULE NAME", help="SCHEDULE NAME", width="medium"
            ),
            "SOURCE_TYPE": st.column_config.Column(
                "SOURCE TYPE", help="SOURCE TYPE", width="medium"
            ),
            "START_RUN_TIME": st.column_config.Column(
                "START RUN TIME (PST)", help="START RUN TIME (PST)", width="medium"
            ),
        }
        st.dataframe(
            upcoming_schedules_df,
            column_config=column_config,
            use_container_width=True,
            hide_index=True,
        )

    idle_query='''
        SELECT
            Last_end_time AS FROM_DATETIME, 
            next_start_time AS TO_DATETIME,
            --idle_seconds 60 AS idle_minutes
        FROM
            (
                SELECT
                    END_TIME_PST AS last_end_time,
                    LEAD (END_TIME_PST) OVER (ORDER BY END_TIME_PST) AS next_start_time,
                    TIMESTAMPDIFF('second', END_TIME_PST, LEAD (END_TIME_PST) OVER (ORDER BY END_TIME_PST)) AS idle_seconds
                FROM
                    EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY
                WHERE
                    END_TIME_PST >= CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP) - INTERVAL '1 day' 
                    AND STATE = 'SUCCESS'
            ) AS idle_time
        WHERE
            idle_seconds / 60 >= 6
            AND next_start_time >= DATEADD (hour, -24, CURRENT_TIMESTAMP)
        ORDER BY
            Last_end_time DESC;
        '''
    
    st.markdown('<hr style="border:0.5px grey; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);">',unsafe_allow_html=True)
    # st.markdown("", unsafe_allow_html=True)
    
    st.markdown(
        """
                <div style="text-align: left;">
                    <h1 class="hover-effect">
                        Matillion Idle Time(24hrs)
                    </h1>
                </div>
                """,
        unsafe_allow_html=True,
    )
    idle_df = session.sql(idle_query)
    idle_df = idle_df.to_pandas()
    column_config1 = {
        "FROM_DATETIME": st.column_config.Column("FROM DATETIME (PST)", width="medium"),
        "TO_DATETIME": st.column_config.Column("TO DATETIME (PST)", width="medium"),
    }
    st.dataframe(
        idle_df, column_config=column_config1, use_container_width=True, hide_index=True
    )

#main
#main
# spinner_placeholder=st.empty()
with st.spinner ('Loading, please wait...'):
    #st.session_state.spinner_text='Loading dataframe and charts...'
    #spinner_placeholder.markdown ("<p style='color: #5D6A85;'>Loading dataframe and charts...</p>", unsafe_allow_html=True) 
    cl1, cl2, cl3 = st.columns ([4,1,1])
    with cl3:
        #st.image("assets/logo_cloudeqs.png", width=200)
        st.image("./assets/logo_cloudeqs.png", width=200)


dbt_schedules_api()
create_df_charts ()
# jobs_status_data()
# spinner_placeholder.markdown ("<p style='color: #5D6A85;'>Loading lagging schedule data...</p>", unsafe_allow_html=True) 
schedule_lag_df ()
# spinner_placeholder.markdown ("<p style='color: #5D6A85;'>Loading matillion idle time data...</p>", unsafe_allow_html=True) 
avg_schedule_lag()
upcoming_sch_idle_time()
# spinner_placeholder.markdown ("")
