
import streamlit as st
# import snowflake
#from snowflake.snowpark.context import get_active_session 
from spcs_helpers.connection import session
import pandas as pd
import plotly.express as px
import streamlit as st
import json
import math
import time as ti
import pytz
from datetime import datetime, date, time, timedelta, timezone 
st.set_page_config(page_title="Job Logs Dashboard", layout="wide")

session=session()

st.set_page_config(
    page_title="Job Logs Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

def load_css(path):
    with open(path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css("CSS/sidebar.css")
load_css("CSS/job.css")

from sidebar import render_sidebar
render_sidebar()
def convert_to_pst(epoch_time):
    utc_time = datetime.fromtimestamp (epoch_time / 1000, tz=pytz.utc) 
    pst_timezone = pytz.timezone ('America/Los_Angeles')
    pst_time = utc_time.astimezone (pst_timezone)
    return pst_time.strftime('%Y-%m-%d %H:%M:%S.%f')

def mat_running_run_and_queue():

    mat_session = session

    time_query=f"""
    SELECT
    CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', MAX(END_TIME_PST)) 
    FROM EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY where END_TIME_PST is not null
    """
    time_data = mat_session.sql(time_query).to_pandas()
    timestamp_str = str(time_data.iloc[0,0])
    date_str, time_str = timestamp_str.split(' ')
    # Further split the time to remove milliseconds 
    time_str = time_str.split('.')[0]
    # Extract hours and minutes
    time_str = time_str[:5]
    #st.write(date_str, '---', time_str) 
    mat_api=f"""
    select EDW_LAB_DEV.OBSERVABILITY.MATILLION_JOBS_API('{date_str}', '{time_str}');
    """
    mat_df = mat_session.sql(mat_api).to_pandas()
    resp = mat_df.iloc[0,0]
    #clean_res=resp.replace("'",'"')
    data=json.loads(resp)
    body_data=data
    rows=[]
    for df in body_data:
        row=(df.get("id"), 
        df.get("projectName"),
        df.get("state"),
        df.get("jobName"),
        df.get("startTime"),
        df.get("endTime")
        )
        rows.append(row)

    columns=['TASK_HISTORY_ID', 'PROJECT_NAME',
             'STATUS','SCHEDULE_NAME',
             'START_TIME','END_TIME']
    mat_data=pd.DataFrame(rows,columns=columns)
    mat_data['SCHEDULE_NAME'] = mat_data['SCHEDULE_NAME'].str.upper() 
    mat_data['JOB_TAG_NAME']=mat_data['SCHEDULE_NAME']

    mat_data['START_TIME'] = mat_data['START_TIME'].apply(convert_to_pst) 
    mat_data['END_TIME'] = mat_data['END_TIME'].apply(convert_to_pst) 
    mat_data.loc[mat_data['STATUS'].isin(['QUEUED']), 'START_TIME'] = None 
    mat_data.loc[mat_data['STATUS'].isin(['RUNNING', 'QUEUED']), 'END_TIME'] = None

    mat_data['TOTAL_RUNTIME_MINUTES'] = (
        pd.to_datetime(mat_data['END_TIME']) - pd.to_datetime (mat_data['START_TIME']) 
    ).dt.total_seconds() // 60
    # Create LINKS column
    mat_data['START_TIME'] = pd.to_datetime(mat_data['START_TIME']).dt.tz_localize(None) 
    mat_data['END_TIME'] = pd.to_datetime(mat_data['END_TIME']).dt.tz_localize(None) 
    mat_data['SOURCE_TYPE']='MATILLION'
    return mat_data

def dbt_failed_jobs (param, time_input_start, time_input_end):

    time_input_start=str(time_input_start).replace('+00:00','') 
    time_input_end=str(time_input_end).replace('+00:00','')

    dbt_session= session
    dbt_api=f"""
    select EDW_LAB_DEV.OBSERVABILITY.FAILED_DBT_JOBS_API({param}, '{time_input_start}', '{time_input_end}');
    """
    dbt_df = dbt_session.sql(dbt_api).to_pandas()
    # st.write(dbt_df)
    response = dbt_df.iloc[0,0]
    data=json.loads(response)
    # st.write(data) 
    rows=[]
    for df in data:
        row=(df.get("id"),
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
    columns=['TASK_HISTORY_ID','trigger_id', 'environment_id', 'account_id', 'project_id','job_definition_id', 'status', 
             'job_id', 'LINKS', 'ERROR_MESSAGE', 'created_at', 'updated_at',
             'START_TIME', 'END_TIME', 'last_heartbeat_at', 'should_start_at', 'STATUS',
             'trigger', 'in_progress', 'is_complete', 'is_error','is_failed', 'duration', 
             'queued_duration', 'run_duration']
    dbt_data=pd.DataFrame (rows, columns=columns)
    # dbt_data['START_TIME'] = pd.to_datetime(dbt_data['START_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S") 
    # dbt_data['END_TIME']=pd.to_datetime (dbt_data['END_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S")
    dbt_data['START_TIME'] = pd.to_datetime(dbt_data['START_TIME'])
    dbt_data['END_TIME'] = pd.to_datetime(dbt_data['END_TIME'])

    dbt_data=dbt_data.query("environment_id in [314701,282110, 218564]") 
    dbt_data['TASK_HISTORY_ID'] = dbt_data['TASK_HISTORY_ID'].astype(int)

    query_schedule='''
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
    schedule_data=dbt_session.sql(query_schedule) 
    schedule_df=schedule_data.to_pandas()
    merged_df=pd.merge(dbt_data, schedule_df, on=['job_id'], how='left')

    merged_df['TOTAL_RUNTIME_MINUTES'] = (pd.to_datetime(merged_df['END_TIME']) - pd.to_datetime(merged_df[ 'START_TIME'])).dt.total_seconds() / 60 
    
    merged_df[ 'START_TIME'] = pd.to_datetime (merged_df['START_TIME']).dt.tz_localize(None) 
    merged_df['END_TIME'] = pd.to_datetime (merged_df['END_TIME']).dt.tz_localize (None)
    columns_to_display = ['SCHEDULE_NAME', 'JOB_TAG_NAME','START_TIME', 'END_TIME', 'TOTAL_RUNTIME_MINUTES', 'TASK_HISTORY_ID', 'STATUS', 'ERROR_MESSAGE',
                    'PROJECT_NAME', 'SOURCE_TYPE','LINKS', 
                    'TRIGGER_BY']
    merged_df = merged_df[columns_to_display]
    return merged_df


def create_history_data(from_date_time):
    #session = session()
    query =f"""
        with cte_mat_temp as(
            SELECT sd.name as SCHEDULE_NAME, upper(rh.job_name) JOB_TAG_NAME, DAYOFWEEK(rh.START_TIME_PST),
                rh.START_TIME_PST AS START_TIME, rh.END_TIME_PST AS END_TIME, TIMESTAMPDIFF('minute',START_TIME, END_TIME) AS TOTAL_RUNTIME_MINUTES, 
                upper(rh.STATE) as STATUS, rh."message" as ERROR_MESSAGE,
                rh. TASK_HISTORY_ID, rh. PROJECT_NAME,
                'MATILLION' AS SOURCE_TYPE,
                null as LINKS, null as TRIGGER_BY
            FROM EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY rh
            JOIN EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS sd
            ON  rh.PROJECT_NAME = sd.PROJECT AND
                rh.JOB_NAME = sd.JOB_NAME
                and run_date=(select max(run_date) from EDW_LAB_DEV.OBSERVABILITY.matillion_schedules_details)
            WHERE enabled=true and
                rh.type in ('SCHEDULE_ORCHESTRATION', 'QUEUE_ORCHESTRATION') and rh.PROJECT_NAME='ZSCALER_BI_DWH' 
                and END_TIME >= DATEADD (hour, -360, CURRENT_TIMESTAMP)
            ORDER BY END_TIME DESC
        ),
        cte_dbt_status as (
            select listagg(distinct 
            CASE
                WHEN contains(lower(rr.status),'error') THEN 'error'        
                ELSE 'success'
            END 
                ,',') status,listagg(distinct 
            CASE
                WHEN contains(lower(rr.status),'error') THEN message      
                ELSE ''
            END
                ,',') message,job_run_id,listagg(distinct di.SELECTED,',') AS JOB_TAG_NAME
            from  
                EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS di 
            inner join  
                EDW_LAB_DEV.OBSERVABILITY.DBT_RUN_RESULTS rr  
            on 
                di.invocation_id = rr.invocation_id 
            where 
                UPPER(STATUS) NOT LIKE '%ERROR%' and di.env='prod' and di.cause_category in ('scheduled','other') and rr.resource_type in ('model','test','snapshot','seed') and
                CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', di.RUN_COMPLETED_AT) >= '{from_date_time}'
                and CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', di.RUN_COMPLETED_AT) >= DATEADD(hour, -168, CURRENT_TIMESTAMP)
                group by job_run_id
        ),
        mat_final_cte as(
            select * from cte_mat_temp where lower(SCHEDULE_NAME) not like '%weekend%' and lower(SCHEDULE_NAME) not like '%weekday%' 
            union
            select * from cte_mat_temp where lower(SCHEDULE_NAME) like '%weekend %' and DAYOFWEEK(START_TIME) in (0,6) 
            union
            select * from cte_mat_temp where lower(SCHEDULE_NAME) like '%weekday%' and (DAYOFWEEK(START_TIME) in (1,2,3,4,5))
        )

        select upper(SCHEDULE_NAME) SCHEDULE_NAME, JOB_TAG_NAME, START_TIME, END_TIME, TOTAL_RUNTIME_MINUTES,
                TASK_HISTORY_ID, STATUS, ERROR_MESSAGE,
                PROJECT_NAME, 'MATILLION' AS SOURCE_TYPE,
                null as LINKS, null as TRIGGER_BY
        FROM mat_final_cte
        UNION ALL
        select  UPPER(st.name) AS SCHEDULE_NAME, ds.JOB_TAG_NAME,
            CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', min(di.RUN_STARTED_AT)) AS START_TIME, 
            CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', max(di.RUN_COMPLETED_AT)) AS END_TIME, 
            TIMESTAMPDIFF('minute',START_TIME, END_TIME) AS TOTAL_RUNTIME_MINUTES, 
            di.job_run_id as TASK_HISTORY_ID, upper(ds.STATUS) as STATUS, ds.message as ERROR_MESSAGE, 
            di.project_name, 'DBT' AS SOURCE_TYPE,
            di.job_url as LINKS, di.cause AS TRIGGER_BY
        FROM EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES st
            inner join EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS di
            on st.id=di.job_id  inner join cte_dbt_status ds on di.job_run_id = ds.job_run_id
        WHERE --st.project_id='270340' and st.environment_id=218564 and 
            di.RUN_COMPLETED_AT >= DATEADD(hour, -168, CURRENT_TIMESTAMP)
        group by di.job_run_id , ds.JOB_TAG_NAME, di.project_name, st.execute_steps, st.name, 
        STATUS,message,  SOURCE_TYPE, LINKS, TRIGGER_BY
        ORDER BY
        END_TIME DESC;
    """
    df = session.sql(query).to_pandas()
    df['START_TIME'] = pd.to_datetime (df['START_TIME']).dt.tz_localize(None) 
    df['END_TIME'] = pd.to_datetime(df['END_TIME']).dt.tz_localize(None)

    return df

def failed_dbt_data():
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
    failed_df['START_TIME'] = pd.to_datetime (failed_df['START_TIME'])
    failed_df['END_TIME'] = pd.to_datetime(failed_df['END_TIME'])
    failed_df['TOTAL_RUNTIME_MINUTES'] = (pd.to_datetime (failed_df['END_TIME']) - pd.to_datetime (failed_df[ 'START_TIME'])).dt.total_seconds() / 60

    failed_df['START_TIME'] = pd.to_datetime(failed_df['START_TIME']).dt.tz_localize(None) 
    failed_df['END_TIME'] = pd.to_datetime (failed_df['END_TIME']).dt.tz_localize(None) 
    query_comments = '''
    SELECT
        --cast(cc.run_id as number) AS TASK_HISTORY_ID,
        cc.run_id AS TASK_HISTORY_ID,
        cc.reviewed,
        cc.comments
    FROM EDW_LAB_DEV.OBSERVABILITY. JOBS_COMMENTS cc
    '''
    comments_data = dbt_session.sql(query_comments)
    comments_df = comments_data.to_pandas()
    # Merge merged_df with comments data on TASK_HISTORY_ID
    failed_df['TASK_HISTORY_ID'] = failed_df['TASK_HISTORY_ID'].astype(int)
    comments_df['TASK_HISTORY_ID'] = comments_df['TASK_HISTORY_ID'].astype(int)
    
    failed_df = pd.merge(failed_df, comments_df, on='TASK_HISTORY_ID', how='left')

    columns_to_display = ['SCHEDULE_NAME', 'JOB_TAG_NAME','START_TIME', 'END_TIME', 'TOTAL_RUNTIME_MINUTES', 'TASK_HISTORY_ID', 'STATUS', 'ERROR_MESSAGE', 
                          'PROJECT_NAME', 'SOURCE_TYPE', 'LINKS',
                          'TRIGGER_BY', 'COMMENTS', 'REVIEWED']
    final_fail_df = failed_df[columns_to_display]
    return final_fail_df

def history_job_data(from_date_time):
    #session = session()
    query =f"""
        with cte_dbt_status as (
            select listagg(distinct 
            CASE
                WHEN contains(lower(rr.status),'error') THEN 'error'        
                ELSE 'success'
            END 
                ,',') status,listagg(distinct 
            CASE
                WHEN contains(lower(rr.status),'error') THEN message      
                ELSE ''
            END
                ,',') message,job_run_id,listagg(distinct di.SELECTED,',') AS JOB_TAG_NAME
            from  
                EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS di 
            inner join  
                EDW_LAB_DEV.OBSERVABILITY.DBT_RUN_RESULTS rr  
            on 
                di.invocation_id = rr.invocation_id 
            where 
                UPPER(STATUS) NOT LIKE '%ERROR%' and di.env='prod' and di.cause_category in ('scheduled','other') and rr.resource_type in ('model','test','snapshot','seed') and
                CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', di.RUN_COMPLETED_AT) >= '{from_date_time}'
                and CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', di.RUN_COMPLETED_AT) < DATEADD(hour, -168, CURRENT_TIMESTAMP)
                group by job_run_id
        ),
        cte_mat_temp as(
            SELECT sd.name as SCHEDULE_NAME, upper(rh.job_name) JOB_TAG_NAME,  DAYOFWEEK (rh.START_TIME_PST),
                rh.START_TIME_PST AS START_TIME, rh.END_TIME_PST AS END_TIME, TIMESTAMPDIFF('minute',START_TIME, END_TIME) AS TOTAL_RUNTIME_MINUTES, 
                upper(rh.STATE) as STATUS, rh."message" as ERROR_MESSAGE,
                rh.TASK_HISTORY_ID, rh.PROJECT_NAME, 
                'MATILLION' AS SOURCE_TYPE,
                null as LINKS, null as TRIGGER_BY
                FROM EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY rh
                JOIN EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS sd
                ON rh.PROJECT_NAME = sd.PROJECT AND 
                rh.JOB_NAME = sd.JOB_NAME
                and run_date=(select max(run_date) from EDW_LAB_DEV.OBSERVABILITY.matillion_schedules_details) 
                WHERE enabled=true and 
                rh.type in  ('SCHEDULE_ORCHESTRATION','QUEUE_ORCHESTRATION' ) and rh.PROJECT_NAME='ZSCALER_BI_DWH'
                    and END_TIME < DATEADD(hour, -168, CURRENT_TIMESTAMP)
                ORDER BY END_TIME DESC
        ),
        mat_final_cte as(
            select * from cte_mat_temp where lower(SCHEDULE_NAME) not like '%weekend%'  and lower(SCHEDULE_NAME) not like '%weekday%'
            union
            select * from cte_mat_temp where lower(SCHEDULE_NAME) like '%weekend%'  and DAYOFWEEK (START_TIME) in (0,6)
            union
            select * from cte_mat_temp where lower(SCHEDULE_NAME) like '%weekday%'  and (DAYOFWEEK (START_TIME) in (1,2,3,4,5) )
        )
        select upper(SCHEDULE_NAME) SCHEDULE_NAME, JOB_TAG_NAME, START_TIME, END_TIME, TOTAL_RUNTIME_MINUTES, 
                TASK_HISTORY_ID, STATUS,ERROR_MESSAGE,
                PROJECT_NAME, 'MATILLION' AS SOURCE_TYPE,
                null as LINKS, null as TRIGGER_BY
                FROM mat_final_cte
        UNION ALL
        select  UPPER(st.name) AS SCHEDULE_NAME, ds.JOB_TAG_NAME,
            CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', min(di.RUN_STARTED_AT)) AS START_TIME, 
            CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', max(di.RUN_COMPLETED_AT)) AS END_TIME, TIMESTAMPDIFF('minute',START_TIME, END_TIME) AS TOTAL_RUNTIME_MINUTES, 
            di.job_run_id as TASK_HISTORY_ID, upper(ds.STATUS) as STATUS, ds.message as ERROR_MESSAGE, 
            di.project_name, 'DBT' AS SOURCE_TYPE, di.job_url as LINKS, di.cause AS TRIGGER_BY
        FROM 
            EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES st
        inner join 
            EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS di
        on 
            st.id=di.job_id  
        inner join 
            cte_dbt_status ds on di.job_run_id = ds.job_run_id
        WHERE 
            di.RUN_COMPLETED_AT >= '{from_date_time}'
            group by di.job_run_id , ds.JOB_TAG_NAME, di.project_name, st.execute_steps, st.name, STATUS,message,  SOURCE_TYPE, LINKS, TRIGGER_BY
                ORDER BY 
                END_TIME DESC;
    """
    df = session.sql(query).to_pandas()
    df['START_TIME'] = pd.to_datetime(df['START_TIME']).dt.tz_localize(None)
    df['END_TIME'] = pd.to_datetime(df['END_TIME']).dt.tz_localize(None)
    return df


    





from_date_time = '2025-05-01'
history_df=create_history_data(from_date_time)
mat_latest_data=mat_running_run_and_queue()

mat_data=pd.concat([mat_latest_data, history_df], ignore_index=True)


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
            <h1 style="font-family: Inter, sans-serif; font-size: 22px; text-align: left;">
                Job History Details
            </h1>
        </div>
        """, unsafe_allow_html=True
    )



local_time = ti.localtime()
gmt_end_time = datetime.now(timezone.utc)
gmt_start_time = datetime.now(timezone.utc)-timedelta(hours=24)

dbt_api_failed_df = dbt_failed_jobs(20, gmt_start_time, gmt_end_time) 
dbt_failed_df=failed_dbt_data()

dbt_failed_df=pd.concat([dbt_failed_df, dbt_api_failed_df], ignore_index=True) 
dbt_failed_df=dbt_failed_df.drop_duplicates(subset=['TASK_HISTORY_ID'])

history_df=pd.concat([dbt_failed_df, mat_data], ignore_index=True) 
#history_df = history_df.dropna (subset=['START_TIME', 'END_TIME'])
history_df.loc[history_df['STATUS'].str.contains("FAILED ERROR", case=False), 'STATUS'] = "FAILED"
history_df[ 'PROJECT_NAME']=history_df[ 'PROJECT_NAME'].str.upper() 
button=False

with st.form(key='my_form'):

    row1_col1, row1_col2, row1_col3, row1_col4 = st.columns(4)

    with row1_col1:
        from_datetime = pd.to_datetime(history_df['START_TIME'].unique().max())
        from_date = date.fromisoformat(str(st.date_input("Select From Date", from_datetime.date())))

    with row1_col2:
        from_time = time.fromisoformat(str(st.time_input("Select From Time", from_datetime.time())))
        from_date_time_filter = datetime.combine(from_date, from_time).strftime('%Y-%m-%d %H:%M:%S.000')

    with row1_col3:
        to_datetime = pd.to_datetime(history_df['END_TIME'].unique().max())
        to_date = date.fromisoformat(str(st.date_input("Select To Date", to_datetime.date())))

    with row1_col4:
        to_time = time.fromisoformat(str(st.time_input("Select To Time", to_datetime.time())))
        to_date_time_filter = datetime.combine(to_date, to_time)


    row2_col1, row2_col2, row2_col3, row2_col4 = st.columns(4)

    with row2_col1:
        source_type_filter = st.selectbox("Select Source Type", ["All"] + list(history_df['SOURCE_TYPE'].unique()))

    with row2_col2:
        project_names = [p for p in history_df['PROJECT_NAME'].unique()]
        project_name_filter = st.selectbox("Select Project Name", ["All"] + project_names)

    with row2_col3:
        job_name_filter = st.text_input("Enter Schedule Name")

    with row2_col4:
        status_filter = st.selectbox("Select Status", ["All", "SUCCESS", "FAILED", "CANCELLED"])

    s1, s2 = st.columns([5, 1])
    with s2:
        button = st.form_submit_button(label="Search")

if button:
        history_data=history_job_data(from_date_time_filter)
        history_df=pd.concat([history_df, history_data], ignore_index=True)
        
        filtered_df = history_df[
            (history_df["START_TIME"] >= from_date_time_filter) &
            (history_df["END_TIME"] <= to_date_time_filter)
        ]
        
        # Filter by source type if not "All"
        if source_type_filter != "All":
            filtered_df = filtered_df[filtered_df['SOURCE_TYPE'] == source_type_filter]
        
        # Filter by project name, including None/NaN values if the user selects "All"
        if project_name_filter != "All":
            filtered_df = filtered_df[(filtered_df['PROJECT_NAME'] == project_name_filter) | (filtered_df['PROJECT_NAME'].isna())]
        
        # Filter by status if not "All"
        if status_filter != "All":
            filtered_df = filtered_df[filtered_df['STATUS'] == status_filter]
    
    # Filter by schedule name if provided by the user
        if job_name_filter:
            filtered_df = filtered_df[filtered_df['SCHEDULE_NAME'].str.contains(job_name_filter, case=False, na=False)]
        if 'LINKS' not in filtered_df.columns:
            filtered_df['LINKS'] = ""
        
        filtered_df['LINKS'] = filtered_df.apply(
            lambda row: f"https://13.90.90.241:8443/#CLOUDX/{row['PROJECT_NAME']}/default/{row['SCHEDULE_NAME']}/run/{row['TASK_HISTORY_ID']}" if row['SOURCE_TYPE'] == "MATILLION" else row['LINKS'],axis=1
        )

        # filtered_df['LINKS'] = filtered_df.apply(
        #     lambda row: (f"https://13.90.90.241:8443/#CLOUDX/{row['PROJECT_NAME']}/default/{row['SCHEDULE_NAME']}/run/{row['TASK_HISTORY_ID']}" if row['SOURCE_TYPE'] == "MATILLION"  else str(row['LINKS']).replace("None","179022")) ,axis=1
        # )
        filtered_df = filtered_df.sort_values(by='START_TIME', ascending=False)
        columns_to_display = ['SCHEDULE_NAME', 'JOB_TAG_NAME', 'START_TIME', 'END_TIME', 'TOTAL_RUNTIME_MINUTES', 'TASK_HISTORY_ID', 'STATUS', 'ERROR_MESSAGE',
                            'PROJECT_NAME', 'SOURCE_TYPE','LINKS', 
                            'TRIGGER_BY']
        filtered_df = filtered_df[columns_to_display]

        st.session_state['filtered_df'] = filtered_df

        if 'filtered_df' in st.session_state:
            filtered_df = st.session_state['filtered_df']

        if not filtered_df.empty:
            rows_per_page = 200
            #Calculate total number of pages
            total_pages = math.ceil(len(filtered_df) / rows_per_page)
            # Create a slider or selectbox for page selection
            page_number = st.number_input('Page', min_value=1, max_value=total_pages, step=1)
            # Calculate the start and end indices for the current page
            start_idx= (page_number - 1) * rows_per_page
            end_idx = start_idx + rows_per_page
            # Slice the DataFrame to display only the records for the current page
            current_page_data = filtered_df.iloc[start_idx:end_idx]

            column_config1={
                "PROJECT_NAME": st.column_config.Column(
                    "PROJECT NAME",
                    help="Project Name",
                    width="medium"
                ),
                "SCHEDULE_NAME": st.column_config.Column(
                    "SCHEDULE NAME",
                    help="SCHEDULE NAME",
                    width="medium"
                ),
                "JOB_TAG_NAME": st.column_config.Column(
                    "JOB/TAG NAME",
                    help="J0B/TAG NAME",
                    width="medium"
                ),
                "START_TIME": st.column_config.Column(
                    "Start TIME (PST)",
                    help="Start TIME (PST)",
                    width="medium"
                ),
                "END_TIME": st.column_config.Column(
                    "END TIME (PST)",
                    help="END TIME (PST)",
                    width="medium"
                ),
                "TOTAL_RUNTIME_MINUTES": st.column_config.Column(
                    "TOTAL RUNTIME (MINS)",
                    help="TOTAL RUNTIME (MINS)",
                    width="medium"
                ),
                "ERROR_MESSAGE": st.column_config.Column(
                    "ERROR MESSAGE",
                    help="ERROR MESSAGE",
                    width="medium"
                ),
                "TASK_HISTORY_ID": st.column_config.Column(
                    "TASK HISTORY ID",
                    help="TASK HISTORY ID",
                    width="medium"
                ),
                "SOURCE_TYPE": st.column_config.Column(
                    "SOURCE TYPE",
                    help="SOURCE TYPE",
                    width="medium"
                ),
                "LINKS": st.column_config.LinkColumn( 
                    "LINKS",
                    help="LINKS",
                    width="medium", 
                    display_text="Details",
                ),
                "LINK_TEXT": st.column_config.TextColumn(
                    "Details",  # Column that shows 'Details' as the clickable text
                    help="Link text"
                )
            }  
            def color_survived(val):
                if val == 'SUCCESS':
                    color = '#5b85fb'  
                elif val == 'FAILED':
                    color = '#f26271'
                elif val == 'CANCELLED':
                    color = '#feb746'
                elif val == 'RUNNING':
                    color = '#61d7a1'
                elif val == 'QUEUED':
                    color= '#fee8c6'
                else:
                    color='white'
                return f'background-color: {color}'
            current_page_data=current_page_data.style.applymap(color_survived, subset=['STATUS'])
            # st.dataframe(df_filtered, column_config = column_config1, hide_index=True)
            st.dataframe(current_page_data,column_config = column_config1 ,key='TASK_HISTORY_ID', use_container_width=True,hide_index=True)    
            # Display current page info


            p1,p2=st.columns([4,0.5])
            with p2:
                st.write(f"Page {page_number} of {total_pages}")

            colors = {'SUCCESS': 'darkgreen', 'FAILED': 'red', 'CANCELLED': 'yellow', 'SKIPPED':'light_blue',"_default": "orange"} 
            col1, col2 =st.columns(2)
            with col1:
                    st.markdown(
                        f"""
                        <div style="display: flex; align-items: center; justify-content: center;">
                            <div style="flex-grow: 1; text-align: left;">
                                <h1 style="color: #5D6A85; margin: 0; font-size: 18px;">Job Status by {project_name_filter} Project</h1> 
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    job_count = filtered_df.groupby('STATUS') ['SCHEDULE_NAME'].count()
                    if not job_count.empty:
                        fig = px.bar(job_count, x=job_count.index, y=job_count.values, color = job_count.index, color_discrete_map=colors, 
                                        width=400, height=400)
                        fig.update_yaxes (title="Job Count",dtick=100)
                        st.plotly_chart(fig)
                    else:
                        st.write('No data to Display')
            with col2:
                if {project_name_filter}!='ALL':
                    st.markdown(
                        f"""
                        <div style="display: flex; align-items: center; justify-content: center;">
                            <div style="Flex-grow: 1; text-align: left;">
                                <h1 style="color: #5D6A85; margin: 0; font-size: 18px;">Execution Time for Last 10 Runs For {job_name_filter}</h1> 
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    # st.title(f'Execution Time for Last 10 Runs For {job_name_filter}')
                    last_10_runs = filtered_df[filtered_df[ 'SCHEDULE_NAME'] == job_name_filter].sort_values(by='END_TIME', ascending=False).head(10) 
                    fig = px.bar(last_10_runs, x='END_TIME', y='TOTAL_RUNTIME_MINUTES', width=400, height=400) 
                    fig.update_yaxes(title="Time(Minutes)",dtick=5)
                    st.plotly_chart(fig)
                    #st.bar_chart(data-last_10_runs, x='END_TIME', y='TOTAL_RUNTIME")
                else:
                    st.write('No data to Display')
else:                
    st.write("No data to display.")



