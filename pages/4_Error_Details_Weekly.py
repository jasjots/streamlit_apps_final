import streamlit as st
import pandas as pd
from spcs_helpers.connection import session
import json
import plotly.express as px
import streamlit as st
import time
import pytz
							   
from datetime import datetime, date, timedelta, timezone
st.set_page_config(page_title="Job Logs Dashboard", layout="wide")

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
load_css("CSS/error.css")

from sidebar import render_sidebar
render_sidebar()
							   
																	  
													   
												 
def convert_to_pst(epoch_time):
    utc_time = datetime.fromtimestamp(epoch_time / 1000, tz=pytz.utc) 
    pst_timezone = pytz.timezone('America/Los_Angeles')
    pst_time = utc_time.astimezone (pst_timezone)
    return pst_time.strftime('%Y-%m-%d %H:%M:%S.%f')

def mat_failed_df():
    query = """
    with cte_dbt_status as (
        select listagg(distinct
        CASE
            WHEN contains(lower(rr.status),'error') THEN 'error' 
            ELSE 'success'
        END
        ,',') status,
        
            listagg(distinct
        CASE
            WHEN contains(lower (rr.status), 'error') THEN message
            ELSE ''
        END
        ,',') message, job_run_id, listagg(distinct di.SELECTED,',') AS JOB_TAG_NAME
        from EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS di 
        inner join EDW_LAB_DEV.OBSERVABILITY.DBT_RUN_RESULTS rr 
        on di.invocation_id = rr.invocation_id
        where 
            UPPER(STATUS) LIKE '%ERROR%' and di.env='prod' and di.cause_category in ('scheduled', 'other') and rr.resource_type in ('model', 'test', 'snapshot', 'seed') and 
            CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', di.RUN_COMPLETED_AT) >= DATEADD (hour, -168, CURRENT_TIMESTAMP) 
            group by job_run_id
    ),
    cte_mat_temp as (
        SELECT upper(sd.name) as SCHEDULE_NAME, upper(rh.job_name) JOB_TAG_NAME, DAYOFWEEK(rh.START_TIME_PST),
            rh.START_TIME_PST AS START_TIME, rh. END_TIME_PST AS END_TIME, TIMESTAMPDIFF('minute',START_TIME, END_TIME) AS TOTAL_RUNTIME_MINUTES, upper(rh.STATE) as STATUS, rh.MESSAGE as ERROR_MESSAGE,
            rh.TASK_HISTORY_ID, rh. PROJECT_NAME,
            'MATILLION' AS SOURCE_TYPE,
            null as LINKS, null as TRIGGER_BY
        FROM 
            EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY rh
        JOIN 
            EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS sd
        ON  rh.PROJECT_NAME = sd.PROJECT AND upper (rh.STATE)='FAILED' and
            rh.JOB_NAME = sd.JOB_NAME
            and run_date=(select max(run_date) from EDW_LAB_DEV.OBSERVABILITY.matillion_schedules_details) 
        WHERE   
                enabled=true and
                rh.JOBTYPE in ('SCHEDULE_ORCHESTRATION', 'QUEUE_ORCHESTRATION') and rh. PROJECT_NAME='ZSCALER_BI_DWH' and
                END_TIME >= DATEADD (hour, -168, CURRENT_TIMESTAMP) ORDER BY END_TIME DESC
    ),
    mat_final_cte as(
        select * from cte_mat_temp where lower (SCHEDULE_NAME) not like '%weekend%' and lower (SCHEDULE_NAME) not like '%weekday%' 
        union
        select * from cte_mat_temp where lower (SCHEDULE_NAME) like '%weekend%' and DAYOFWEEK (START_TIME) in (0,6) 
        union
        select * from cte_mat_temp where lower (SCHEDULE_NAME) like '%weekday%' and (DAYOFWEEK (START_TIME) in (1,2,3,4,5) )
    ),
    combinedData as(
        SELECT
            SCHEDULE_NAME, JOB_TAG_NAME, START_TIME, END_TIME, TOTAL_RUNTIME_MINUTES,
            TASK_HISTORY_ID, STATUS, ERROR_MESSAGE,
            PROJECT_NAME, 'MATILLION' AS SOURCE_TYPE,
            null as LINKS, null as TRIGGER_BY
            FROM mat_final_cte
    )
    select
        distinct cd.*,cc.REVIEWED,CC.REVIEWER_NAME,
        listagg(cc.comments, ', ') AS COMMENTS
    From
        CombinedData cd
    LEFT JOIN
        EDW_LAB_DEV.OBSERVABILITY.JOBS_COMMENTS cc
    ON
        cd.TASK_HISTORY_ID = cc.run_id 
        AND UPPER(cc.source_type) = UPPER(cd.SOURCE_TYPE)
    GROUP BY ALL
    ORDER BY
        cd.END_TIME DESC ;
    """
    df = session.sql(query).to_pandas()
    df['START_TIME'] = pd.to_datetime(df['START_TIME']).dt.tz_localize(None) 
    df['END_TIME'] = pd.to_datetime(df['END_TIME']).dt.tz_localize(None)

    return df

def dbt_failed_jobs (param, time_input_start, time_input_end): 
    time_input_start=str(time_input_start).replace('+00:00','') 
    time_input_end=str(time_input_end).replace('+00:00','')

    dbt_session = session 
    dbt_api=f"""
        select EDW_LAB_DEV.OBSERVABILITY.FAILED_DBT_JOBS_API({param}, '{time_input_start}','{time_input_end}');
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
    columns=['TASK_HISTORY_ID', 'trigger_id','environment_id', 'account_id', 'project_id','job_definition_id', 'status', 
             'job_id', 'LINKS', 'ERROR_MESSAGE', 'created_at', 'updated_at',
             'START_TIME','END_TIME','last_heartbeat_at', 'should_start_at','Status',
             'trigger', 'in_progress','is_complete', 'is_error','is_failed','duration', 
             'queued_duration', 'run_duration']
    dbt_data=pd.DataFrame (rows,columns=columns)
    # dbt_data['START_TIME'] = pd.to_datetime(dbt_data['START_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S')
    # dbt_data['END_TIME'] = pd.to_datetime(dbt_data['END_TIME']).dt.strftime("%Y-%m-%d %H:%M:%S") 
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

    merged_df['TOTAL_RUNTIME_MINUTES'] = (merged_df['END_TIME'] - merged_df[ 'START_TIME']).dt.total_seconds() / 60
    # merged_df=merged_df[merged_df['environment_id'] != 322201]
    merged_df['START_TIME'] = pd.to_datetime(merged_df['START_TIME']).dt.tz_localize(None)
    merged_df['END_TIME'] = pd.to_datetime (merged_df['END_TIME']).dt.tz_localize(None)
    
    query_comments = '''
    SELECT
        --cast(cc.run_id as number) AS TASK_HISTORY_ID,CC.REVIEWER_NAME,
        cc.run_id AS TASK_HISTORY_ID,CC.REVIEWER_NAME,
        cc.reviewed,
        listagg(cc.comments, ', ') AS COMMENTS
    FROM 
        EDW_LAB_DEV.OBSERVABILITY.JOBS_COMMENTS cc
        GROUP BY ALL
    '''
    comments_data = dbt_session.sql(query_comments)
    comments_df = comments_data.to_pandas()
    # Merge merged_df with comments data on TASK_HISTORY_ID
    merged_df = pd.merge (merged_df, comments_df, on='TASK_HISTORY_ID', how='left')
    columns_to_display = ['SCHEDULE_NAME', 'JOB_TAG_NAME','START_TIME', 'END_TIME', 'TOTAL_RUNTIME_MINUTES', 'TASK_HISTORY_ID',  'ERROR_MESSAGE', 
                          'PROJECT_NAME', 'SOURCE_TYPE', 'LINKS',
                          'TRIGGER_BY', 'COMMENTS', 'REVIEWED', 'REVIEWER_NAME']
    final_merge_df = merged_df[columns_to_display]
    # jobs_count=dbt_data[(dbt_data['status'] == param)].shape[0]
    
    return final_merge_df

def mat_running_run_and_queue():
    
    mat_session = session
    time_query=f"""
    SELECT
    CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', MAX(END_TIME_PST))
    FROM EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY where END_TIME_PST is not null
    """
    time_data = mat_session.sql(time_query).to_pandas()
    timestamp_str = str(time_data.iloc[0,0])
    if timestamp_str == 'None' or ' ' not in timestamp_str:
        return pd.DataFrame()
    date_str, time_str = timestamp_str.split(' ')
    # Further split the time to remove milliseconds 
    time_str = time_str.split('.')[0]
    # Extract hours and minutes
    time_str = time_str[:5]
    #st.write(date_str,'---', time_str)
    mat_api=f"""
    select EDW_LAB_DEV.OBSERVABILITY.MATILLION_JOBS_API('{date_str}', '{time_str}');
    """
    mat_df = mat_session.sql(mat_api).to_pandas()
    resp = mat_df.iloc[0,0]
    # clean_res=resp.replace("'", '"') 
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
    mat_data=pd.DataFrame(rows, columns=columns)
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
    query_comments = '''
            SELECT
                --cast (cc.run_id as number) AS TASK_HISTORY_ID,CC.REVIEWER_NAME,
                cc.run_id AS TASK_HISTORY_ID,CC.REVIEWER_NAME,
                cc.reviewed,
                listagg(cc.comments, ', ') AS COMMENTS
            FROM EDW_LAB_DEV.OBSERVABILITY.JOBS_COMMENTS cc
            GROUP BY ALL
            '''
    comments_data = mat_session.sql(query_comments) 
    comments_df = comments_data.to_pandas()
        # Merge merged_df with comments data on TASK_HISTORY_ID
    comments_df['TASK_HISTORY_ID'] = comments_df['TASK_HISTORY_ID'].astype(int)
    mat_data = pd.merge(mat_data, comments_df, on='TASK_HISTORY_ID', how='left') 
    return mat_data[mat_data['STATUS']=='FAILED']

def failed_dbt_data():
    dbt_session = session
    query_failed = '''
    SELECT distinct cast(fj.id as number) as TASK_HISTORY_ID, upper(st.NAME) AS schedule_name, st.execute_steps AS JOB_TAG_NAME, 
        fj.started_at as start_time,
        fj.finished_at as end_time,
        'FAILED' as STATUS,
        fj.status_message as error_message, di.project_name,
        'DBT' AS SOURCE_TYPE,
        fj.href as LINKS, fj."TRIGGER" as trigger_by
    FROM EDW_LAB_DEV.OBSERVABILITY.DBT_FAILED_JOBS fj
    left join EDW_LAB_DEV.OBSERVABILITY.DBT_SCHEDULES st on fj.job_id=st.id 
    LEFT JOIN EDW_LAB_DEV.OBSERVABILITY.DBT_PROJECTS di
    ON di.project_id = st.project_id;
    '''

    failed_data = dbt_session.sql(query_failed)
    failed_df = failed_data.to_pandas()
    failed_df['START_TIME'] = pd.to_datetime (failed_df['START_TIME'])
    failed_df['END_TIME'] = pd.to_datetime(failed_df['END_TIME'])

    failed_df['TOTAL_RUNTIME_MINUTES'] = (pd.to_datetime (failed_df['END_TIME']) - pd.to_datetime(failed_df[ 'START_TIME'])).dt.total_seconds() / 60 
    failed_df['START_TIME'] = pd.to_datetime (failed_df['START_TIME']).dt.tz_localize(None) 
    failed_df['END_TIME'] = pd.to_datetime(failed_df['END_TIME']).dt.tz_localize(None)
    query_comments ='''
    SELECT
        --cast(cc.run_id as number) AS TASK_HISTORY_ID,CC.REVIEWER_NAME,
        cc.run_id AS TASK_HISTORY_ID,CC.REVIEWER_NAME,
        cc.reviewed,
        listagg(cc.comments, ', ') AS COMMENTS
    FROM EDW_LAB_DEV.OBSERVABILITY.JOBS_COMMENTS cc
    GROUP BY ALL
    '''

    comments_data = dbt_session.sql(query_comments)
    comments_df = comments_data.to_pandas()
    # Merge merged_df with comments data on TASK_HISTORY_ID
    failed_df['TASK_HISTORY_ID'] = failed_df['TASK_HISTORY_ID'].astype(int)
    comments_df['TASK_HISTORY_ID'] = comments_df['TASK_HISTORY_ID'].astype(int)
    failed_df = pd.merge(failed_df, comments_df, on='TASK_HISTORY_ID', how='left')

    columns_to_display = ['SCHEDULE_NAME', 'JOB_TAG_NAME','START_TIME', 'END_TIME', 'TOTAL_RUNTIME_MINUTES', 'TASK_HISTORY_ID', 'STATUS', 'ERROR_MESSAGE', 
                          'PROJECT_NAME', 'SOURCE_TYPE', 'LINKS',
                          'TRIGGER_BY', 'COMMENTS', 'REVIEWED','REVIEWER_NAME']
    final_fail_df = failed_df[columns_to_display]
    return final_fail_df

def fetch_all_error_data():
    """Loader - executes all SQL queries to fetch latest error data."""
    gmt_end_time = datetime.now(timezone.utc)
    gmt_start_time = datetime.now(timezone.utc) - timedelta(hours=24)
    
    dbt_api_failed_df = dbt_failed_jobs(20, gmt_start_time, gmt_end_time)
    dbt_df = failed_dbt_data()
    df = pd.concat([dbt_df, dbt_api_failed_df], ignore_index=True)
    df = df.drop_duplicates(subset=['TASK_HISTORY_ID'])
    
    mat_df = mat_failed_df()
    df = pd.concat([df, mat_df], ignore_index=True)
    
    mat_api_fail = mat_running_run_and_queue()
    mat_api_fail['START_TIME'] = pd.to_datetime(mat_api_fail['START_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S')
    mat_api_fail['END_TIME'] = pd.to_datetime(mat_api_fail['END_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['START_TIME'] = pd.to_datetime(df['START_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['END_TIME'] = pd.to_datetime(df['END_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    df = pd.concat([df, mat_api_fail], ignore_index=True)
    df['STATUS'] = 'FAILED'
    
    return df

@st.fragment
def render_error_details_fragment():
    """Fragment: Error Details with session_state filters - only this re-renders on filter change."""
    # Load cached data once
    if 'error_details_data' not in st.session_state:
        with st.spinner('Loading error details...'):
            st.session_state['error_details_data'] = fetch_all_error_data()
    
    df = st.session_state['error_details_data'].copy()
    
    # Initialize session state for filters
    if 'error_filters' not in st.session_state:
        st.session_state['error_filters'] = {
            'from_date': (pd.to_datetime(df["START_TIME"].max())).date(),
            'to_date': (pd.to_datetime(df["END_TIME"].max())).date(),
            'source_type': 'ALL',
        }
    
    source_type_options = ['ALL', 'DBT', 'MATILLION']
    status_options = ['FAILED']
    
    with st.container(border=False):
        st.markdown(
            """
            <div>
                <h1 class="hover-effect">
                    Error Details Weekly
                </h1>
            </div>
            """, unsafe_allow_html=True
        )
        st.markdown("")
        
        # Callback for filter changes
        def _update_error_filters():
            st.session_state['error_filters'] = {
                'from_date': st.session_state.get('error_from_date_sel', st.session_state['error_filters']['from_date']),
                'to_date': st.session_state.get('error_to_date_sel', st.session_state['error_filters']['to_date']),
                'source_type': st.session_state.get('error_source_sel', 'ALL'),
            }
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            from_date_time_filter = st.date_input(
                "Select From Date",
                value=st.session_state['error_filters']['from_date'],
                key='error_from_date_sel',
                on_change=_update_error_filters
            )
        
        with col2:
            to_date_time_filter = st.date_input(
                "Select To Date",
                value=st.session_state['error_filters']['to_date'],
                key='error_to_date_sel',
                on_change=_update_error_filters
            )
        
        with col3:
            source_type_filter = st.selectbox(
                "Source Type",
                options=source_type_options,
                index=0,
                key='error_source_sel',
                on_change=_update_error_filters
            )
        
        with col4:
            status_filter = st.selectbox("Status", options=status_options, index=0)
        
        # Use filters from session state
        from_date_val = st.session_state['error_filters']['from_date']
        to_date_val = st.session_state['error_filters']['to_date']
        source_type_val = st.session_state['error_filters']['source_type']
        
        # Apply filters locally (no SQL re-execution)
        if source_type_val == 'ALL':
            df_filtered = df[(pd.to_datetime(df["START_TIME"]).dt.date >= from_date_val) & 
                            (pd.to_datetime(df["END_TIME"]).dt.date <= to_date_val)]
        else:
            df_filtered = df[(pd.to_datetime(df["START_TIME"]).dt.date >= from_date_val) & 
                            (pd.to_datetime(df["END_TIME"]).dt.date <= to_date_val) & 
                            (df['SOURCE_TYPE'] == source_type_val)]
        
        df_filtered = df_filtered.copy()
        df_filtered['LINKS'] = df_filtered.apply(
            lambda row: f"https://13.90.90.241:8443/#CLOUDX/{row['PROJECT_NAME']}/default/{row['SCHEDULE_NAME']}/run/{row['TASK_HISTORY_ID']}" 
            if row['SOURCE_TYPE'] == "MATILLION" else row['LINKS'], axis=1
        )
        
        df_filtered = df_filtered.sort_values(by='START_TIME', ascending=False)
        df_filtered['START_TIME'] = pd.to_datetime(df_filtered['START_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
        df_filtered['END_TIME'] = pd.to_datetime(df_filtered['END_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
        
        column_config1 = {
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
                help="JOB/TAG NAME",
                width="medium"
            ),
            "START_TIME": st.column_config.Column(
                "START TIME (PST)",
                help="Start TIME (PST)",
                width="medium"
            ),
            "END_TIME": st.column_config.Column(
                "END TIME (PST)",
                help="END TIME (PST)",
                width="medium"
            ),
            "TOTAL_RUNTIME_MINUTES": st.column_config.Column(
                "TOTAL RUNTIME (MIN)",
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
            "TRIGGER_BY": st.column_config.Column(
                "TRIGGER BY",
                help="TRIGGER BY",
                width="medium"
            ),
            "REVIEWER_NAME": st.column_config.Column(
                "REVIEWER NAME",
                help="REVIEWER NAME",
                width="medium"
            )
        }
        
        def color_status(val):
            color_map = {
                "SUCCESS": "#5b85fb",
                "FAILED": "#f26271",
                "CANCELLED": "#feb746",
                "RUNNING": "#61d7a1",
                "QUEUED": "#fee8c6",
            }
            return f'background-color: {color_map.get(val, "white")}; font-weight:bold; font-family: Inter, sans-serif;'
        
        df_styled = df_filtered.style.applymap(
            color_status, subset=["STATUS"]
        ).set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#7fcbf0"),
                        ("color", "#000"),
                        ("font-size", "12px"),
                        ("font-family", "Inter, sans-serif"),
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

#session = session
										  
# spinner_placeholder=st.empty()
# with st.spinner ('Loading, please wait...'):
#     #st.session_state.spinner_text='Loading dataframe and charts...'
#     #spinner_placeholder.markdown ("<p style='color: #5D6A85;'>Loading dataframe and charts...</p>", unsafe_allow_html=True) 
#     cl1, cl2, cl3 = st.columns ([4,1,1])
#     with cl3:
#         st.image("assets/logo_cloudeqs.png", width=200)



with st.container(border=False):
    st.markdown(
        """
        <div>
            <h1 class="hover-effect">
                Error Details Weekly
            </h1>
        </div>
        """, unsafe_allow_html=True
    )
    st.markdown("")



local_time = time.localtime()
gmt_end_time = datetime.now(timezone.utc)
gmt_start_time = datetime.now(timezone.utc) - timedelta (hours=24)

dbt_api_failed_df = dbt_failed_jobs(20, gmt_start_time,gmt_end_time) 
# st.dataframe (dbt_api_failed_df)

dbt_df=failed_dbt_data()

df=pd.concat([dbt_df, dbt_api_failed_df], ignore_index=True) 
df=df.drop_duplicates (subset=['TASK_HISTORY_ID'])
# st.dataframe (df)

mat_df=mat_failed_df()
#st.dataframe (mat_df)
df=pd.concat([df, mat_df], ignore_index=True)

mat_api_fail=mat_running_run_and_queue()
if not mat_api_fail.empty and 'START_TIME' in mat_api_fail.columns:
    mat_api_fail['START_TIME'] = pd.to_datetime(mat_api_fail['START_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S') 
    mat_api_fail['END_TIME'] = pd.to_datetime(mat_api_fail['END_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['START_TIME'] = pd.to_datetime(df['START_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S') 
df['END_TIME'] = pd.to_datetime(df['END_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S')

df=pd.concat([df, mat_api_fail], ignore_index=True)
# st.dataframe (mat_api_fail)

source_type_options = ['ALL', 'DBT', 'MATILLION'] #+ list(df['SOURCE_TYPE'].unique())
status_options = ['FAILED']
#comment_options = ['ALL']+ list(df['COMMENTS'].unique())

df['STATUS']='FAILED'

# st.dataframe(df)
with st.container(border=False):
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1], gap="small")

    with col1:
        from_date_time_filter = st.date_input(
            "Select From Date", (pd.to_datetime(df["START_TIME"].max())).date()
        )

    with col2:
        to_date_time_filter = st.date_input(
            "Select To Date", (pd.to_datetime(df["END_TIME"].max())).date()
        )

    with col3:
        source_type_filter = st.selectbox("Source Type", options=source_type_options, index=0)

    with col4:
        status_filter = st.selectbox("Status", options=status_options, index=0)



    if source_type_filter=='ALL':
        df_filtered = df[(pd.to_datetime(df["START_TIME"]).dt.date >= from_date_time_filter) & (pd.to_datetime(df["END_TIME"]).dt.date <= to_date_time_filter)]
    else:
        df_filtered = df[
            (pd.to_datetime(df["START_TIME"]).dt.date >= from_date_time_filter) & (pd.to_datetime(df["END_TIME"]).dt.date <= to_date_time_filter) & (df['SOURCE_TYPE'] == source_type_filter)]


    df_filtered['LINKS'] = df_filtered.apply(
        lambda row: f"https://13.90.90.241:8443/#CLOUDX/{row['PROJECT_NAME']}/default/{row['SCHEDULE_NAME']}/run/{row['TASK_HISTORY_ID']}" 
        if row['SOURCE_TYPE'] == "MATILLION" 
        else row['LINKS'],
        axis=1
    )

    df_filtered = df_filtered.sort_values(by='START_TIME', ascending=False)

    df_filtered['START_TIME'] = pd.to_datetime(df_filtered['START_TIME'])
    df_filtered["START_TIME"] = df_filtered["START_TIME"].dt.strftime(
            "%B %d, %Y at %I:%M %p"
        )
    df_filtered['END_TIME'] = pd.to_datetime(df_filtered['END_TIME'])
    df_filtered["END_TIME"] = df_filtered["END_TIME"].dt.strftime(
            "%B %d, %Y at %I:%M %p"
        )
    
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
            return f'background-color: {color_map.get(val, "white")};font-family: Inter, sans-serif;'
    
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


# with st.container(border=False):
#     st.markdown(
#         """
#         <div>
#             <h1 style="font-family: Inter, sans-serif; font-size: 22px; text-align: left;">
#                 Failures(By Source Type)
#             </h1>
#         </div>
#         """, unsafe_allow_html=True
#     )

# df["START_TIME"] = pd.to_datetime(df["START_TIME"], errors="coerce")


# time_range = st.selectbox(
#     "Select Time Range",
#     ["Overall", "Last 7 Days", "Last 30 Days", "Last 90 Days"]
# )

# if time_range == "Last 7 Days":
#     start_date = datetime.now() - timedelta(days=7)
#     df_failed = df[(df["STATUS"] == "FAILED") & (df["START_TIME"] >= start_date)]
# elif time_range == "Last 30 Days":
#     start_date = datetime.now() - timedelta(days=30)
#     df_failed = df[(df["STATUS"] == "FAILED") & (df["START_TIME"] >= start_date)]
# elif time_range == "Last 90 Days":
#     start_date = datetime.now() - timedelta(days=90)
#     df_failed = df[(df["STATUS"] == "FAILED") & (df["START_TIME"] >= start_date)]
# else:  # Overall
#     df_failed = df[df["STATUS"] == "FAILED"]

# failure_counts = df_failed["SOURCE_TYPE"].value_counts()

# col1, col2 = st.columns([2, 2])  # col1 = narrow, col2 = wide

# with col1:
#     fig, ax = plt.subplots(figsize=(3, 3))  
#     ax.pie(
#         failure_counts, 
#         labels=failure_counts.index, 
#         autopct='%0.1f%%', 
#         startangle=90
#     )
#     # ax.set_title("Failures by Source Type", fontsize=10)
#     st.pyplot(fig)