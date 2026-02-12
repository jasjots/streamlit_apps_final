
import streamlit as st 
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import plotly.express as px
import io
import altair as alt
from spcs_helpers.connection import session

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
load_css("CSS/monthly_job.css")


from sidebar import render_sidebar
render_sidebar()
#from snowflake.snowpark.context import get_active_session

# Streamlit app configuration
st.set_page_config(
    page_title="Monthly Job Details",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "About": "# This is demo application for observability project"
    }
)

with st.container(border=False):
    st.markdown(
        """
        <div>
            <h1 class="hover-effect">
                Job Details Monthly
            </h1>
        </div>
        """, unsafe_allow_html=True
    )


# from sidebar import render_sidebar
# render_sidebar()

# Data loading functions

def get_monthly_jobs_data():
    """Query for monthly job statistics (DBT + Matillion)."""
    month_year_query = """
SELECT
    rr.model_execution_id AS TASK_HISTORY_ID,
    di.project_name AS PROJECT_NAME,
    upper(rr.NAME) AS SCHEDULE_NAME,
    LISTAGG(di.SELECTED, ',') AS TAG,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rr.COMPILE_STARTED_AT) AS START_TIME,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rr.EXECUTE_COMPLETED_AT) AS END_TIME, 
    listagg(distinct rr.status,',') status,
    TIMESTAMPDIFF('minute',START_TIME, END_TIME) AS TOTAL_RUNTIME_MINUTES,
    MONTHNAME(START_TIME) as JOB_MONTH,
    YEAR(START_TIME) as JOB_YEAR, 
    rr.message AS ERROR_MESSAGE,
    'DBT' AS SOURCE_TYPE, 
    di.job_url AS LINKS,
    di.cause AS TRIGGER_BY
FROM
    EDW_LAB_DEV.OBSERVABILITY.DBT_RUN_RESULTS rr
INNER JOIN
    EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS di
    ON di.invocation_id = rr.INVOCATION_ID
WHERE
    di.env='prod' and di.cause_category='scheduled' and rr.resource_type in ('model', 'snapshot', 'seed')
    and START_TIME >= DATE_TRUNC('YEAR', DATEADD('YEAR', -2, CURRENT_TIMESTAMP()))
    group by TASK_HISTORY_ID, PROJECT_NAME, SCHEDULE_NAME,START_TIME, END_TIME, TOTAL_RUNTIME_MINUTES, JOB_MONTH, ERROR_MESSAGE, SOURCE_TYPE, LINKS, TRIGGER_BY

UNION ALL

SELECT
    rh.TASK_HISTORY_ID,
    rh.PROJECT_NAME,
    upper(sd.name) AS SCHEDULE_NAME,
    '-' AS TAG,
    rh.START_TIME_PST AS START_TIME,
    rh.END_TIME_PST AS END_TIME,
    rh.STATE AS STATUS,
    TIMESTAMPDIFF('minute',START_TIME, END_TIME) AS TOTAL_RUNTIME_MINUTES,
    MONTHNAME(rh.START_TIME_PST) as JOB_MONTH,
    YEAR(rh.START_TIME_PST) as JOB_YEAR,
    rh.MESSAGE AS ERROR_MESSAGE,
    'MATILLION' AS SOURCE_TYPE,
    NULL AS LINKS,
    NULL AS TRIGGER_BY
FROM
    EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY rh
JOIN
    EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS sd
    ON rh.PROJECT_NAME = sd.PROJECT AND
        rh.JOB_NAME = sd.JOB_NAME
WHERE
    enabled=true and day_of_week=true and run_date=(select max(run_date) from EDW_LAB_DEV.OBSERVABILITY.matillion_schedules_details) and 
    rh.JOBTYPE = 'SCHEDULE ORCHESTRATION'
    AND START_TIME >= DATE_TRUNC ("YEAR", DATEADD('YEAR', -2, CURRENT_TIMESTAMP()))
ORDER BY
    END_TIME DESC;
    """
    return session.sql(month_year_query).to_pandas()


def get_monthly_trend_data():
    """Query for longest running jobs per month."""
    query_for_each_month = """
WITH dbt_job_data AS (
    SELECT distinct
        upper(rr.NAME) AS SCHEDULE_NAME,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rr.COMPILE_STARTED_AT) AS START_TIME, 
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rr.EXECUTE_COMPLETED_AT) AS END_TIME, 
        TIMESTAMPDIFF('minute',START_TIME, END_TIME) AS TOTAL_RUNTIME_MINUTES,
        MONTHNAME(START_TIME) as JOB_MONTH,
        YEAR(START_TIME) as JOB_YEAR
    FROM
        EDW_LAB_DEV.OBSERVABILITY.DBT_RUN_RESULTS rr
    INNER JOIN
        EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS di
    ON di.invocation_id = rr.INVOCATION_ID
    WHERE
        di.env='prod' and di.cause_category='scheduled' and rr.resource_type in ('model', 'snapshot', 'seed') 
        and START_TIME >= DATE_TRUNC('YEAR', DATEADD('YEAR', -2, CURRENT_TIMESTAMP()))
),
mat_job_data AS (
    SELECT distinct
        upper(sd.name) AS SCHEDULE_NAME, 
        rh.START_TIME_PST AS START_TIME,
        rh.END_TIME_PST AS END_TIME,
        TIMESTAMPDIFF('minute',START_TIME, END_TIME) AS TOTAL_RUNTIME_MINUTES, 
        MONTHNAME(rh.START_TIME_PST) as JOB_MONTH,
        YEAR(rh.START_TIME_PST) as JOB_YEAR
    FROM
        EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY rh
    JOIN
        EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS sd
        ON rh.PROJECT_NAME = sd.PROJECT AND
            rh.JOB_NAME = sd.JOB_NAME
    WHERE
        enabled=true and day_of_week=true and run_date=(select max(run_date) from EDW_LAB_DEV.OBSERVABILITY.matillion_schedules_details) and 
        rh.JOBTYPE = 'SCHEDULE ORCHESTRATION'
        AND START_TIME >= DATE_TRUNC('YEAR', DATEADD('YEAR', -2, CURRENT_TIMESTAMP()))
)
SELECT
SCHEDULE_NAME,
JOB_MONTH,
JOB_YEAR,
max(TOTAL_RUNTIME_MINUTES) AS max_execution_time_minute
FROM
dbt_job_data
GROUP BY
SCHEDULE_NAME, JOB_MONTH, JOB_YEAR
UNION
SELECT
SCHEDULE_NAME,
JOB_MONTH,
JOB_YEAR,
max(TOTAL_RUNTIME_MINUTES) AS max_execution_time_minute
FROM
mat_job_data
GROUP BY
SCHEDULE_NAME, JOB_MONTH, JOB_YEAR
ORDER BY
max_execution_time_minute DESC;
    """
    return session.sql(query_for_each_month).to_pandas()


def get_job_execution_stats():
    """Query for job execution counts and average execution times."""
    job_execution_count = """
WITH dbt_job_data AS (
    SELECT
        rr.NAME AS SCHEDULE_NAME,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rr.COMPILE_STARTED_AT) AS START_TIME, 
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rr.EXECUTE_COMPLETED_AT) AS END_TIME, 
        TIMESTAMPDIFF("minute",
                       CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rr.COMPILE_STARTED_AT), 
                       CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rr.EXECUTE_COMPLETED_AT)  
        ) AS TOTAL_RUNTIME_MINUTES,
        MONTHNAME(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rr.COMPILE_STARTED_AT)) AS JOB_MONTH, 
        YEAR(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rr.COMPILE_STARTED_AT)) AS JOB_YEAR
    FROM
        EDW_LAB_DEV.OBSERVABILITY.DBT_RUN_RESULTS rr
    INNER JOIN
        EDW_LAB_DEV.OBSERVABILITY.DBT_INVOCATIONS di
        ON di.invocation_id = rr.INVOCATION_ID
    WHERE
        di.env='prod'
        AND di.cause_category='scheduled'
        AND rr.resource_type IN ('model', 'snapshot', 'seed')
        AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rr.COMPILE_STARTED_AT) >= DATE_TRUNC('YEAR', DATEADD('YEAR', -2, CURRENT_TIMESTAMP()))
)
SELECT
    SCHEDULE_NAME,
    JOB_MONTH,
    JOB_YEAR,
    COUNT(*) AS execution_count,
    AVG(TOTAL_RUNTIME_MINUTES) AS avg_execution_time_seconds
FROM
    dbt_job_data
GROUP BY
    SCHEDULE_NAME, JOB_MONTH, JOB_YEAR
UNION
SELECT
    SCHEDULE_NAME,
    JOB_MONTH, 
    JOB_YEAR,
    COUNT(0) AS execution_count,
    SUM(TOTAL_RUNTIME_MINUTES/68.8)/COUNT(8) AS avg_execution_time_seconds
FROM
(
    SELECT
        task_history_id,
        sd.name AS SCHEDULE_NAME,
        rh.START_TIME_PST AS START_TIME,
        rh.END_TIME_PST AS END_TIME,
        TIMESTAMPDIFF('second', rh.START_TIME_PST, rh.END_TIME_PST) AS TOTAL_RUNTIME_MINUTES, 
        MONTHNAME(rh.START_TIME_PST) AS JOB_MONTH,
        YEAR(rh.START_TIME_PST) AS JOB_YEAR
    FROM
        EDW_LAB_DEV.OBSERVABILITY.RUN_HISTORY_SUMMARY rh
    JOIN
        EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS sd
        ON rh.PROJECT_NAME = sd.PROJECT
        AND rh.JOB_NAME = sd.JOB_NAME
    WHERE
        enabled=true
        AND day_of_week=true
        AND run_date=(SELECT MAX(run_date) FROM EDW_LAB_DEV.OBSERVABILITY.MATILLION_SCHEDULES_DETAILS) 
        AND rh.JOBTYPE = 'SCHEDULE ORCHESTRATION'
) subquery
GROUP BY
    SCHEDULE_NAME, JOB_MONTH, JOB_YEAR
ORDER BY
    execution_count DESC;
    """
    return session.sql(job_execution_count).to_pandas()



def fetch_all_monthly_data():
    """Master loader - combines all monthly data sources."""
    # Optional: these st.write will show in UI only if this runs inside app execution
    # st.write("Running SQL: get_monthly_jobs_data (called)")

    monthly_df = get_monthly_jobs_data()
    longest = get_monthly_trend_data()
    job_count = get_job_execution_stats()

    # print("fetch_all_monthly_data executed")
    return monthly_df, longest, job_count


@st.fragment
def render_status_pie_fragment(filtered_df, selected_month, selected_year):
    filtered_df["STATUS"] = filtered_df["STATUS"].astype(str).str.strip().str.upper()

    successful_jobs = filtered_df[filtered_df['STATUS'] == 'SUCCESS'].shape[0]
    failed_jobs = filtered_df[filtered_df['STATUS'] == 'FAILED'].shape[0]
    cancelled_jobs = filtered_df[filtered_df['STATUS'] == 'CANCELLED'].shape[0]

    st.markdown(
        f"""
        <div style="text-align: left;">
            <h1 style="color: #5D6A85; font-size: 18px; margin: 6;">
                Job Status count in {selected_month} {selected_year}
            </h1>
        </div>
        """,
        unsafe_allow_html=True,
    )

    fig, ax = plt.subplots(figsize=(4, 4))
    labels = ["Success", "Failed", "Cancelled"]
    sizes = [successful_jobs, failed_jobs, cancelled_jobs]
    colors = ["#5b85fb", "#f26271", "#feb746"]

    if sum(sizes) == 0:
        st.write("No data to display in the pie chart.")
        # ✅ Debug: show what STATUS values exist
        st.write("STATUS unique:", filtered_df["STATUS"].dropna().unique())
    else:
        wedges, texts = ax.pie(sizes, colors=colors, startangle=160, wedgeprops=dict(width=1))
        for i, txt in enumerate(texts):
            txt.set_text(f"{labels[i]}: {sizes[i]}")
        ax.legend(wedges, labels, title="Job Status", loc="upper left", bbox_to_anchor=(1, 0, 1, 1))
        buf = io.BytesIO()
        plt.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        st.image(buf)


@st.fragment
def render_monthly_fragment():
    if 'monthly_filters' not in st.session_state:
        st.session_state.monthly_filters = {'year': 'All', 'month': 'All'}

    def _update_filters():
        st.session_state.monthly_filters['year'] = st.session_state.year_select
        st.session_state.monthly_filters['month'] = st.session_state.month_select

    monthly_df, longest, job_count = fetch_all_monthly_data()

    # Apply your existing filters (same logic)
    selected_year = st.session_state.monthly_filters['year']
    selected_month = st.session_state.monthly_filters['month']

    filtered_df = monthly_df.copy()
    if selected_year != "All":
        filtered_df = filtered_df[filtered_df["JOB_YEAR"] == int(selected_year)]
    if selected_month != "All":
        filtered_df = filtered_df[filtered_df['JOB_MONTH'] == selected_month]

    # ✅ Debug must be AFTER filtered_df exists
    st.write("monthly_df rows:", monthly_df.shape[0])
    st.write("filtered_df rows:", filtered_df.shape[0])
    st.write("Selected year/month:", selected_year, selected_month)
    if filtered_df.shape[0] > 0:
        st.write("STATUS unique:", filtered_df["STATUS"].dropna().unique())

    # ✅ Call the pie fragment (important)
    render_status_pie_fragment(filtered_df, selected_month, selected_year)



# -------------------- Fragment 2: Top Long Running Jobs (same code moved) --------------------
@st.fragment
def render_top_long_running_fragment(filtered_df, selected_month, selected_year):
    unique_jobs_df = filtered_df.groupby(["SCHEDULE_NAME", "SOURCE_TYPE"]).agg({"TOTAL_RUNTIME_MINUTES": "max"}).reset_index()
    unique_jobs_df["SOURCE_TYPE"] = unique_jobs_df["SOURCE_TYPE"].replace({"MATILLION": "MT", "DBT": "T"})
    top_10_long_running_jobs = unique_jobs_df.nlargest(18, "TOTAL_RUNTIME_MINUTES")

    fig = px.bar(
        top_10_long_running_jobs,
        x='SCHEDULE_NAME',
        y='TOTAL_RUNTIME_MINUTES',
        color='SCHEDULE_NAME',
        title=f"Top 10 Long Running jobs in {selected_month} {selected_year}",
        labels={'TOTAL_RUNTIME_MINUTES': 'Total Runtime (Minutes)', 'JOB_MONTH': ''},
        text='SOURCE_TYPE'
    )
    fig.update_layout(
        yaxis_title="Total Runtime Minutes",
        legend_title="Schedule Name",
        bargap=0.1,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis_showticklabels=False,
        title_font=dict(size=20, color='#5D6A85')
    )
    fig.update_traces(textposition='outside', textfont_size=12)
    st.plotly_chart(fig)


# -------------------- Fragment 3: Top Execution Count Table (same code moved) --------------------
@st.fragment

@st.fragment
def render_exec_count_table_fragment(job_count, selected_month, selected_year):
    st.markdown(
        f"""
        <div style="text-align: left;">
            <h1 style="color: #5D6A85; font-size: 18px; margin: 0;">
                Top 10 jobs with highest execution counts in {selected_month} {selected_year}
            </h1>
        </div>
        """,
        unsafe_allow_html=True
    )

    # st.write("selected_month:", selected_month, "selected_year:", selected_year)
    # st.write("JOB_YEAR dtype:", job_count["JOB_YEAR"].dtype)
    # st.write("JOB_MONTH sample:", job_count["JOB_MONTH"].dropna().unique()[:12])

   
    job_count["JOB_MONTH"] = job_count["JOB_MONTH"].astype(str).str.strip()

    job_count_filtered = job_count

    if selected_month != "All":
        job_count_filtered = job_count_filtered[job_count_filtered["JOB_MONTH"] == selected_month]

    if selected_year != "All":
        job_count_filtered = job_count_filtered[job_count_filtered["JOB_YEAR"] == int(selected_year)]

    st.write("Filtered rows:", job_count_filtered.shape[0])

    df = job_count_filtered.nlargest(10, "EXECUTION_COUNT")
    final_df = df[["SCHEDULE_NAME", "EXECUTION_COUNT", "AVG_EXECUTION_TIME_SECONDS"]]

    column_config1 = {
        "SCHEDULE_NAME": st.column_config.Column("SCHEDULE NAME", help="SCHEDULE NAME", width="medium"),
        "EXECUTION_COUNT": st.column_config.Column("EXECUTION COUNT", help="EXECUTION COUNT", width="medium"),
        "AVG_EXECUTION_TIME_SECONDS": st.column_config.Column("AVG EXECUTION TIME (SEC)", help="AVG EXECUTION TIME SECONDS", width="medium"),
    }

    st.dataframe(final_df, column_config=column_config1, hide_index=True)




@st.fragment
def render_longest_each_month_fragment(longest, selected_year, month_order):
    # Bottom charts
    if selected_year != "All":
        year_filtered_df = longest[longest["JOB_YEAR"] == int(selected_year)]
    else:
        year_filtered_df = longest

    
    df2 = year_filtered_df.dropna(subset=['MAX_EXECUTION_TIME_MINUTE'])
    idx = df2.groupby('JOB_MONTH')['MAX_EXECUTION_TIME_MINUTE'].idxmax()
    longest_running_each_month = df2.loc[idx].reset_index(drop=True)


    available_months = sorted(longest_running_each_month['JOB_MONTH'].unique(), key=lambda x: month_order.index(x))
    longest_running_each_month = longest_running_each_month.set_index("JOB_MONTH").loc[available_months].reset_index()

    fig = px.bar(
        longest_running_each_month,
        x='JOB_MONTH',
        y='MAX_EXECUTION_TIME_MINUTE',
        color='SCHEDULE_NAME',
        barmode='group',
        title=f"Longest Running Job for Each Month in {selected_year}",
        labels={'MAX_EXECUTION_TIME_MINUTE': 'Max Runtime (Minutes)', 'JOB_MONTH': 'Job Month'},
    )
    fig.update_layout(
        xaxis_title="Job Month",
        yaxis_title="MAX_EXECUTION_TIME_MINUTE",
        legend_title="Schedule Name",
        bargap=0.1,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        title_font=dict(size=20, color='#5D6A85')
    )
    st.plotly_chart(fig)


# -------------------- Your main fragment: same code, now calling isolated fragments --------------------
@st.fragment
def render_monthly_fragment():
    """Fragment UI with session state filters for monthly job details."""

    # Initialize session state for filters
    if 'monthly_filters' not in st.session_state:
        st.session_state.monthly_filters = {
            'year': 'All',
            'month': 'All'
        }

    def _update_filters():
        st.session_state.monthly_filters['year'] = st.session_state.year_select
        st.session_state.monthly_filters['month'] = st.session_state.month_select



    # Load all data once (cached)
    monthly_df, longest, job_count = fetch_all_monthly_data()

    # Filter section
    col1, col2 = st.columns([3, 3])
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    with col1:
        unique_years = sorted(monthly_df['JOB_YEAR'].unique().tolist())
        st.selectbox("Select Year", ["All"] + unique_years, key='year_select', on_change=_update_filters)

    with col2:
        selected_year = st.session_state.monthly_filters['year']
        if selected_year != "All":
            filtered_year_df = monthly_df[monthly_df['JOB_YEAR'] == int(selected_year)]
        else:
            filtered_year_df = monthly_df
        unique_months_for_year = sorted(filtered_year_df['JOB_MONTH'].unique(), key=lambda x: month_order.index(x))
        st.selectbox("Select Month", ["All"] + unique_months_for_year, key='month_select', on_change=_update_filters)

    # Apply filters (local filtering - same logic)
    selected_year = st.session_state.monthly_filters['year']
    selected_month = st.session_state.monthly_filters['month']

    filtered_df = monthly_df.copy()
    if selected_year != "All":
        filtered_df = filtered_df[filtered_df["JOB_YEAR"] == int(selected_year)]
    if selected_month != "All":
        filtered_df = filtered_df[filtered_df['JOB_MONTH'] == selected_month]

    # Charts (layout same, now chart bodies are isolated fragments)
    col1, col2 = st.columns([3, 3])

    with col1:
        render_status_pie_fragment(filtered_df, selected_month, selected_year)

    with col2:
        render_top_long_running_fragment(filtered_df, selected_month, selected_year)

    chart1, chart2 = st.columns([1, 1])

    with chart1:
        render_exec_count_table_fragment(job_count, selected_month, selected_year)

    with chart2:
        render_longest_each_month_fragment(longest, selected_year, month_order)

    
    # with chart2:
    #     fig = px.bar(
    #         longest_running_each_month,
    #         x='JOB_MONTH',
    #         y='MAX_EXECUTION_TIME_MINUTE',
    #         color='SCHEDULE_NAME',
    #         barmode='group',
    #         title=f"Longest Running Job for Each Month in {selected_year}",
    #         labels={'MAX_EXECUTION_TIME_MINUTE': 'Max Runtime (Minutes)', 'JOB_MONTH': 'Job Month'},
    #     )
    #     fig.update_layout(
    #         xaxis_title="Job Month",
    #         yaxis_title="MAX_EXECUTION_TIME_MINUTE",
    #         legend_title="Schedule Name",
    #         bargap=0.1,
    #         plot_bgcolor='rgba(0,0,0,0)',
    #         paper_bgcolor='rgba(0,0,0,0)',
    #         title_font=dict(size=20, color='#5D6A85')
    #     )
    #     st.plotly_chart(fig)


# Main page rendering
st.markdown("""<style>div[data-testid="stAppViewContainer"] { padding: 0; } .stAppViewContainer > div { margin: 0; } .stAppViewContainer > div > div { margin: 0; }</style>""", unsafe_allow_html=True)

# # Logo
# cl1, cl2, cl3 = st.columns([4, 1, 1])
# with cl3:
#     st.image("assets/logo_cloudeqs.png", width=200)

# Render the monthly job details fragment
render_monthly_fragment()
