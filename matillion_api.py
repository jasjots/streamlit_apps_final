import os
import time
import requests
import streamlit as st
from dotenv import load_dotenv
from spcs_helpers.connection import session

session=session()
# -------------------------
# Setup
# -------------------------
load_dotenv()

REGION_BASE = "https://13.90.90.241:8443"
#"https://us1.api.matillion.com"
API_BASE = f"{REGION_BASE}/rest/v1/project"
#/dpc/v1"


# -------------------------
# Auth & low‑level helpers
# -------------------------
def get_access_token(session):
    """
    Fetch Matillion access token from Snowflake using Snowpark session.
    Calls:
        EDW_LAB_DEV.OBSERVABILITY.MATILLION_ACCESS_TOKEN()
    """

    try:
        df = session.sql(
            "SELECT EDW_LAB_DEV.OBSERVABILITY.MATILLION_ACCESS_TOKEN() AS TOKEN"
        ).collect()

        if df and df[0].TOKEN:
            return df[0].TOKEN
        else:
            st.error("❌ Snowflake function returned NULL token.")
            return None

    except Exception as e:
        st.error(f"❌ Error fetching access token from Snowflake: {e}")
        return None



@st.cache_data(ttl=900)
def auth_headers():
    token = get_access_token(session)
    if not token:
        return None
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def api_get(path, params=None):
    headers = auth_headers()
    if not headers:
        return None
    try:
        # Convert params dict → JSON string
        import json
        params_json = json.dumps(params) if params else None

        query = f"""
            SELECT EDW_LAB_DEV.OBSERVABILITY.API_GET_REQUEST(
                '{API_BASE}'
            ) AS response
        """

        df = session.sql(query).collect()

        if df and df[0].RESPONSE:
            return df[0].RESPONSE  # Snowflake VARIANT → Python dict
        else:
            st.error("❌ API function returned no response.")
            return None

    except Exception as e:
        st.error(f"❌ Snowflake API_GET_REQUEST failed: {e}")
        return None

def api_post(path, json_body):
    headers = auth_headers()
    if not headers:
        return None
    try:
        # Convert params dict → JSON string
        import json
        params_json = json.dumps(params) if params else None

        query = f"""
            SELECT EDW_LAB_DEV.OBSERVABILITY.API_POST_REQUEST(
                '{API_BASE}'
                
            ) AS response
        """

        df = session.sql(query).collect()

        if df and df[0].RESPONSE:
            return df[0].RESPONSE  # Snowflake VARIANT → Python dict
        else:
            st.error("❌ API function returned no response.")
            return None

    except Exception as e:
        st.error(f"❌ Snowflake API_POST_REQUEST failed: {e}")
        return None


def api_patch(path, json_body):
    headers = auth_headers()
    if not headers:
        return None
    try:
        # Convert params dict → JSON string
        import json
        params_json = json.dumps(params) if params else None

        query = f"""
            SELECT EDW_LAB_DEV.OBSERVABILITY.API_PATCH_REQUEST(
                '{API_BASE}'
            ) AS response
        """

        df = session.sql(query).collect()

        if df and df[0].RESPONSE:
            return df[0].RESPONSE  # Snowflake VARIANT → Python dict
        else:
            st.error("❌ API function returned no response.")
            return None

    except Exception as e:
        st.error(f"❌ Snowflake API_PATCH_REQUEST failed: {e}")
        return None


# -------------------------
# DPC API wrappers (reused)
# -------------------------
@st.cache_data(ttl=300)
def list_projects():
    data = api_get("/projects")
    return data.get("results", []) if data else []


@st.cache_data(ttl=300)
def list_environments(project_id: str):
    data = api_get(f"/projects/{project_id}/environments")
    return data.get("results", []) if data else []


@st.cache_data(ttl=120)
def list_published_pipelines(project_id: str, env_name: str, page_size: int = 100, max_pages: int = 1000):
    all_results, page = [], 0
    while True:
        data = api_get(
            f"/projects/{project_id}/published-pipelines"
            f"?environmentName={env_name}&page={page}&size={page_size}"
        ) or {}
        results = data.get("results", [])
        all_results.extend(results)

        if data.get("last") is True:
            break
        total_pages = data.get("totalPages")
        if isinstance(total_pages, int) and page + 1 >= total_pages:
            break
        if len(results) < page_size:
            break

        page += 1
        if page >= max_pages:
            break

    return sorted(all_results, key=lambda r: (r.get("name") or "").lower())


def execute_pipeline(project_id: str, environment_name: str, pipeline_name: str, execution_tag: str | None = None):
    body = {"pipelineName": pipeline_name, "environmentName": environment_name}
    if execution_tag:
        body["executionTag"] = execution_tag
    return api_post(f"/projects/{project_id}/pipeline-executions", body)


def get_execution_status(project_id: str, pipeline_execution_id: str):
    return api_get(f"/projects/{project_id}/pipeline-executions/{pipeline_execution_id}")


def cancel_execution(project_id: str, pipeline_execution_id: str):
    body = {"forceUpdate": True, "status": "TERMINATED"}
    return api_patch(f"/projects/{project_id}/pipeline-executions/{pipeline_execution_id}", body)


def recent_executions(project_id: str, pipeline_name: str, days: int = 1, limit: int = 100):
    params = {"projectId": project_id, "pipelineName": pipeline_name, "timeFrame": f"P{days}D", "limit": limit}
    url = f"{API_BASE}/pipeline-executions"
    headers = auth_headers()
    if not headers:
        return []
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json().get("results", [])


# Pagination helper used by Dashboard
def list_pipeline_executions(params: dict):
    """Fetch all pipeline executions using 'more' pagination token."""
    headers = auth_headers()
    if not headers:
        return []

    api_url = f"{API_BASE}/pipeline-executions"
    all_results, pagination_token = [], None

    while True:
        paged = params.copy()
        if pagination_token:
            paged["paginationToken"] = pagination_token

        resp = requests.get(api_url, headers=headers, params=paged)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        all_results.extend(results)

        pagination_token = data.get("more")
        if not pagination_token:
            break

    return all_results
