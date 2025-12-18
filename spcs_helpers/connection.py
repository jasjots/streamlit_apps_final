import os
import snowflake.connector
from snowflake.snowpark import Session
import streamlit as st

def get_login_token():
    with open('/snowflake/session/token', 'r') as f:
        return f.read()

@st.cache_resource
def connection(user_token: str = None) -> snowflake.connector.SnowflakeConnection:
    if os.path.isfile('/snowflake/session/token'):
        token = get_login_token()
        if user_token:
            token = f"{token}.{user_token}"    
        creds = {
            'host': os.getenv('SNOWFLAKE_HOST'),
            'port': os.getenv('SNOWFLAKE_PORT'),
            'protocol': 'https',
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'authenticator': 'oauth',
            'token': token,
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }
    else:
        creds = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }
    connection = snowflake.connector.connect(**creds)
    return connection

@st.cache_resource
def session(user_token: str = None) -> Session:
    if os.path.isfile('/snowflake/session/token'):
        return Session.builder.configs({'connection': connection()}).create()

    return st.connection(
        "MY_CONNECTION_WITH_SSO",
        type="snowflake",
        authenticator="externalbrowser",
        user="jasjots@cloudeqs.com",
        account="KHA80474"
    ).session()