#Manager for the session state of the applications
import streamlit as st
from lib.snowflake.snowflake_session_manager import SnowflakeSessionManager

def initSnowflake(path="./secrets/configuration.toml", profile="snowflake"):
    if "snowflakesession" not in st.session_state:
        st.session_state.snowflakesession = SnowflakeSessionManager(path, profile)
