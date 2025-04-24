import pandas as pd
import streamlit as st
import snowflake.connector
from io import StringIO
import os, io
import lib.utils.session  # Helper for session state
import lib.snowflake.notifications as nc
#from lib.snowflake.snowflake_stage_manager import SnowflakeStageManager
#STAGEMANAGER=SnowflakeStageManager(SESSION)

#st.set_page_config(layout="wide")   

lib.utils.session.initSnowflake()

SESSION=st.session_state.snowflakesession.session
if "message" not in st.session_state:
    st.session_state.message = ""
    
    
    
import streamlit as st
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from typing import Optional

SESSION=st.session_state.snowflakesession.session
# Function to list tables in the database
def list_tables(session: Session) -> list[str]:
    """
    Retrieves a list of table names from the Snowflake database.

    Args:
        session (Session): The active Snowpark session.

    Returns:
        list[str]: A list of table names, or an empty list in case of an error.
    """
    try:
        tables = SESSION.sql("SHOW TABLES").collect()
        table_names = [table["name"] for table in tables]  # Extract table names
        return table_names
    except Exception as e:
        st.error(f"Error listing tables: {e}")
        return []

# Function to display the content of a selected table
def display_table_content(session: Session, table_name: str):
    """
    Displays the content of a selected table using st.dataframe.

    Args:
        session (Session): The active Snowpark session.
        table_name (str): The name of the table to display.
    """
    try:
        df = session.sql(f"select * FROM {table_name}").to_pandas()
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error displaying table content for {table_name}: {e}")

def main():
    """
    Snowflake Stage Manager
    """
    # Set the page title
    st.set_page_config(page_title="Snowflake Table Viewer", layout="wide")
    if not SESSION:
        return  # Stop if session creation failed

    # List tables
    table_names = list_tables(SESSION)

    # Select a table using a dropdown
    if table_names:
        selected_table = st.selectbox("Select a table to view:", table_names)

        # Display the content of the selected table
        display_table_content(SESSION, selected_table)
    else:
        st.info("No tables found in the specified schema.")

if __name__ == "__main__":
    main()
