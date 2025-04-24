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
if "filelist" not in st.session_state:
    st.session_state.filelist = ""

if "staged_file_list" not in st.session_state:
    st.session_state.staged_file_list=None

if 'stage_selection' not in st.session_state:
    st.session_state.stage_selection = 0
    
# Initialize notification center
notification_center = nc.NotificationCenter()

def upload_file_to_stage(files, stage_name):
    """
    Uploads a file-like object to a Snowflake stage.

    Args:
        conn: Snowflake connection object.
        file:  A file-like object (e.g., BytesIO, TextIOWrapper) or a string representing the file path.
        stage_name: The name of the Snowflake stage.
        path: The path within the stage (optional).

    Returns:
        str: The full path of the file in the stage if upload is successful, None otherwise.
    """
    #with st.spinner("Please Wait....", show_time=True):
    for file in files:
        try:
            # Create file stream using BytesIO and upload
            file_stream = io.BytesIO(file.getvalue())
            SESSION.file.put_stream(
                file_stream,
                f"{stage_name}/{file.name}",
                auto_compress=False,
                overwrite=True,
            )
            notification_center.add_notification(nc.Message("success", f"File '{file.name}' has been uploaded successfully!", 1000))
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
            return None
        finally:
            pass


def on_change_stage_list():
    """
    Lists files in a Snowflake stage.

    Args:
        conn: Snowflake connection object.
        stage_name: The name of the Snowflake stage.
        path: The path within the stage (optional).

    Returns:
        list: A list of dictionaries, where each dictionary contains file information
            (name, size, etc.), or None on error.
    """
    stage_name=st.session_state.stage_name
    path=""
    try:
        full_stage_path = f"@{stage_name}/{path}" if path else f"@{stage_name}"
        if SESSION:
            results = SESSION.sql(f"LIST {full_stage_path}").collect()
            st.session_state.staged_file_list=pd.DataFrame(results)
        return results
    except snowflake.connector.errors.ProgrammingError as e:
        st.error(f"Error listing files in stage: {e}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None
    finally:
        pass

def remove_staged_file(rows):
    filesList=st.session_state.staged_file_list
    try:
        if len(rows)>0:        
            for i in rows:
                full_stage_path = f"@{filesList.iloc[i]['name']}" 
                if SESSION:
                    SESSION.sql(f"REMOVE {full_stage_path}").collect()
    except snowflake.connector.errors.ProgrammingError as e:
        st.error(f"Error listing files in stage: {e}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None
    finally:
        pass

def list_snowflake_stages():
    """
    Retrieves a list of stages from Snowflake.

    Args:
        session: The Snowpark session object.
        database_name: Optional. The name of the database to list stages from.
                    If None, uses the current database in the session.
        schema_name: Optional. The name of the schema to list stages from.
                    If None, and database_name is provided, lists stages
                    from all schemas in the database. If both are None,
                    lists stages from all schemas in the current database.

    Returns:
        A list of stage names (str).  Returns an empty list if no stages are found,
        or if there's an error.  Handles errors internally.
    """
    try:
        if SESSION:
            # Stages in current database.
            results = SESSION.sql("SHOW STAGES").collect()
        return pd.DataFrame(results)["name"]
    except Exception as e:
        print(f"Error retrieving stages: {e}")
        return []
    
def refreshFilesList():
    if st.session_state["uploaded_file"] is not None:
        file_path_in_stage = upload_file_to_stage(
            st.session_state["uploaded_file"], st.session_state["stage_name"]
        )
        if file_path_in_stage:
            st.success(f"File uploaded successfully to: {file_path_in_stage}")
            st.session_state.message=f"File uploaded successfully to: {file_path_in_stage}"
    else:
        st.error("Please select a file to upload.")
        
def process_editor_changes():
    delete_records=st.session_state.file_updates['deleted_rows']
    remove_staged_file(delete_records)
    on_change_stage_list()
    
    #st.session_state.staged_file_list = st.session_state.staged_file_list.drop(index).reset_index(drop=True)

####################################################################################
def main():
    """
    Snowflake Stage Manager
    """
    st.title("Snowflake Stage File Manager")
    st.caption(f"{SESSION.connection.account}/{SESSION.connection.database}/{SESSION.connection.schema}")
    df_stages_list = list_snowflake_stages()
    with st.expander("Staged Files Manager", expanded=True):
        st.selectbox(f"Stages", df_stages_list, index=st.session_state['stage_selection'], key="stage_name", on_change=on_change_stage_list)
        on_change_stage_list()
        st.caption(f"Files in stage @{st.session_state.stage_name}:")
        df_files=st.session_state.staged_file_list
        if df_files is not None:
            if len(df_files)>0:
                df_files.drop('md5', axis=1, inplace=True)
                st.data_editor(df_files, num_rows="dynamic", key="file_updates", on_change=process_editor_changes)  # Display file information in a DataFrame
            else:
                st.info(
                    f"No files found in stage @{st.session_state.stage_name} or error occurred."
                )
    with st.expander("File Upload Manager"):
        # File upload widget
        st.file_uploader("Choose a file", accept_multiple_files = True, on_change=refreshFilesList, key="uploaded_file")
        # Upload button
        if st.button("Upload File"):
            if st.session_state["uploaded_file"] is not None:
                file_path_in_stage = upload_file_to_stage(
                    st.session_state["uploaded_file"], st.session_state.stage_name)
                if file_path_in_stage:
                    st.success(f"File uploaded successfully to: {file_path_in_stage}")
            else:
                st.error("Please select a file to upload.")      
        on_change_stage_list() 
    
    # Display notifications. Crucially, this is at the *end* of the script.
    notification_center.display_notifications()

if __name__ == "__main__":
    main()

