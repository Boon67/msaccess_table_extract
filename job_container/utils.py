
from typing import List, Optional
from snowflake.snowpark import Session
from access_util import MSAccessUtils
from pathlib import Path
import tempfile, os, json
from snowflake.snowpark.types import StructType, StructField, VariantType
import pandas as pd
import datetime


def list_files_in_stage(
    session,
    stage_name: str,
    path: str = ''
) -> Optional[List[str]]:
    """
    Lists files in a Snowflake stage, with optional filtering by path and pattern.

    Args:
        connection_params (dict): A dictionary containing Snowflake connection parameters.
            See https://docs.snowflake.com/en/developer-guide/python-connector-api.html#connect
            for available parameters (e.g., 'user', 'password', 'account', 'warehouse', 'database', 'schema').
        stage_name (str): The name of the Snowflake stage.
        path (str, optional): The path within the stage to list files from. Defaults to ''.
        pattern (str, optional):  A pattern to filter files by (e.g., 'data*.csv'). Defaults to '%'.

    Returns:
        Optional[List[str]]: A list of file names in the stage that match the pattern, or None on error.
    """
    try:
        # List files in the stage
        
        sql=f"LIST '@{stage_name}/{path}';"
        files_df = session.sql(sql)
        if files_df.count() == 0:
            return []  # Return an empty list if no files found
        files = files_df.collect() # Collect only if there are results
        # Extract file names and last modified times.  Convert the last_modified time.
        file_list = [{"name": file.name, "last_modified": file.last_modified} for file in files]
        return file_list
    except Exception as e:
        print(f"Error: {e}")
        return None

    finally:
        pass

def move_staged_file(
    session: Session,
    file_name: str,
    source_stage: str,
    target_stage: str,
    create_target_stage: bool = False
) -> bool:
    """
    Moves a file from one stage to another in Snowflake using a Snowpark Session.

    This simplified version takes the source and target file paths as single strings,
    assuming they include both the stage name and the file path.  For example:
    "source_stage/path/to/file.csv" and "target_stage/path/to/file.csv"

    Args:
        session: The Snowpark Session object.
        source_stage_file: The source stage and file path (e.g., "my_source_stage/data/file.csv").
        target_stage_file: The target stage and file path (e.g., "my_target_stage/destination/file.csv").
        create_target_stage: Boolean indicating whether to create the target stage if it doesn't exist.
            Defaults to False.

    Returns:
        True if the file was moved successfully, False otherwise.
    """
    try:
        # Extract stage names
        source_stage_name = source_stage.split('/')[0].upper()
        target_stage_name = target_stage.split('/')[0].upper()


        # Check if the source stage exists
        if not session.sql(f"SHOW STAGES LIKE '{source_stage_name}'").collect():
            print(f"Error: Source stage '{source_stage_name}' does not exist.")
            return False

        # Check if the target stage exists, and create it if requested
        if not session.sql(f"SHOW STAGES LIKE '{target_stage_name}'").collect():
            if create_target_stage:
                try:
                    session.sql(f"CREATE STAGE {target_stage_name}").collect()
                    print(f"Target stage '{target_stage_name}' created.")
                except Exception as e:
                    print(f"Error creating target stage '{target_stage_name}': {e}")
                    return False
            else:
                print(f"Error: Target stage '{target_stage_name}' does not exist.")
                return False

        # Use the COPY INTO location command to move the file
        copy_statement = f"""
            COPY FILES INTO @{target_stage}
            FROM @{source_stage}
            FILES = ('{file_name}')
        """
        session.sql(copy_statement).collect()

        # Check the copy result
        copy_result = session.sql(f"SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))").collect()
        if not copy_result:
            print("Error: Copy operation failed. No result returned from COPY INTO.")
            return False
        # Remove the file from the source stage
        remove_statement = f"REMOVE @{source_stage}/{file_name}"
        remove_result = session.sql(remove_statement).collect()
        remove_result_string = str(remove_result[0]["result"])

        if remove_result_string.startswith("removed"):
            print(f"File '{file_name}' successfully moved to '{target_stage}'.")
            return True
        else:
            print(f"Warning: File '{file_name}' was copied, but failed to remove.  You may need to remove it manually. Remove result: {remove_result_string}")
            return True

    except Exception as e:
        print(f"Error moving file: {e}")
        return False
    
    
def write_json_string_to_table(session: Session, json_string: str, table_name: str) -> None:
    """
    Writes a JSON string to a Snowflake table.  The JSON string is treated as a single row
    with a single VARIANT column.

    Args:
        session: The Snowpark session to use.
        json_string: The JSON string to write.
        table_name: The name of the table to write to.
        create_table:  Boolean indicating whether to create the table if it doesn't exist.
                       If True, the table is created with a single VARIANT column.
                       If False, the table must already exist with a compatible schema.
    """
    try:
        # 1. Parse the JSON string
        try:
            df=pd.DataFrame({"data":json.loads(json_string)})
            now = datetime.datetime.now()
            timestamp_string=""
            timestamp_string = now.strftime("%Y%m%d_%H%M%S")
            table_name = f"{table_name}_{timestamp_string}"
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON string: {e}")    
        session.write_pandas(df, table_name, auto_create_table=True, overwrite=True)
    except Exception as e:
        print(f"Error writing JSON to table: {e}") 
    
    
def process_file(session,filename, stage_name):
    # Save file to a temporary location
    stage_file_url = f"{stage_name}/{filename}"
    temp_file_path = str(Path(tempfile.gettempdir()))
    #file_stream = session.file.get_stream(stage_file_url,)
    #file_bytes = file_stream.readall()  # Read all bytesbytes
    try:
        tmpf=session.file.get(stage_file_url, temp_file_path)
    except Exception as e:
        return f"Error saving uploaded file: {e}"
    fullpath=f"{temp_file_path}/{filename}"
    try:
        # Read the table data
        tablelist=MSAccessUtils.read_access_file(fullpath)
        print(f"Tables: {tablelist}\n")
        
        table_data={}
        for table in tablelist["tables"]:
            table_data[table]=MSAccessUtils.read_table_data(fullpath, table)
        #table_data={"customers": [{"customer_id": "1", "name": "Dave Lister"}, {"customer_id": "2", "name": "Arnold Rimmer"}, {"customer_id": "3", "name": "The Cat"}, {"customer_id": "4", "name": "Holly"}, {"customer_id": "5", "name": "Kryten"}, {"customer_id": "6", "name": "Kristine Kochanski"}], "orders": [{"order_id": "1", "customer_id": "2", "product_id": "1", "amount": "7"}, {"order_id": "2", "customer_id": "2", "product_id": "3", "amount": "2"}, {"order_id": "3", "customer_id": "1", "product_id": "2", "amount": "3"}, {"order_id": "4", "customer_id": "6", "product_id": "3", "amount": "5"}], "products": [{"product_id": "1", "title": "Chair"}, {"product_id": "2", "title": "Table"}, {"product_id": "3", "title": "Computer"}]}    
        
        print(f"{json.dumps(table_data)}\n")
        write_json_string_to_table(session, json.dumps(table_data), filename.replace('.','_'))
        
    except Exception as e:
        return f"Error extracting files: {e}"
    finally:
        # Delete the temporary file
        os.remove(fullpath)
        return table_data
