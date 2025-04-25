
import os
import logging
import json
import toml  # Import the toml library
from snowflake.snowpark import Session
import utils

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



def get_login_token():
    """
    Read the login token supplied automatically by Snowflake. These tokens
    are short lived and should always be read right before creating any new connection.
    """
    with open("/snowflake/session/token", "r") as f:
        return f.read()

def connect_snowflake(toml_file_path: str) -> Session:
    """
    Establishes a Snowpark session using connection parameters from a TOML file.

    Args:
        toml_file_path (str): The path to the TOML file containing Snowflake connection
            parameters.  The TOML file should have a structure like this:

            [snowflake]
            account = "your_account_identifier"
            user = "your_username"
            password = "your_password"
            role = "your_role"
            warehouse = "your_warehouse"  # Optional
            database = "your_database"    # Optional
            schema = "your_schema"        # Optional

    Returns:
        Session: A Snowpark session object.

    Raises:
        FileNotFoundError: If the TOML file does not exist.
        toml.TomlDecodeError: If there is an error decoding the TOML file.
        Exception: If the connection to Snowflake fails.
    """
    try:
        with open(toml_file_path, 'r') as f:
            config = toml.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"TOML file not found at: {toml_file_path}")
    except toml.TomlDecodeError as e:
        raise toml.TomlDecodeError(f"Error decoding TOML file: {e}")
    snowflake_config = config.get('snowflake', {})  #handles if there is no snowflake section
    
    if os.path.exists("/snowflake/session/token"):
        logger.info("Creating a session as service user.")
        connection_params = {
            'host': os.getenv('SNOWFLAKE_HOST'),
            'port': os.getenv('SNOWFLAKE_PORT'),
            'protocol': "https",
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'authenticator': "oauth",
            'token': open('/snowflake/session/token', 'r').read(),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True 
        }
    
    else:
        logger.info("Creating a session as user defined in TOML file.")
        connection_params = {
            "account": snowflake_config.get("account"),
            "user": snowflake_config.get("user"),
            "password": snowflake_config.get("password"),
            "private_key_file":snowflake_config.get("private_key_path"),
            "role": snowflake_config.get("role"),
            "warehouse": snowflake_config.get("warehouse"),  # Optional
            "database": snowflake_config.get("database"),    # Optional
            "schema": snowflake_config.get("schema")        # Optional
        }

    # Remove keys with None values, as Snowpark handles defaults.
    connection_params = {k: v for k, v in connection_params.items() if v is not None}

    try:
        session = Session.builder.configs(connection_params).create()
        session.use_role( snowflake_config.get("role"))
        if snowflake_config.get("warehouse") is not None:
            session.use_warehouse(snowflake_config.get("warehouse"))
        logger.info(f"Successfully connected to Snowflake as user: {session.get_current_user()}")
        return session
    except Exception as e:
        raise Exception(f"Failed to connect to Snowflake: {e}")


def main():
    """
    Main function to orchestrate the file processing workflow.
    """
    env="snowflake"
    # Load configuration from a TOML file
    config_file = "./secrets/configuration.toml"  # You can change the filename if needed
    try:
        with open(config_file, "r") as f:
            config = toml.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_file}")
        return

    session = connect_snowflake(config_file)
    if session is None:
        logger.error("Failed to establish a database connection. Exiting.")
        return  # Exit if connection fails

    raw_stage = config[env]["raw_stage"]
    processing_stage = config[env]["processing_stage"]
    complete_stage = config[env]["complete_stage"]
    error_stage = config[env]["error_stage"]
    
    #For Testing sample data 
    #table_data={"customers": [{"customer_id": "1", "name": "Dave Lister"}, {"customer_id": "2", "name": "Arnold Rimmer"}, {"customer_id": "3", "name": "The Cat"}, {"customer_id": "4", "name": "Holly"}, {"customer_id": "5", "name": "Kryten"}, {"customer_id": "6", "name": "Kristine Kochanski"}], "orders": [{"order_id": "1", "customer_id": "2", "product_id": "1", "amount": "7"}, {"order_id": "2", "customer_id": "2", "product_id": "3", "amount": "2"}, {"order_id": "3", "customer_id": "1", "product_id": "2", "amount": "3"}, {"order_id": "4", "customer_id": "6", "product_id": "3", "amount": "5"}], "products": [{"product_id": "1", "title": "Chair"}, {"product_id": "2", "title": "Table"}, {"product_id": "3", "title": "Computer"}]}    
    #utils.write_json_string_to_table(session, json.dumps(table_data), "test")
    
    #1.  Extract stage names from the configuration
    files_list=utils.list_files_in_stage(session, raw_stage)
    #2.  Move the file and process them sequentially
    if files_list:
        if len(files_list)==0:
            logger.info("No files to process")
            return
        #Iterate through each of the files
        for f in files_list:
            filename=f["name"].split("/")[-1]
            utils.move_staged_file(session, filename, raw_stage,processing_stage)
            results=utils.process_file(session,filename, processing_stage)
            if results:
                utils.move_staged_file(session, filename, processing_stage,complete_stage)
            else:
                utils.move_staged_file(session, filename, processing_stage,error_stage)


if __name__ == "__main__":
    main()
