import toml
from snowflake.snowpark import Session
from snowflake.connector import connect
from snowflake.connector.connection import SnowflakeConnection
import jwt, os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Type
from pydantic import BaseModel, Field
from typing_extensions import Annotated
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend 
from cryptography.hazmat.primitives import asymmetric

import base64
#from lib.snowflake.snowflake_stage_manager import SnowflakeStageManager 

class SnowflakeSessionManager:
    """
    Manages Snowflake sessions using Snowpark, supporting TOML configuration,
    username/password or key pair authentication, and JWT token generation
    for the Snowflake REST API.  Also generates classes for Cortex API endpoints.
    """

    def __init__(self, config_file: str="connections.toml", profile: str = "snowflake"):
        """
        Initializes the SnowflakeSessionManager with configuration from a TOML file.

        Args:
            config_file (str): Path to the TOML configuration file.
            profile (str, optional): The profile name in the TOML file. Defaults to "default".
        """
        self.config_file = config_file
        self.profile = profile
        self.config = self._load_config()
        self.session: Optional[Session] = None
        self.connection: Optional[SnowflakeConnection] = None
        self.account = self.config.get("account")
        self.user = self.config.get("user")
        self.database = self.config.get("database")
        self.schema = self.config.get("schema")
        self.warehouse = self.config.get("warehouse")
        self.password = self.config.get("password")
        self.private_key_path = self.config.get("private_key_path")
        self.private_key_passphrase = self.config.get("private_key_passphrase")
        self.connect()
        
       # self.stageManager= SnowflakeStageManager(self.session)

    def _load_config(self) -> Dict[str, Any]:
        """
        Loads the Snowflake configuration from the TOML file.

        Returns:
            Dict[str, Any]: The configuration dictionary.

        Raises:
            FileNotFoundError: If the configuration file does not exist.
            KeyError: If the specified profile does not exist.
        """
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, "r") as f:
                    config = toml.load(f)
            else:
                print(f"Error: Configuration file not found at {self.config_file}")
                return None
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Configuration file not found: {self.config_file}") from e
        except toml.TomlDecodeError as e:
            raise ValueError(f"Error decoding TOML: {self.config_file}") from e

        if self.profile not in config:
            raise KeyError(f"Profile '{self.profile}' not found in configuration file.")
        return config[self.profile]

    def connect(self) -> None:
        """
        Establishes a Snowflake session using either username/password or key pair authentication.
        """
        if self.session:
            return  #  No need to connect again.

        connection_params: Dict[str, Any] = {
            "account": self.config.get("account"),
            "user": self.config.get("user"),
            "database": self.config.get("database"),
            "schema": self.config.get("schema"),
            "warehouse": self.config.get("warehouse"),
            "ocsp_policy": "FAIL_OPEN" # Add this to avoid OCSP issues.
        }

        if self.password is not None:
            connection_params["password"] = self.config["password"]
            self.session = Session.builder.configs(connection_params).create()
            self.connection = self.session.connection
        elif self.private_key_path:
            connection_params["private_key"] = self.get_kp_token()
            self.session = Session.builder.configs(connection_params).create()
            self.connection = self.session.connection
        else:
            raise ValueError(
                "Authentication method not found in configuration. "
                "Provide either 'password' or 'private_key' and 'user'."
            )
        if self.database:
            self.session.use_database(self.database)
        if self.schema:
            self.session.use_schema(self.schema)
        if self.warehouse:
            self.session.use_warehouse(self.warehouse)
            
            
    ##########################
    def get_kp_token(self):
        with open(self.private_key_path, "rb") as key:
            if self.private_key_passphrase is None:
                p_key= serialization.load_pem_private_key(
                key.read(),
                password=None,
                backend=default_backend()
                )

            else:
                p_key= serialization.load_pem_private_key(
                key.read(),
                password=self.private_key_passphrase.encode(),
                backend=default_backend()
                )
                
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
                )
            return pkb

        
            now = datetime.utcnow()
            payload = {
                "iss": self.user,
                "sub": self.user,
                "iat": now,
                "exp": now + timedelta(minutes=60),  # Token expiration time
                "aud": "https://{account}.snowflakecomputing.com/".format(account=self.account),
            }

            token = jwt.encode(payload, p_key, algorithm="RS256")
            return token


    
    def get_snowsql_connection(self) -> SnowflakeConnection:
        """
        Returns a raw Snowflake connection using the snowflake-connector-python.
        This is useful for things the Snowpark Session doesn't directly support.
        """
        if self.connection is None:
            connection_params: Dict[str, Any] = {
                "account": self.config.get("account"),
                "user": self.config.get("user"),
                "database": self.config.get("database"),
                "schema": self.config.get("schema"),
                "warehouse": self.config.get("warehouse"),
                "ocsp_policy": "FAIL_OPEN" # Add this to avoid OCSP issues.
            }

            if "password" in self.config:
                connection_params["password"] = self.config["password"]
                self.connection = connect(**connection_params)
            elif "private_key" in self.config and "user" in self.config:
                connection_params["private_key"] = self.config["private_key"]
                self.connection = connect(**connection_params)
            else:
                raise ValueError(
                    "Authentication method not found in configuration. "
                    "Provide either 'password' or 'private_key' and 'user'."
                )
        return self.connection

    def close(self) -> None:
        """
        Closes the Snowflake session and connection.
        """
        if self.session:
            self.session.close()
            self.session = None
        if self.connection:
            self.connection.close()
            self.connection = None

    def create_jwt_token(self) -> str:
        """
        Generates a JWT token for authenticating with the Snowflake REST API.

        Returns:
            str: The JWT token.

        Raises:
            ValueError: If required configuration is missing.
        """
        
        account = self.account
        user = self.user
        private_key = self.private_key_path

        # Decode the private key
        try:
            with open(self.private_key_path, "rb") as key:
                if self.private_key_passphrase is None:
                    p_key= serialization.load_pem_private_key(
                    key.read(),
                    password=None,
                    backend=default_backend()
                    )

                else:
                    p_key= serialization.load_pem_private_key(
                    key.read(),
                    password=self.private_key_passphrase.encode(),
                    backend=default_backend()
                    )
                        
            
            
            key_bytes = p_key.encode('utf-8')
            private_key_obj = serialization.load_pem_private_key(
                key_bytes,
                password=None,
                backend=default_backend()
            )
            private_key = private_key_obj
        except Exception as e:
            raise ValueError(f"Error decoding private key: {e}")

        now = datetime.utcnow()
        payload = {
            "iss": user,
            "sub": user,
            "iat": now,
            "exp": now + timedelta(minutes=60),  # Token expiration time
            "aud": "https://{account}.snowflakecomputing.com/".format(account=account),
        }

        try:
            token = jwt.encode(payload, private_key, algorithm="RS256")
            return token
        except Exception as e:
            raise RuntimeError(f"Error generating JWT token: {e}")

    @staticmethod
    def _create_pydantic_model(name: str, fields: Dict[str, Type]) -> Type[BaseModel]:
        """
        Dynamically creates a Pydantic model with the given name and fields.

        Args:
            name (str): The name of the Pydantic model.
            fields (Dict[str, Type]): A dictionary of field names and their types.

        Returns:
            Type[BaseModel]: The created Pydantic model class.
        """
        # Use type annotation
        field_annotations = {}
        for field_name, field_type in fields.items():
            field_annotations[field_name] = (field_type, Field())

        annotated_fields = {k: Annotated[v[0], v[1]] for k, v in field_annotations.items()}

        model_class = type(name, (BaseModel,), {"__annotations__": annotated_fields})
        return model_class

    def generate_cortex_classes(self, endpoints: Dict[str, Dict[str, Any]]) -> Dict[str, Type[BaseModel]]:
        """
        Generates Pydantic classes for Cortex API request and response schemas
        based on a dictionary of endpoints.

        Args:
            endpoints (Dict[str, Dict[str, Any]]): A dictionary of Cortex API endpoints,
                where each endpoint has 'request' and 'response' schemas
                defined as dictionaries of field names and their corresponding types.
                Example:
                {
                    "Summarize": {
                        "request": {"text": str},
                        "response": {"summary": str}
                    },
                    "AnalyzeSentiment": {
                        "request": {"text": str},
                        "response": {"sentiment": str, "score": float}
                    }
                }

        Returns:
            Dict[str, Type[BaseModel]]: A dictionary of generated Pydantic model classes,
                where keys are endpoint names (e.g., "SummarizeRequest", "SummarizeResponse").
        """
        models = {}
        for endpoint_name, schemas in endpoints.items():
            if "request" in schemas:
                request_model_name = f"{endpoint_name}Request"
                request_fields = schemas["request"]
                request_model_class = self._create_pydantic_model(request_model_name, request_fields)
                models[request_model_name] = request_model_class
            if "response" in schemas:
                response_model_name = f"{endpoint_name}Response"
                response_fields = schemas["response"]
                response_model_class = self._create_pydantic_model(response_model_name, response_fields)
                models[response_model_name] = response_model_class
        return models

    def __enter__(self):
        """
        Supports using the class as a context manager.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Closes the session when exiting the context.
        """
        self.close()

def main():
    """
    Snowflake SessionManager Tool.
    """
    # Example usage with a TOML configuration file
    config_file = "config.toml"  # Replace with your actual config file
    try:
        with open(config_file, "w") as f:
            f.write(
                """
                [default]
                account = "your_account"  # Replace with your Snowflake account identifier
                user = "your_user"      # Replace with your Snowflake username
                password = "your_password"  # Replace with your Snowflake password OR
                # private_key = "-----BEGIN RSA PRIVATE KEY-----\\n...\\n-----END RSA PRIVATE KEY-----\\n" #  OR use a private key
                database = "your_database"  # Replace with your Snowflake database name
                schema = "your_schema"      # Replace with your Snowflake schema name
                warehouse = "your_warehouse"    # Replace with your Snowflake warehouse name

                [alternative]
                account = "your_account"
                user = "your_user"
                private_key = "-----BEGIN RSA PRIVATE KEY-----\\n...\\n-----END RSA PRIVATE KEY-----\\n"
                database = "your_database"
                schema = "your_schema"
                warehouse = "your_warehouse"
                """
            )
    except Exception as e:
        print(f"Error creating config.toml: {e}")

    try:
        with SnowflakeSessionManager(config_file, profile="default") as session_manager:
            session_manager.connect()
            print(f"Snowpark Session connected.  Session: {session_manager.session}")
            print(f"Snowflake Connection (connector): {session_manager.connection}")

            # Example: Create and use a DataFrame (Snowpark)
            df = session_manager.session.sql("SELECT current_timestamp()")
            df.show()

            # Example: Get a raw connection (snowflake-connector-python)
            conn = session_manager.get_snowsql_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT current_version()")
            print("Current Version (snowflake-connector-python):", cursor.fetchone())
            cursor.close()
            conn.close()  #  Important:  Close this connection too.

            # Example: Generate JWT token
            try:
                jwt_token = session_manager.create_jwt_token()
                print(f"JWT Token: {jwt_token}")
            except ValueError as e:
                print(f"Error generating JWT token: {e}")

            # Example Cortex API endpoint definitions
            cortex_endpoints = {
                "Summarize": {
                    "request": {"text": str},
                    "response": {"summary": str}
                },
                "AnalyzeSentiment": {
                    "request": {"text": str},
                    "response": {"sentiment": str, "score": float}
                }
            }

            # Generate Cortex API classes
            cortex_models = session_manager.generate_cortex_classes(cortex_endpoints)
            print("Generated Cortex API models:")
            for model_name, model_class in cortex_models.items():
                print(f"- {model_name}: {model_class}")

            # Example of using the generated models (Illustrative)
            if "SummarizeRequest" in cortex_models and "SummarizeResponse" in cortex_models:
                SummarizeRequest = cortex_models["SummarizeRequest"]
                SummarizeResponse = cortex_models["SummarizeResponse"]

                # In a real application, you would get this data from an API call
                request_data = {"text": "This is a long text to summarize."}
                try:
                    request_obj = SummarizeRequest(**request_data)
                    print(f"Summarize Request object: {request_obj}")
                    # Simulate a response from the API
                    response_data = {"summary": "This is a short summary."}
                    response_obj = SummarizeResponse(**response_data)
                    print(f"Summarize Response object: {response_obj}")
                except Exception as e:
                    print(f"Error creating Pydantic models: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        #  cleanup
        try:
            import os
            os.remove(config_file)
        except:
            pass

if __name__ == "__main__":
    main()
