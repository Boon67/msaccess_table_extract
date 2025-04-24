from snowflake.snowpark import Session
import os
from typing import Optional, Dict, Any, Type, List


class SnowflakeStageManager:
    """
    A class for managing files in a Snowflake stage using Snowpark.  It handles
    uploading, removing, and listing files.

    Attributes:
        session (Session): The Snowpark session object.
        stage_name (str): The name of the Snowflake stage.
    """

    def __init__(self, session: Session, stage_name: str=None):
        """
        Initializes the SnowflakeStageManager with a Snowpark session and stage name.

        Args:
            session (Session): The Snowpark session object.
            stage_name (str): The name of the Snowflake stage.
        """
        if not isinstance(session, Session):
            raise TypeError("session must be a valid Snowpark Session object.")
        #if not isinstance(stage_name, str):
            #raise TypeError("stage_name must be a string.")
        #if not stage_name:
            #raise ValueError("stage_name cannot be empty.")

        self.session = session
        self.stage_name = stage_name
        # Ensure the stage name is properly formatted (with or without @).
        if stage_name is not None:
            self.stage_name_full = stage_name if stage_name.startswith('@') else f'@{stage_name}'

    def upload_file(self, local_file_path: str, stage_file_path: Optional[str] = None, create_directory: bool = False) -> str:
        """
        Uploads a file from a local path to the Snowflake stage.

        Args:
            local_file_path (str): The path to the local file to upload.
            stage_file_path (str, optional): The path within the stage to upload the file to.
                If None, the file is uploaded to the root of the stage with the same name.
                If a directory is specified and create_directory is False, the upload will fail if the directory does not exist
            create_directory (bool, optional): If True, creates the directory in the stage if it does not exist.
                Defaults to False.

        Returns:
            str: The full path of the file in the stage.

        Raises:
            FileNotFoundError: If the local file does not exist.
            ValueError: If the stage file path is invalid.
            Exception: If the upload fails.
        """
        if not isinstance(local_file_path, str):
            raise TypeError("local_file_path must be a string.")
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Local file not found: {local_file_path}")
        if stage_file_path is not None and not isinstance(stage_file_path, str):
            raise TypeError("stage_file_path must be a string or None.")
        if not isinstance(create_directory, bool):
            raise TypeError("create_directory must be a boolean")

        file_name = os.path.basename(local_file_path)
        stage_path = self.stage_name_full
        if stage_file_path:
            stage_path = f"{self.stage_name_full}/{stage_file_path}"

        if not stage_path.endswith('/'):
            stage_path += '/'

        full_stage_file_path = f"{stage_path}{file_name}"

        # Check if the directory exists, and create it if necessary
        if stage_file_path and create_directory:
            try:
                self.session.sql(f"CREATE DIRECTORY IF NOT EXISTS '{stage_path}'").collect()
            except Exception as e:
                raise Exception(f"Failed to create directory in stage: {e}")

        try:
            self.session.file.upload(local_file_path, stage_path)
            return full_stage_file_path
        except Exception as e:
            raise Exception(f"Failed to upload file: {e}")

    def remove_file(self, stage_file_path: str) -> bool:
        """
        Removes a file from the Snowflake stage.

        Args:
            stage_file_path (str): The path to the file in the stage to remove.

        Returns:
            bool: True if the file was removed successfully, False otherwise.

        Raises:
            TypeError: If stage_file_path is not a string.
            ValueError: If stage_file_path is empty.
            Exception: If the removal fails.
        """
        if not isinstance(stage_file_path, str):
            raise TypeError("stage_file_path must be a string.")
        if not stage_file_path:
            raise ValueError("stage_file_path cannot be empty.")

        # Ensure the stage file path starts with the stage name
        if not stage_file_path.lower().startswith(self.stage_name_full.lower()):
            if stage_file_path.startswith('/'):
                stage_file_path = f"{self.stage_name_full}{stage_file_path}"
            else:
                stage_file_path = f"{self.stage_name_full}/{stage_file_path}"

        try:
            result = self.session.sql(f"REMOVE '{stage_file_path}'").collect()
            # The result is a list of Row objects.  Check the first row and column.
            if result and result[0][0] == stage_file_path:
                return True
            else:
                return False
        except Exception as e:
            raise Exception(f"Failed to remove file: {e}")

    def list_files(self, stage_path: Optional[str] = None) -> List[str]:
        """
        Lists files in the Snowflake stage.

        Args:
            stage_path (str, optional): The path within the stage to list files from.
                If None, lists files from the root of the stage. Defaults to None.

        Returns:
            List[str]: A list of file paths in the stage.  Returns an empty list if no files are found.

         Raises:
            TypeError: If stage_path is not a string or None.
            Exception: If the listing fails.
        """
        if stage_path is not None and not isinstance(stage_path, str):
            raise TypeError("stage_path must be a string or None.")

        # Construct the full stage path.
        full_stage_path = self.stage_name_full
        if stage_path:
            full_stage_path = f"{self.stage_name_full}/{stage_path}"

        try:
            files_df = self.session.sql(f"LIST '{full_stage_path}'").to_pandas()
            # Extract the 'name' column, which contains the full file path
            if not files_df.empty:
                return files_df['name'].tolist()
            else:
                return []
        except Exception as e:
            raise Exception(f"Failed to list files: {e}")
