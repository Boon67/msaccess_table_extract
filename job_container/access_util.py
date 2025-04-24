import pandas as pd
import subprocess
from io import BytesIO
import csv
from typing import List, Dict

class MSAccessUtils:
    def __init__(self):
        pass
    
    def read_access_file(file_path: str, password: str = "") -> Dict[str, List[str]]:
        """
        Reads an MS Access database file and returns a list of table names.

        Args:
            file_path (str): Path to the MS Access file.
            password (str, optional): Password for the MS Access database.  mdbtools does not support passwords.

        Returns:
            dict: A dictionary with a single key "tables" and a list of table names as the value,
                or an error message string if an error occurs.
        """
        try:
            # mdbtools does not support passwords.  Warn if a password is provided.
            if password:
                print("Warning: mdbtools does not support passwords.  The password will be ignored.")

            # Get a list of table names
            process = subprocess.Popen(['mdb-tables', '-1', file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            if stderr:
                return {"error": f"Error listing tables: {stderr.decode()}"}  # Return a dictionary with an error key
            table_names = stdout.decode().strip().split('\n')
            return {"tables": table_names}  # Return the table names in a dictionary

        except Exception as e:
            return {"error": f"An unexpected error occurred: {e}"}  # Return a dictionary with an error key


    def read_table_data(file_path: str, table_name: str) -> List[Dict[str, str]]:
        """
        Reads data from a specified table in an MS Access database file.

        Args:
            file_path (str): Path to the MS Access file.
            table_name (str): Name of the table to read.

        Returns:
            List[Dict[str, str]]: A list of dictionaries, where each dictionary
                                represents a row in the table.  The keys are
                                column names and the values are the corresponding
                                row values (as strings).  Returns an empty list
                                if there's an error.
        """
        try:
            process = subprocess.Popen(['mdb-export', file_path, table_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()

            if stderr:
                print(f"Error exporting table data: {stderr.decode()}")
                return []

            csv_data = stdout.decode()
            reader = csv.DictReader(csv_data.splitlines())
            return list(reader)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return []
