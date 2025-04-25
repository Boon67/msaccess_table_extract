# Snowflake Access Database Ingestion Pipeline

This project provides a solution for automatically ingesting data from Microsoft Access Database (.accdb or .mdb) files stored in a Snowflake stage. When a new Access database file is detected on the stage, a containerized job is triggered to:

1.  **Read** all tables within each Access database file.
2.  **Export** the data from each table.
3.  **Load** the data into a designated Snowflake table based on the filename and timestamp.
4.  **Include Metadata:** Each record in the Snowflake table will be augmented with the original Access database filename and a timestamp indicating when the data was processed.

This solution leverages containerization for portability and scalability, making it easy to deploy and manage within your Snowflake environment.

## Architecture

The high-level architecture of this solution is as follows:
+-----------------+      +-----------------+      +-----------------------+
| Snowflake Stage |----->| Containerized   |----->| Snowflake Target Table|
| (Access Files)  |      | Ingestion Job   |      |                       |
+-----------------+      +-----------------+      +-----------------------+
Event Trigger (e.g., Snowpipe, custom notification)

When a new Access database file is uploaded to the designated Snowflake stage, an event trigger (such as Snowpipe or a custom notification mechanism) initiates the containerized ingestion job. The container then reads the file, extracts the table data, and loads it into the target Snowflake table, adding the filename and timestamp for context.

## Components

The key components of this project are:

* **Containerized Ingestion Job:** A Docker container that encapsulates the logic for:
    * Reading the Access database file from the Snowflake stage.
    * Reading data from each table.
    * Loading the table into a dataframe
    * Saving the the table data into the target Snowflake table, including columns for the filename and timestamp.
* **Snowflake Stage:** The designated location in Snowflake where the Access database files will be stored.
* **Snowflake Target Table:** The Snowflake table will be generated automatically keeping the filename and a time stamp of the extraction. The table content will be stored as a variant
* **Event Trigger Mechanism:** A mechanism to automatically trigger the containerized job when a new file lands on the Snowflake stage. This could be:
    * **Snowpipe:** Snowflake's continuous data ingestion service.
    * **Custom Notification Service:** A service (e.g., AWS Lambda, Azure Function) that listens for stage events and triggers the container.
    * **Snowflake Task** A periodic run (sample code provided)

## Setup and Deployment
To set up and deploy this solution, follow these steps:

1.  **Prerequisites:**
    * A Snowflake account.
    * Docker installed on your deployment environment.
    * Access to a container registry (e.g., Docker Hub, AWS ECR, Azure Container Registry).
    * Appropriate Snowflake roles and permissions to create stages, tables, and execute data loading.
    * The necessary libraries within the container to read Access database files (e.g., `pyodbc` with the appropriate drivers).

2.  **Container Image Creation:**
    * Write the Python (or other suitable language) script to perform the data extraction and loading logic. This script should:
        * Accept the path to the Access database file on the Snowflake stage as an argument.
        * Use a library to connect to and read data from the Access database.
        * Establish a connection to Snowflake using the Snowflake Python Connector.
        * Dynamically create or insert data into the target Snowflake table, including the filename and a timestamp (e.g., using Snowflake's `CURRENT_TIMESTAMP()` function during the load).
    * Create a `Dockerfile` to build the container image. This file should:
        * Specify a base image.
        * Install the necessary dependencies (Python libraries, ODBC drivers, etc.).
        * Copy your script into the container.
        * Define the entry point for the container (the execution of your script).
    * Build the Docker image: `docker build -t your-registry/your-image-name:tag .`
    * Push the Docker image to your chosen container registry: `docker push your-registry/your-image-name:tag`
    * Check the Build.sh.sample file for a pattern to automate

3.  **Snowflake Setup:**
    * The setup folder has a setup script to setup the environment. The requirements are to have a target user account and ideally use key/pair authentication for access from the container for testing locally, and using the token on the container to execute as the caller role.

4.  **Event Trigger Configuration:**
    This needs to be configured based on your use case.
    * **Using Snowpipe:**
        * Create a Snowpipe that listens for new files on your stage.
        * Configure the Snowpipe to trigger a Snowpark Container Services (or similar) task that runs your container. You'll need to pass the filename of the newly arrived file to your container as an argument.
    * **Using a Custom Notification Service:**
        * Set up a notification mechanism (e.g., using Snowflake stage notifications to a cloud messaging service like AWS SQS or Azure Queue Storage).
        * Create a service (e.g., AWS Lambda, Azure Function) that listens to these notifications.
        * This service should be configured to pull and run your container image, passing the filename of the newly uploaded Access database file as an argument to the container.
    * **Using a Schedule Task to Run Periodically** 
        * Setup a task to execute the Job Service periodically.
        * Sample setup code provided in the setup folder.
    

5.  **Container Execution in Snowflake (Example using Snowpark Container Services):**
    * Ensure Snowpark Container Services is enabled for your Snowflake account.
    * Create a Snowpark Container Services service and task that pulls your container image from the registry and runs it when triggered (e.g., by Snowpipe). You'll need to configure the task to receive the filename from the Snowpipe event.

## Usage

1.  Upload your Microsoft Access Database (.accdb or .mdb) files to the configured Snowflake stage (e.g., `raw`).
2.  The configured event trigger (Snowpipe or custom service) will automatically detect the new file. *Configured Per your use case*
3.  The containerized ingestion job will be launched.
4.  The job will read all tables from the Snowflake Stage, export their contents, and load the data and create a table with the filename and _timestamp in Snowflake on the target schema

## Considerations and Future Enhancements

* **Error Handling:** Implement robust error handling within the containerized job to manage potential issues like invalid file formats, database connection errors, or schema mismatches.
* **Schema Evolution:** Consider how to handle changes in the schema of the Access database tables over time. You might need to implement logic to dynamically adjust the target Snowflake table or store the data in a more flexible format (e.g., JSON).
* **Security:** Ensure proper security measures are in place for accessing the Snowflake stage, connecting to Snowflake from the container, and handling any sensitive data within the Access databases.
* **Scalability:** For very large Access databases or frequent file uploads, consider optimizing the containerized job and the Snowflake loading process for better performance.
* **Logging and Monitoring:** Implement comprehensive logging within the container and consider setting up monitoring tools to track the success and failure of the ingestion jobs.
* **Table Name Tracking:** The current design loads all data into a single table. You might want to enhance it to either create separate Snowflake tables for each Access database table or include the original table name as a column in the target table for better organization.
* **Data Type Mapping:** Carefully consider the mapping of data types between Microsoft Access and Snowflake to ensure data integrity.
