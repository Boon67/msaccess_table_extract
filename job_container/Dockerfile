# Use a Python base image
FROM python:3.9-slim-buster
ARG PORT=8000
# Set the working directory inside the container
WORKDIR /app
ENV PORT=${PORT}
# Copy the Python script into the container
COPY app.py /app/app.py
COPY utils.py /app/utils.py
COPY access_util.py /app/access_util.py
COPY rsa_key.p8 /app/rsa_key.p8
COPY configuration.toml /app/configuration.toml


# Install mdbtools and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    mdbtools \
    && rm -rf /var/lib/apt/lists/*


RUN pip install --no-cache-dir pandas snowflake snowflake-snowpark-python toml snowflake-connector-python[pandas]


# Run the application using Uvicorn
CMD ["python", "app.py"]
