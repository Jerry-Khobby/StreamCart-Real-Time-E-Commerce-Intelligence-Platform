#stage 1 - get the airflow image 
FROM apache/airflow:2.8.1-python3.11


USER root


# Install system dependencies
RUN apt-get update && apt-get install -y \
  default-jdk \
  curl \
  git \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set workdir BEFORE switching user
WORKDIR /opt/airflow
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

# Switch to airflow user for pip installs (required by this base image)
USER airflow

# Copy and install Python dependencies
COPY --chown=airflow:root requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt