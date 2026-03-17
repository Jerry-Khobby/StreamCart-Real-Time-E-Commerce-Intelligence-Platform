FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
  default-jdk \
  curl \
  git \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Switch to airflow user BEFORE installing Python packages
USER airflow

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set working directory and Python path
WORKDIR /opt/airflow
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH