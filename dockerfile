# Use official Airflow image as base
FROM apache/airflow:2.7.3-python3.10

# Switch to root to install system packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements and install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt

# Copy application files with proper ownership
COPY --chown=airflow:root app/ /opt/airflow/app/
COPY --chown=airflow:root data/ /opt/airflow/data/
COPY --chown=airflow:root sql/ /opt/airflow/sql/
COPY --chown=airflow:root dags/ /opt/airflow/dags/

# Create necessary directories
RUN mkdir -p /opt/airflow/logs /opt/airflow/data/staging

# Set working directory
WORKDIR /opt/airflow