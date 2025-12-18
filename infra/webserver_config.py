import os

# Airflow Webserver Configuration
# This file configures the Flask webserver for Airflow

# Flask configuration
SECRET_KEY = 'airflow-webserver-secret-key-2025'
MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size

# Session configuration (using default securecookie backend)
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'

# Airflow logging configuration
BASE_LOG_FOLDER = os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'logs')

