"""
WeAdapt Data Warehouse ETL Pipeline (v3 - Refactored and Fixed)

This version corrects Python import errors that cause DAG parsing failures in modularized Airflow setups.

Key Fix:
- Simplified Python Path: Uses a single, explicit `sys.path.insert(0, '/opt/airflow/scripts')`
  which directly corresponds to the volume mount defined in `docker-compose.yml`. This removes
  fragile relative path calculations (`os.path.join`, `__file__`) that can fail during parsing.
"""
import sys
import os
from datetime import datetime, timedelta
import logging

# [FIX] Correctly and robustly add the scripts directory to Python's path.
# This must be at the top, before attempting to import any custom modules.
# The path '/opt/airflow/scripts' is determined by the `volumes` section in the
# docker-compose.yml for the Airflow scheduler and webserver.
sys.path.insert(0, os.path.abspath('/opt/airflow/scripts'))

from airflow.decorators import dag
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator

# --- Custom Module Imports ---
# These imports will now work correctly because of the sys.path modification above.
# If these modules have their own dependencies, they must be installed via requirements.txt.
try:
    from validation.environment_checker import validate_environment
    from ingestion.ingestion_runner import run_data_ingestion
    from quality.dq_runner import run_data_quality_checks
    from metrics.collector import collect_pipeline_metrics
    from cleanup.cleanup_handler import cleanup_on_failure
    from notifications.success_notifier import send_success_notification
    from notifications.failure_notifier import send_failure_notification
except ImportError as e:
    # If imports fail, log a critical error to the Airflow scheduler logs
    # to make debugging easier.
    logging.critical(f"DAG Import Error: {e}. Check if '/opt/airflow/scripts' exists and is populated.")
    # Re-raise the error to ensure the DAG is marked as broken in the UI
    raise

# --- DAG Configuration ---

# Use Airflow Variables for configuration with sensible defaults for local dev
POSTGRES_CONN_ID = Variable.get("weadapt_pg_conn", default_var="postgres_default")
DATA_DIR = Variable.get("weadapt_data_dir", default_var="/opt/airflow/sql")
STAGING_FILE_PATH = Variable.get("weadapt_staging_path", default_var="/opt/airflow/data/ready")

DEFAULT_ARGS = {
    'owner': 'weadapt_data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['data-team@weadapt.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='weadapt_dwh_etl_pipeline_v3_fixed',
    default_args=DEFAULT_ARGS,
    description='Production ETL pipeline for WeAdapt Data Warehouse (Fixed Imports)',
    schedule_interval='0 6 * * *',
    tags=['weadapt', 'dwh', 'etl', 'production'],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    template_searchpath=['/opt/airflow/sql', '/opt/airflow/sql/ddl', '/opt/airflow/scripts'],
)
def weadapt_pipeline():
    """
    ### WeAdapt Data Warehouse ETL Pipeline
    This DAG orchestrates the entire ETL process for the WeAdapt DWH.
    It validates the environment, ingests data, runs transformations,
    checks data quality, and handles success/failure notifications.
    """
    
    # 1. Control and Validation Tasks
    start_pipeline = EmptyOperator(task_id='start')

    validate_env = ShortCircuitOperator(
        task_id='validate_environment',
        python_callable=validate_environment,
        op_kwargs={'postgres_conn_id': POSTGRES_CONN_ID, 'data_dir': DATA_DIR, 'staging_file_path': STAGING_FILE_PATH},
    )

    wait_for_data_files = FileSensor(
        task_id='wait_for_source_files',
        filepath=STAGING_FILE_PATH,
        poke_interval=60,
        timeout=600,
        mode='poke',
    )

    # 2. Schema and Staging Setup
    init_schemas = PostgresOperator(
        task_id='initialize_schemas',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='/opt/airflow/sql/01_create_schemas.sql',
    )

    create_extensions = PostgresOperator(
        task_id='create_extensions',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='/opt/airflow/sql/02_create_extensions.sql',
    )

    create_staging_tables = PostgresOperator(
        task_id='create_staging_tables',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='/opt/airflow/sql/ddl/staging_schema.sql',
    )

    # 3. Core ETL tasks
    ingest_data = PythonOperator(
        task_id='ingest_source_data',
        python_callable=run_data_ingestion,
        op_kwargs={'data_dir': DATA_DIR},
    )

    staging_transform = PostgresOperator(
        task_id='staging_transformations',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='/opt/airflow/scripts/transform/staging/load_staging.sql',
    )

    # 4. Warehouse Dimension and Fact Loading
    load_dim_date = PostgresOperator(
        task_id='load_dim_date',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='/opt/airflow/scripts/transform/warehouse/dim_date.sql',
    )

    load_dim_customer = PostgresOperator(
        task_id='load_dim_customer',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='/opt/airflow/scripts/transform/warehouse/dim_customer.sql',
    )

    load_dim_product = PostgresOperator(
        task_id='load_dim_product',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='/opt/airflow/scripts/transform/warehouse/dim_product.sql',
    )

    load_dim_merchant = PostgresOperator(
        task_id='load_dim_merchant',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='/opt/airflow/scripts/transform/warehouse/dim_merchant.sql',
    )

    load_dim_campaign = PostgresOperator(
        task_id='load_dim_campaign',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='/opt/airflow/scripts/transform/warehouse/dim_campaign.sql',
    )

    load_fact_orders = PostgresOperator(
        task_id='load_fact_orders',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='/opt/airflow/scripts/transform/warehouse/fact_orders.sql',
    )
    
    # 5. Quality, Metrics and Notifications
    data_quality_checks = PythonOperator(
        task_id='data_quality_validation',
        python_callable=run_data_quality_checks,
    )

    collect_metrics = PythonOperator(
        task_id='collect_pipeline_metrics',
        python_callable=collect_pipeline_metrics,
        op_kwargs={'postgres_conn_id': POSTGRES_CONN_ID},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    end_pipeline = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED)

    success_notification = PythonOperator(
        task_id='success_notification',
        python_callable=send_success_notification,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # 6. Failure Handling Path
    failure_notification = PythonOperator(
        task_id='failure_notification',
        python_callable=send_failure_notification,
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    
    cleanup_on_failure_task = PythonOperator(
        task_id='cleanup_on_failure',
        python_callable=cleanup_on_failure,
        op_kwargs={'postgres_conn_id': POSTGRES_CONN_ID},
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # --- Task Dependencies ---
    start_pipeline >> validate_env
    validate_env >> [wait_for_data_files, init_schemas, create_extensions]
    [init_schemas, create_extensions, wait_for_data_files] >> create_staging_tables

    create_staging_tables >> ingest_data >> staging_transform

    staging_transform >> [load_dim_date, load_dim_customer, load_dim_product]

    # Set up dependencies for merchant and campaign dimensions
    for upstream in [load_dim_date, load_dim_customer, load_dim_product]:
        upstream >> load_dim_merchant
        upstream >> load_dim_campaign

    [load_dim_merchant, load_dim_campaign] >> load_fact_orders

    load_fact_orders >> data_quality_checks

    data_quality_checks >> collect_metrics >> success_notification >> end_pipeline

    # Define failure path
    [validate_env, wait_for_data_files, create_staging_tables, ingest_data, staging_transform, load_fact_orders, data_quality_checks] >> cleanup_on_failure_task >> failure_notification >> end_pipeline

# Instantiate the DAG
weadapt_pipeline_dag = weadapt_pipeline()

