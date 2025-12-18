"""
WeAdapt Data Warehouse - Data Ingestion DAG

This DAG handles the ingestion of raw data files into staging tables.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

sys.path.insert(0, os.path.abspath('/opt/airflow/scripts'))

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

try:
    from ingestion.ingestion_runner import run_data_ingestion
except ImportError as e:
    logging.critical(f"DAG Import Error: {e}")
    raise

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
    dag_id='weadapt_data_ingestion',
    default_args=DEFAULT_ARGS,
    description='Data ingestion from source files to staging tables',
    schedule_interval=None,  # Manual only
    tags=['weadapt', 'ingestion', 'staging'],
    catchup=False,
    max_active_runs=1,
)
def data_ingestion():
    """
    Data ingestion DAG for WeAdapt DWH
    """

    start_ingestion = EmptyOperator(task_id='start_ingestion')

    ingest_data = PythonOperator(
        task_id='ingest_source_data',
        python_callable=run_data_ingestion,
        op_kwargs={'data_dir': '/opt/airflow/sql'},
    )

    end_ingestion = EmptyOperator(task_id='end_ingestion')

    # Dependencies - run ingestion directly (no file sensor needed for local testing)
    start_ingestion >> ingest_data >> end_ingestion

data_ingestion_dag = data_ingestion()

