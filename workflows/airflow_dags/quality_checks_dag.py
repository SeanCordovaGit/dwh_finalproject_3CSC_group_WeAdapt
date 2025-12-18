"""
weadapt Data Warehouse - Quality Checks DAG

This DAG handles data quality validation after warehouse loading.
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
    from quality.dq_runner import run_data_quality_checks
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
    dag_id='weadapt_quality_checks',
    default_args=DEFAULT_ARGS,
    description='Run data quality checks on warehouse tables',
    schedule_interval=None,  # Manual only
    tags=['weadapt', 'quality', 'validation'],
    catchup=False,
    max_active_runs=1,
)
def quality_checks():
    """
    Quality checks DAG for weadapt DWH
    """

    start_quality = EmptyOperator(task_id='start_quality_checks')

    data_quality_checks = PythonOperator(
        task_id='data_quality_validation',
        python_callable=run_data_quality_checks,
    )

    end_quality = EmptyOperator(task_id='end_quality_checks')

    # Dependencies
    start_quality >> data_quality_checks >> end_quality

quality_checks_dag = quality_checks()

