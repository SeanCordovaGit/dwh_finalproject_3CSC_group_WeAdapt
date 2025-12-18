"""
weadapt Data Warehouse - Staging Transformation DAG

This DAG handles the transformation of raw staging data into cleansed staging tables.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

sys.path.insert(0, os.path.abspath('/opt/airflow/scripts'))

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    dag_id='weadapt_staging_transform',
    default_args=DEFAULT_ARGS,
    description='Transform raw staging data into cleansed staging tables',
    schedule_interval=None,  # Manual only
    tags=['weadapt', 'staging', 'transform'],
    catchup=False,
    max_active_runs=1,
    template_searchpath=['/opt/airflow/scripts'],
)
def staging_transform():
    """
    Staging transformation DAG for weadapt DWH
    """

    start_transform = EmptyOperator(task_id='start_staging_transform')

    staging_transform = PostgresOperator(
        task_id='staging_transformations',
        postgres_conn_id='postgres_default',
        sql='transform/staging/load_staging.sql',
    )

    end_transform = EmptyOperator(task_id='end_staging_transform')

    # Dependencies
    start_transform >> staging_transform >> end_transform

staging_transform_dag = staging_transform()

