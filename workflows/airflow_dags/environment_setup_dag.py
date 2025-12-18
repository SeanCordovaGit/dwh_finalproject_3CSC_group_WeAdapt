"""
weadapt Data Warehouse - Environment Setup DAG

This DAG handles the initial setup of the data warehouse environment:
- Schema creation
- Database extensions
- Staging table creation
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
    dag_id='weadapt_environment_setup',
    default_args=DEFAULT_ARGS,
    description='Environment setup for weadapt Data Warehouse',
    schedule_interval=None,  # Manual only
    tags=['weadapt', 'setup', 'infrastructure'],
    catchup=False,
    max_active_runs=1,
    template_searchpath=['/opt/airflow/sql', '/opt/airflow/sql/ddl'],
)
def environment_setup():
    """
    Environment setup DAG for weadapt DWH
    """

    start_setup = EmptyOperator(task_id='start_setup')

    init_schemas = PostgresOperator(
        task_id='initialize_schemas',
        postgres_conn_id='postgres_default',
        sql='01_create_schemas.sql',
    )

    create_extensions = PostgresOperator(
        task_id='create_extensions',
        postgres_conn_id='postgres_default',
        sql='02_create_extensions.sql',
    )

    create_staging_tables = PostgresOperator(
        task_id='create_staging_tables',
        postgres_conn_id='postgres_default',
        sql='staging_schema.sql',
    )

    create_warehouse_tables = PostgresOperator(
        task_id='create_warehouse_tables',
        postgres_conn_id='postgres_default',
        sql='warehouse_schema.sql',
    )

    end_setup = EmptyOperator(task_id='end_setup')

    # Dependencies
    start_setup >> init_schemas >> create_extensions >> create_staging_tables >> create_warehouse_tables >> end_setup

environment_setup_dag = environment_setup()

