"""
weadapt Data Warehouse - Fact Loading DAG

This DAG handles the loading of fact tables in the warehouse layer.
"""

import sys
import os
from datetime import datetime, timedelta

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
    dag_id='weadapt_fact_loading',
    default_args=DEFAULT_ARGS,
    description='Load fact tables in the warehouse',
    schedule_interval=None,  # Manual only
    tags=['weadapt', 'facts', 'warehouse'],
    catchup=False,
    max_active_runs=1,
    template_searchpath=['/opt/airflow/scripts'],
)
def fact_loading():
    """
    Fact loading DAG for weadapt DWH
    """

    start_facts = EmptyOperator(task_id='start_fact_loading')

    load_fact_orders = PostgresOperator(
        task_id='load_fact_orders',
        postgres_conn_id='postgres_default',
        sql='transform/warehouse/fact_orders_lightning.sql',
    )

    end_facts = EmptyOperator(task_id='end_fact_loading')

    # Dependencies - sequential execution to avoid conflicts
    start_facts >> load_fact_orders >> end_facts

fact_loading_dag = fact_loading()
