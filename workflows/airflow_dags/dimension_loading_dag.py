"""
weadapt Data Warehouse - Dimension Loading DAG

This DAG handles the loading of all dimension tables in the warehouse layer.
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
    dag_id='weadapt_dimension_loading',
    default_args=DEFAULT_ARGS,
    description='Load all dimension tables in the warehouse',
    schedule_interval=None,  # Manual only
    tags=['weadapt', 'dimensions', 'warehouse'],
    catchup=False,
    max_active_runs=1,
    template_searchpath=['/opt/airflow/scripts'],
)
def dimension_loading():
    """
    Dimension loading DAG for weadapt DWH
    """

    start_dims = EmptyOperator(task_id='start_dimension_loading')

    load_dim_date = PostgresOperator(
        task_id='load_dim_date',
        postgres_conn_id='postgres_default',
        sql='transform/warehouse/dim_date.sql',
    )

    load_dim_customer = PostgresOperator(
        task_id='load_dim_customer',
        postgres_conn_id='postgres_default',
        sql='transform/warehouse/dim_customer.sql',
    )

    load_dim_product = PostgresOperator(
        task_id='load_dim_product',
        postgres_conn_id='postgres_default',
        sql='transform/warehouse/dim_product.sql',
    )

    load_dim_merchant = PostgresOperator(
        task_id='load_dim_merchant',
        postgres_conn_id='postgres_default',
        sql='transform/warehouse/dim_merchant.sql',
    )

    load_dim_staff = PostgresOperator(
        task_id='load_dim_staff',
        postgres_conn_id='postgres_default',
        sql='transform/warehouse/dim_staff.sql',
    )

    load_dim_campaign = PostgresOperator(
        task_id='load_dim_campaign',
        postgres_conn_id='postgres_default',
        sql='transform/warehouse/dim_campaign.sql',
    )

    end_dims = EmptyOperator(task_id='end_dimension_loading')

    # Dependencies - load dimensions in parallel where possible
    start_dims >> load_dim_date

    # Customer, product, merchant, staff, campaign can load after date
    load_dim_date >> [load_dim_customer, load_dim_product, load_dim_merchant, load_dim_staff, load_dim_campaign]

    [load_dim_customer, load_dim_product, load_dim_merchant, load_dim_staff, load_dim_campaign] >> end_dims

dimension_loading_dag = dimension_loading()

