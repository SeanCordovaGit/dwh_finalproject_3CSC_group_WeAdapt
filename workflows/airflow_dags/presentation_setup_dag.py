"""
weadapt Data Warehouse - Presentation Setup DAG

This DAG handles the creation of presentation layer objects:
- Analytical views
- Materialized views
- BI-ready structures
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
    dag_id='weadapt_presentation_setup',
    default_args=DEFAULT_ARGS,
    description='Create presentation layer views and materialized views',
    schedule_interval=None,  # Manual only
    tags=['weadapt', 'presentation', 'views'],
    catchup=False,
    max_active_runs=1,
    template_searchpath=['/opt/airflow/sql', '/opt/airflow/sql/ddl', '/opt/airflow/sql/views', '/opt/airflow/sql/materialized'],
)
def presentation_setup():
    """
    Presentation setup DAG for weadapt DWH
    """

    start_presentation = EmptyOperator(task_id='start_presentation_setup')

    create_warehouse_schema = PostgresOperator(
        task_id='create_warehouse_schema',
        postgres_conn_id='postgres_default',
        sql='warehouse_schema.sql',
    )

    create_presentation_schema = PostgresOperator(
        task_id='create_presentation_schema',
        postgres_conn_id='postgres_default',
        sql='presentation_schema.sql',
    )

    create_customer_segments_view = PostgresOperator(
        task_id='create_customer_segments_view',
        postgres_conn_id='postgres_default',
        sql='view_customer_segments.sql',
    )

    create_merchant_performance_view = PostgresOperator(
        task_id='create_merchant_performance_view',
        postgres_conn_id='postgres_default',
        sql='view_merchant_performance.sql',
    )

    create_daily_sales_agg = PostgresOperator(
        task_id='create_daily_sales_agg',
        postgres_conn_id='postgres_default',
        sql='mat_agg_daily_sales.sql',
    )

    create_dashboard_summary_view = PostgresOperator(
        task_id='create_dashboard_summary_view',
        postgres_conn_id='postgres_default',
        sql='view_dashboard_summary.sql',
    )

    create_orders_dimensions_view = PostgresOperator(
        task_id='create_orders_dimensions_view',
        postgres_conn_id='postgres_default',
        sql='view_orders_with_dimensions.sql',
    )

    end_presentation = EmptyOperator(task_id='end_presentation_setup')

    # Dependencies - create schemas first, then views in dependency order
    start_presentation >> create_warehouse_schema
    start_presentation >> create_presentation_schema

    # First create basic views (no dependencies)
    create_warehouse_schema >> create_customer_segments_view
    create_warehouse_schema >> create_merchant_performance_view
    create_warehouse_schema >> create_daily_sales_agg
    create_warehouse_schema >> create_orders_dimensions_view

    create_presentation_schema >> create_customer_segments_view
    create_presentation_schema >> create_merchant_performance_view
    create_presentation_schema >> create_daily_sales_agg
    create_presentation_schema >> create_orders_dimensions_view

    # Dashboard summary depends on other views, so create it last
    create_customer_segments_view >> create_dashboard_summary_view
    create_merchant_performance_view >> create_dashboard_summary_view
    create_daily_sales_agg >> create_dashboard_summary_view

    create_customer_segments_view >> end_presentation
    create_merchant_performance_view >> end_presentation
    create_daily_sales_agg >> end_presentation
    create_orders_dimensions_view >> end_presentation
    create_dashboard_summary_view >> end_presentation


presentation_setup_dag = presentation_setup()
