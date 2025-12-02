from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import sys

# Add app directory to path
sys.path.append('/opt/airflow/app')

from ingest_all_data import load_to_staging
from transform_load import run_transformations

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'shopzada_etl',
    default_args=default_args,
    description='ETL pipeline for ShopZada DWH',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Create staging directory
create_staging_dir = BashOperator(
    task_id='create_staging_directory',
    bash_command='mkdir -p /opt/airflow/data/staging',
    dag=dag,
)

# Extract and Load to Staging
extract_load_staging = PythonOperator(
    task_id='extract_load_staging',
    python_callable=load_to_staging,
    dag=dag,
)

# Transform and Load to Warehouse
transform_load_warehouse = PythonOperator(
    task_id='transform_load_warehouse',
    python_callable=run_transformations,
    dag=dag,
)

# Create presentation views
create_views = BashOperator(
    task_id='create_presentation_views',
    bash_command='''
    psql postgresql://shopzada:shopzada@shopzada-dw:5432/shopzada_dw -f /opt/airflow/sql/presentation_views.sql
    ''',
    dag=dag,
)

# Data quality checks
data_quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command='''
    psql postgresql://shopzada:shopzada@shopzada-dw:5432/shopzada_dw -c "
    SELECT '\''staging.stg_users'\'' as table_name, COUNT(*) as row_count FROM staging.stg_users
    UNION ALL
    SELECT '\''warehouse.dim_customer'\'', COUNT(*) FROM warehouse.dim_customer
    UNION ALL
    SELECT '\''warehouse.fact_sales'\'', COUNT(*) FROM warehouse.fact_sales
    UNION ALL
    SELECT '\''presentation.vw_campaign_order_volume'\'', COUNT(*) FROM presentation.vw_campaign_order_volume;
    "
    ''',
    dag=dag,
)

# Set task dependencies
create_staging_dir >> extract_load_staging >> transform_load_warehouse >> create_views >> data_quality_check
