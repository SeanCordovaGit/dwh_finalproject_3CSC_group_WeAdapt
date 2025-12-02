import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime, timedelta
import numpy as np
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
def get_db_engine():
    """Create database engine for PostgreSQL"""
    db_host = os.getenv('DB_HOST', 'shopzada-dw')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'shopzada_dw')
    db_user = os.getenv('DB_USER', 'shopzada')
    db_password = os.getenv('DB_PASSWORD', 'shopzada')
    return create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

def truncate_table(engine, schema, table_name):
    """Safely truncate a table using CASCADE to handle foreign key constraints"""
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name} CASCADE"))
        conn.commit()

def create_time_dimension(start_date='2020-01-01', end_date='2025-12-31'):
    """Create time dimension table with date attributes"""
    engine = get_db_engine()

    # Check if time dimension already exists and has data
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM warehouse.dim_time")).fetchone()
        if result[0] > 0:
            logger.info("Time dimension already populated, skipping creation")
            return

    # Generate date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    time_data = []
    for date in date_range:
        time_data.append({
            'date_actual': date.date(),
            'day_of_week': date.weekday() + 1,  # Monday = 1, Sunday = 7
            'day_name': date.strftime('%A'),
            'month_actual': date.month,
            'month_name': date.strftime('%B'),
            'quarter_actual': (date.month - 1) // 3 + 1,
            'year_actual': date.year,
            'is_weekend': date.weekday() >= 5  # Saturday = 5, Sunday = 6
        })

    time_df = pd.DataFrame(time_data)
    time_df.to_sql('dim_time', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Created time dimension with {len(time_df)} records")

def load_customer_dimension():
    """Load customer dimension from staging"""
    engine = get_db_engine()

    # Truncate existing data
    truncate_table(engine, 'warehouse', 'dim_customer')

    query = """
    SELECT
        user_id,
        name,
        job_title,
        job_level,
        CURRENT_DATE as effective_date,
        '9999-12-31'::DATE as expiry_date,
        TRUE as is_current
    FROM staging.stg_users
    """

    df = pd.read_sql(query, engine)

    # Handle missing values
    df = df.fillna({
        'name': 'Unknown',
        'job_title': 'Unknown',
        'job_level': 'Unknown'
    })

    df.to_sql('dim_customer', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} customers to dimension table")

def load_customer_dimension_no_truncate():
    """Load customer dimension from staging (without truncation)"""
    engine = get_db_engine()

    query = """
    SELECT
        user_id,
        name,
        job_title,
        job_level,
        CURRENT_DATE as effective_date,
        '9999-12-31'::DATE as expiry_date,
        TRUE as is_current
    FROM staging.stg_users
    """

    df = pd.read_sql(query, engine)

    # Handle missing values
    df = df.fillna({
        'name': 'Unknown',
        'job_title': 'Unknown',
        'job_level': 'Unknown'
    })

    df.to_sql('dim_customer', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} customers to dimension table")

def load_product_dimension():
    """Load product dimension from staging"""
    engine = get_db_engine()

    # Truncate existing data
    truncate_table(engine, 'warehouse', 'dim_product')

    query = """
    SELECT
        product_id,
        product_name,
        product_type,
        product_type as product_category,  -- Using product_type as category for now
        price,
        CURRENT_DATE as effective_date,
        '9999-12-31'::DATE as expiry_date,
        TRUE as is_current
    FROM staging.stg_products
    """

    df = pd.read_sql(query, engine)

    # Handle missing values
    df = df.fillna({
        'product_name': 'Unknown',
        'product_type': 'Unknown',
        'product_category': 'Unknown',
        'price': 0.0
    })

    df.to_sql('dim_product', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} products to dimension table")

def load_product_dimension_no_truncate():
    """Load product dimension from staging (without truncation)"""
    engine = get_db_engine()

    query = """
    SELECT
        product_id,
        product_name,
        product_type,
        product_type as product_category,  -- Using product_type as category for now
        price,
        CURRENT_DATE as effective_date,
        '9999-12-31'::DATE as expiry_date,
        TRUE as is_current
    FROM staging.stg_products
    """

    df = pd.read_sql(query, engine)

    # Handle missing values
    df = df.fillna({
        'product_name': 'Unknown',
        'product_type': 'Unknown',
        'product_category': 'Unknown',
        'price': 0.0
    })

    df.to_sql('dim_product', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} products to dimension table")

def load_campaign_dimension():
    """Load campaign dimension from staging"""
    engine = get_db_engine()

    # Truncate existing data
    truncate_table(engine, 'warehouse', 'dim_campaign')

    query = """
    SELECT
        campaign_id,
        campaign_name,
        campaign_description,
        CASE
            WHEN discount LIKE '%1%' THEN 1.0
            WHEN discount LIKE '%5%' THEN 5.0
            WHEN discount LIKE '%10%' THEN 10.0
            WHEN discount LIKE '%20%' THEN 20.0
            ELSE 0.0
        END as discount_percentage,
        CURRENT_DATE as effective_date,
        '9999-12-31'::DATE as expiry_date,
        TRUE as is_current
    FROM staging.stg_campaigns
    """

    df = pd.read_sql(query, engine)

    # Handle missing values
    df = df.fillna({
        'campaign_name': 'Unknown',
        'campaign_description': '',
        'discount_percentage': 0.0
    })

    df.to_sql('dim_campaign', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} campaigns to dimension table")

def load_campaign_dimension_no_truncate():
    """Load campaign dimension from staging (without truncation)"""
    engine = get_db_engine()

    query = """
    SELECT
        campaign_id,
        campaign_name,
        campaign_description,
        CASE
            WHEN discount LIKE '%1%' THEN 1.0
            WHEN discount LIKE '%5%' THEN 5.0
            WHEN discount LIKE '%10%' THEN 10.0
            WHEN discount LIKE '%20%' THEN 20.0
            ELSE 0.0
        END as discount_percentage,
        CURRENT_DATE as effective_date,
        '9999-12-31'::DATE as expiry_date,
        TRUE as is_current
    FROM staging.stg_campaigns
    """

    df = pd.read_sql(query, engine)

    # Handle missing values
    df = df.fillna({
        'campaign_name': 'Unknown',
        'campaign_description': '',
        'discount_percentage': 0.0
    })

    df.to_sql('dim_campaign', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} campaigns to dimension table")

def load_staff_dimension():
    """Load staff dimension from staging"""
    engine = get_db_engine()

    # Truncate existing data
    truncate_table(engine, 'warehouse', 'dim_staff')

    # This assumes the HTML table structure - may need adjustment based on actual data
    query = """
    SELECT
        COALESCE(staff_id, 'STAFF_' || ROW_NUMBER() OVER ()) as staff_id,
        COALESCE(name, 'Unknown') as name,
        COALESCE(department, 'Unknown') as department,
        COALESCE(position, 'Unknown') as position,
        CURRENT_DATE as hire_date,
        CURRENT_DATE as effective_date,
        '9999-12-31'::DATE as expiry_date,
        TRUE as is_current
    FROM staging.stg_staff
    """

    df = pd.read_sql(query, engine)

    df.to_sql('dim_staff', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} staff members to dimension table")

def load_staff_dimension_no_truncate():
    """Load staff dimension from staging (without truncation)"""
    engine = get_db_engine()

    # This assumes the HTML table structure - may need adjustment based on actual data
    query = """
    SELECT
        COALESCE(staff_id, 'STAFF_' || ROW_NUMBER() OVER ()) as staff_id,
        COALESCE(name, 'Unknown') as name,
        COALESCE(department, 'Unknown') as department,
        COALESCE(position, 'Unknown') as position,
        CURRENT_DATE as hire_date,
        CURRENT_DATE as effective_date,
        '9999-12-31'::DATE as expiry_date,
        TRUE as is_current
    FROM staging.stg_staff
    """

    df = pd.read_sql(query, engine)

    df.to_sql('dim_staff', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} staff members to dimension table")

def load_fact_sales_no_truncate():
    """Load sales fact table from staging orders (without truncation)"""
    engine = get_db_engine()

    # Preload dimension keys for performance
    logger.info("Preloading dimension keys...")
    time_keys = pd.read_sql("SELECT date_actual, time_key FROM warehouse.dim_time", engine)
    time_key_map = dict(zip(time_keys['date_actual'], time_keys['time_key']))

    customer_keys = pd.read_sql("SELECT user_id, customer_key FROM warehouse.dim_customer", engine)
    customer_key_map = dict(zip(customer_keys['user_id'], customer_keys['customer_key']))

    product_keys = pd.read_sql("SELECT product_id, product_key FROM warehouse.dim_product", engine)
    product_key_map = dict(zip(product_keys['product_id'], product_keys['product_key']))

    campaign_keys = pd.read_sql("SELECT campaign_id, campaign_key FROM warehouse.dim_campaign", engine)
    campaign_key_map = dict(zip(campaign_keys['campaign_id'], campaign_keys['campaign_key']))

    staff_keys = pd.read_sql("SELECT staff_id, staff_key FROM warehouse.dim_staff", engine)
    staff_key_map = dict(zip(staff_keys['staff_id'], staff_keys['staff_key']))

    query = """
    SELECT
        o.*,
        -- Generate some sample data for missing fields
        CASE WHEN RANDOM() < 0.3 THEN 'CAMP001' ELSE NULL END as campaign_id,
        CASE WHEN RANDOM() < 0.5 THEN 'STAFF_' || (RANDOM() * 10 + 1)::INT ELSE 'STAFF_1' END as staff_id,
        -- Add timestamps if missing
        COALESCE(o.order_date, CURRENT_TIMESTAMP - INTERVAL '30 days' * RANDOM()) as order_date,
        COALESCE(o.delivery_date, o.order_date + INTERVAL '3 days') as delivery_date
    FROM staging.stg_orders o
    """

    df = pd.read_sql(query, engine)

    # Ensure we have required fields
    if df.empty:
        logger.warning("No order data found in staging")
        return

    # Create fact table data with preloaded keys
    fact_data = []

    for _, row in df.iterrows():
        # Get dimension keys from preloaded maps
        time_key = time_key_map.get(row['order_date'].date() if hasattr(row['order_date'], 'date') else row['order_date'], 1)
        customer_key = customer_key_map.get(row.get('user_id'), 1)
        product_key = product_key_map.get(row.get('product_id'), 1)
        campaign_key = campaign_key_map.get(row.get('campaign_id'))
        staff_key = staff_key_map.get(row.get('staff_id'), 1)

        # Calculate amounts
        quantity = row.get('quantity', 1)
        unit_price = row.get('price', 0.0)
        total_amount = quantity * unit_price
        discount_amount = 0.0  # Could be calculated based on campaign

        fact_data.append({
            'time_key': time_key,
            'customer_key': customer_key,
            'product_key': product_key,
            'campaign_key': campaign_key,
            'staff_key': staff_key,
            'order_id': row.get('order_id', f"ORDER_{len(fact_data)}"),
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'discount_amount': discount_amount,
            'order_date': row['order_date'],
            'delivery_date': row['delivery_date'],
            'order_status': row.get('status', 'completed')
        })

    fact_df = pd.DataFrame(fact_data)
    fact_df.to_sql('fact_sales', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Loaded {len(fact_df)} sales records to fact table")

def create_campaign_performance_fact_no_truncate():
    """Create sample campaign performance data (without truncation)"""
    engine = get_db_engine()

    # Get campaigns
    campaigns_query = "SELECT campaign_key, campaign_id FROM warehouse.dim_campaign LIMIT 10"
    campaigns_df = pd.read_sql(campaigns_query, engine)

    if campaigns_df.empty:
        logger.warning("No campaigns found for performance data")
        return

    # Generate sample performance data
    performance_data = []
    for _, campaign in campaigns_df.iterrows():
        performance_data.append({
            'campaign_key': campaign['campaign_key'],
            'time_key': np.random.randint(1, 365),  # Random date
            'impressions': np.random.randint(1000, 100000),
            'clicks': np.random.randint(10, 10000),
            'conversions': np.random.randint(1, 1000),
            'revenue': np.random.uniform(100, 10000),
            'cost': np.random.uniform(50, 5000)
        })

    perf_df = pd.DataFrame(performance_data)
    perf_df.to_sql('fact_campaign_performance', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Created {len(perf_df)} campaign performance records")

def load_fact_sales():
    """Load sales fact table from staging orders"""
    engine = get_db_engine()

    # Truncate existing data
    truncate_table(engine, 'warehouse', 'fact_sales')

    # Preload dimension keys for performance
    logger.info("Preloading dimension keys...")
    time_keys = pd.read_sql("SELECT date_actual, time_key FROM warehouse.dim_time", engine)
    time_key_map = dict(zip(time_keys['date_actual'], time_keys['time_key']))

    customer_keys = pd.read_sql("SELECT user_id, customer_key FROM warehouse.dim_customer", engine)
    customer_key_map = dict(zip(customer_keys['user_id'], customer_keys['customer_key']))

    product_keys = pd.read_sql("SELECT product_id, product_key FROM warehouse.dim_product", engine)
    product_key_map = dict(zip(product_keys['product_id'], product_keys['product_key']))

    campaign_keys = pd.read_sql("SELECT campaign_id, campaign_key FROM warehouse.dim_campaign", engine)
    campaign_key_map = dict(zip(campaign_keys['campaign_id'], campaign_keys['campaign_key']))

    staff_keys = pd.read_sql("SELECT staff_id, staff_key FROM warehouse.dim_staff", engine)
    staff_key_map = dict(zip(staff_keys['staff_id'], staff_keys['staff_key']))

    query = """
    SELECT
        o.*,
        -- Generate some sample data for missing fields
        CASE WHEN RANDOM() < 0.3 THEN 'CAMP001' ELSE NULL END as campaign_id,
        CASE WHEN RANDOM() < 0.5 THEN 'STAFF_' || (RANDOM() * 10 + 1)::INT ELSE 'STAFF_1' END as staff_id,
        -- Add timestamps if missing
        COALESCE(o.order_date, CURRENT_TIMESTAMP - INTERVAL '30 days' * RANDOM()) as order_date,
        COALESCE(o.delivery_date, o.order_date + INTERVAL '3 days') as delivery_date
    FROM staging.stg_orders o
    """

    df = pd.read_sql(query, engine)

    # Ensure we have required fields
    if df.empty:
        logger.warning("No order data found in staging")
        return

    # Create fact table data with preloaded keys
    fact_data = []

    for _, row in df.iterrows():
        # Get dimension keys from preloaded maps
        time_key = time_key_map.get(row['order_date'].date() if hasattr(row['order_date'], 'date') else row['order_date'], 1)
        customer_key = customer_key_map.get(row.get('user_id'), 1)
        product_key = product_key_map.get(row.get('product_id'), 1)
        campaign_key = campaign_key_map.get(row.get('campaign_id'))
        staff_key = staff_key_map.get(row.get('staff_id'), 1)

        # Calculate amounts
        quantity = row.get('quantity', 1)
        unit_price = row.get('price', 0.0)
        total_amount = quantity * unit_price
        discount_amount = 0.0  # Could be calculated based on campaign

        fact_data.append({
            'time_key': time_key,
            'customer_key': customer_key,
            'product_key': product_key,
            'campaign_key': campaign_key,
            'staff_key': staff_key,
            'order_id': row.get('order_id', f"ORDER_{len(fact_data)}"),
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'discount_amount': discount_amount,
            'order_date': row['order_date'],
            'delivery_date': row['delivery_date'],
            'order_status': row.get('status', 'completed')
        })

    fact_df = pd.DataFrame(fact_data)
    fact_df.to_sql('fact_sales', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Loaded {len(fact_df)} sales records to fact table")

def get_time_key(date):
    """Get time dimension key for a date"""
    if pd.isna(date):
        return 1  # Default to first date
    engine = get_db_engine()
    query = text("SELECT time_key FROM warehouse.dim_time WHERE date_actual = :date")
    result = engine.execute(query, {'date': date.date() if hasattr(date, 'date') else date}).fetchone()
    return result[0] if result else 1

def get_customer_key(user_id):
    """Get customer dimension key"""
    if pd.isna(user_id) or user_id is None:
        return 1  # Unknown customer
    engine = get_db_engine()
    query = text("SELECT customer_key FROM warehouse.dim_customer WHERE user_id = :user_id")
    result = engine.execute(query, {'user_id': user_id}).fetchone()
    return result[0] if result else 1

def get_product_key(product_id):
    """Get product dimension key"""
    if pd.isna(product_id) or product_id is None:
        return 1  # Unknown product
    engine = get_db_engine()
    query = text("SELECT product_key FROM warehouse.dim_product WHERE product_id = :product_id")
    result = engine.execute(query, {'product_id': product_id}).fetchone()
    return result[0] if result else 1

def get_campaign_key(campaign_id):
    """Get campaign dimension key"""
    if pd.isna(campaign_id) or campaign_id is None:
        return None  # No campaign
    engine = get_db_engine()
    query = text("SELECT campaign_key FROM warehouse.dim_campaign WHERE campaign_id = :campaign_id")
    result = engine.execute(query, {'campaign_id': campaign_id}).fetchone()
    return result[0] if result else None

def get_staff_key(staff_id):
    """Get staff dimension key"""
    if pd.isna(staff_id) or staff_id is None:
        return 1  # Default staff
    engine = get_db_engine()
    query = text("SELECT staff_key FROM warehouse.dim_staff WHERE staff_id = :staff_id")
    result = engine.execute(query, {'staff_id': staff_id}).fetchone()
    return result[0] if result else 1

def create_campaign_performance_fact():
    """Create sample campaign performance data"""
    engine = get_db_engine()

    # Truncate existing data
    truncate_table(engine, 'warehouse', 'fact_campaign_performance')

    # Get campaigns
    campaigns_query = "SELECT campaign_key, campaign_id FROM warehouse.dim_campaign LIMIT 10"
    campaigns_df = pd.read_sql(campaigns_query, engine)

    if campaigns_df.empty:
        logger.warning("No campaigns found for performance data")
        return

    # Generate sample performance data
    performance_data = []
    for _, campaign in campaigns_df.iterrows():
        performance_data.append({
            'campaign_key': campaign['campaign_key'],
            'time_key': np.random.randint(1, 365),  # Random date
            'impressions': np.random.randint(1000, 100000),
            'clicks': np.random.randint(10, 10000),
            'conversions': np.random.randint(1, 1000),
            'revenue': np.random.uniform(100, 10000),
            'cost': np.random.uniform(50, 5000)
        })

    perf_df = pd.DataFrame(performance_data)
    perf_df.to_sql('fact_campaign_performance', engine, schema='warehouse', if_exists='append', index=False)
    logger.info(f"Created {len(perf_df)} campaign performance records")

def run_transformations():
    """Run all transformation steps"""
    logger.info("Starting data transformations...")

    engine = get_db_engine()

    try:
        with engine.begin() as conn:
            # Truncate all existing warehouse tables in dependency order (facts first)
            logger.info("Clearing existing warehouse data...")
            truncate_table(engine, 'warehouse', 'fact_campaign_performance')
            truncate_table(engine, 'warehouse', 'fact_sales')
            truncate_table(engine, 'warehouse', 'dim_customer')
            truncate_table(engine, 'warehouse', 'dim_product')
            truncate_table(engine, 'warehouse', 'dim_campaign')
            truncate_table(engine, 'warehouse', 'dim_staff')
            # Note: dim_time is not truncated as it's static

            # Create time dimension (only if not exists)
            create_time_dimension()

            # Load dimensions (without individual truncation since we did it above)
            load_customer_dimension_no_truncate()
            load_product_dimension_no_truncate()
            load_campaign_dimension_no_truncate()
            load_staff_dimension_no_truncate()

            # Load facts (without individual truncation since we did it above)
            load_fact_sales_no_truncate()
            create_campaign_performance_fact_no_truncate()

        logger.info("Data transformations completed successfully")

    except Exception as e:
        logger.error(f"Error during transformations: {e}")
        raise

if __name__ == "__main__":
    run_transformations()
