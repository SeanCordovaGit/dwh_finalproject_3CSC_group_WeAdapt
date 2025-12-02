import pandas as pd
import json
import pickle
import os
from sqlalchemy import create_engine
import logging

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

# base loaders
def load_csv(path):
    return pd.read_csv(path)

def load_json(path):
    return pd.read_json(path)

def load_excel(path):
    return pd.read_excel(path)

def load_html(path):
    return pd.read_html(path)[0]

def load_parquet(path):
    return pd.read_parquet(path)

def load_pickle(path):
    with open(path, 'rb') as f:
        return pickle.load(f)

# dispatch
def ingest(path):
    ext = os.path.splitext(path)[1].lower()

    if ext == ".csv":
        return load_csv(path)
    if ext == ".json":
        return load_json(path)
    if ext == ".xlsx":
        return load_excel(path)
    if ext == ".html":
        return load_html(path)
    if ext == ".parquet":
        return load_parquet(path)
    if ext == ".pickle":
        return load_pickle(path)

    raise ValueError(f"Unsupported file format: {ext}")

# Data ingestion functions
def ingest_business_product_list():
    """Load product data from Excel file"""
    df = ingest("data/Business Department/product_list.xlsx")
    logger.info(f"Loaded {len(df)} products")
    return df

def ingest_customer_users():
    """Load user data from CSV file"""
    df = ingest("data/Customer Management Department/user_job.csv")
    logger.info(f"Loaded {len(df)} users")
    return df

def ingest_marketing_campaigns():
    """Load campaign data from CSV file"""
    df = ingest("data/Marketing Department/campaign_data.csv")
    logger.info(f"Loaded {len(df)} campaigns")
    return df

def ingest_marketing_transactional_campaigns():
    """Load transactional campaign data from CSV file"""
    df = ingest("data/Marketing Department/transactional_campaign_data.csv")
    logger.info(f"Loaded {len(df)} transactional campaigns")
    return df

def ingest_enterprise_staff():
    """Load staff data from HTML file"""
    df = ingest("data/Enterprise Department/staff_data.html")
    logger.info(f"Loaded {len(df)} staff members")
    return df

def ingest_enterprise_merchants():
    """Load merchant data from HTML file"""
    df = ingest("data/Enterprise Department/merchant_data.html")
    logger.info(f"Loaded {len(df)} merchants")
    return df

def ingest_order_data():
    """Load and combine order data from multiple parquet files"""
    dfs = []
    for i in range(1, 4):
        try:
            df = ingest(f"data/Enterprise Department/order_with_merchant_data{i}.parquet")
            dfs.append(df)
            logger.info(f"Loaded order data from file {i}: {len(df)} records")
        except Exception as e:
            logger.warning(f"Could not load order data file {i}: {e}")

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined order data: {len(combined_df)} total records")
        return combined_df
    else:
        logger.warning("No order data files could be loaded")
        return pd.DataFrame()

# Data loading functions to staging
def load_to_staging():
    """Load all data to staging tables"""
    engine = get_db_engine()

    try:
        # Load users
        users_df = ingest_customer_users()
        if not users_df.empty:
            users_df.to_sql('stg_users', engine, schema='staging', if_exists='replace', index=False)
            logger.info("Loaded users to staging")

        # Load products
        products_df = ingest_business_product_list()
        if not products_df.empty:
            products_df.to_sql('stg_products', engine, schema='staging', if_exists='replace', index=False)
            logger.info("Loaded products to staging")

        # Load campaigns
        campaigns_df = pd.concat([
            ingest_marketing_campaigns(),
            ingest_marketing_transactional_campaigns()
        ], ignore_index=True)
        if not campaigns_df.empty:
            campaigns_df.to_sql('stg_campaigns', engine, schema='staging', if_exists='replace', index=False)
            logger.info("Loaded campaigns to staging")

        # Load staff
        staff_df = ingest_enterprise_staff()
        if not staff_df.empty:
            # Process staff data (assuming HTML table has appropriate columns)
            staff_df.to_sql('stg_staff', engine, schema='staging', if_exists='replace', index=False)
            logger.info("Loaded staff to staging")

        # Load orders
        orders_df = ingest_order_data()
        if not orders_df.empty:
            orders_df.to_sql('stg_orders', engine, schema='staging', if_exists='replace', index=False)
            logger.info("Loaded orders to staging")

        logger.info("Data loading to staging completed successfully")

    except Exception as e:
        logger.error(f"Error loading data to staging: {e}")
        raise

if __name__ == "__main__":
    load_to_staging()
