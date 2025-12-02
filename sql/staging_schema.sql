-- Staging Layer Schema for ShopZada DWH
-- This schema holds raw data from various sources

CREATE SCHEMA IF NOT EXISTS staging;

-- Staging table for customer user data
CREATE TABLE IF NOT EXISTS staging.stg_users (
    user_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    job_title VARCHAR(255),
    job_level VARCHAR(100)
);

-- Staging table for products
CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(500),
    product_type VARCHAR(100),
    price DECIMAL(10,2)
);

-- Staging table for campaigns
CREATE TABLE IF NOT EXISTS staging.stg_campaigns (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(500),
    campaign_description TEXT,
    discount VARCHAR(50)
);

-- Staging table for order data (from parquet files)
CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id VARCHAR(50),
    merchant_id VARCHAR(50),
    user_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INTEGER,
    price DECIMAL(10,2),
    order_date TIMESTAMP,
    delivery_date TIMESTAMP,
    status VARCHAR(50)
);

-- Staging table for staff data
CREATE TABLE IF NOT EXISTS staging.stg_staff (
    staff_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    department VARCHAR(100),
    position VARCHAR(100),
    hire_date DATE
);
