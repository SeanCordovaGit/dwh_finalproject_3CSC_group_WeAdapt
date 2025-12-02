-- Warehouse Layer Schema for ShopZada DWH
-- Star Schema Design (Kimball Methodology)

CREATE SCHEMA IF NOT EXISTS warehouse;

-- Dimension Tables

-- Time Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_time (
    time_key SERIAL PRIMARY KEY,
    date_actual DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    month_actual INTEGER,
    month_name VARCHAR(10),
    quarter_actual INTEGER,
    year_actual INTEGER,
    is_weekend BOOLEAN
);

-- Customer/User Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    user_id VARCHAR(50) UNIQUE,
    name VARCHAR(255),
    job_title VARCHAR(255),
    job_level VARCHAR(100),
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);

-- Product Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE,
    product_name VARCHAR(500),
    product_type VARCHAR(100),
    product_category VARCHAR(100),
    price DECIMAL(10,2),
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);

-- Campaign Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_campaign (
    campaign_key SERIAL PRIMARY KEY,
    campaign_id VARCHAR(50) UNIQUE,
    campaign_name VARCHAR(500),
    campaign_description TEXT,
    discount_percentage DECIMAL(5,2),
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);

-- Staff Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_staff (
    staff_key SERIAL PRIMARY KEY,
    staff_id VARCHAR(50) UNIQUE,
    name VARCHAR(255),
    department VARCHAR(100),
    position VARCHAR(100),
    hire_date DATE,
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);

-- Fact Tables

-- Sales Fact Table
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    time_key INTEGER REFERENCES warehouse.dim_time(time_key),
    customer_key INTEGER REFERENCES warehouse.dim_customer(customer_key),
    product_key INTEGER REFERENCES warehouse.dim_product(product_key),
    campaign_key INTEGER REFERENCES warehouse.dim_campaign(campaign_key),
    staff_key INTEGER REFERENCES warehouse.dim_staff(staff_key),
    order_id VARCHAR(50),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    order_date TIMESTAMP,
    delivery_date TIMESTAMP,
    order_status VARCHAR(50)
);

-- Campaign Performance Fact Table
CREATE TABLE IF NOT EXISTS warehouse.fact_campaign_performance (
    campaign_perf_key SERIAL PRIMARY KEY,
    time_key INTEGER REFERENCES warehouse.dim_time(time_key),
    campaign_key INTEGER REFERENCES warehouse.dim_campaign(campaign_key),
    impressions INTEGER,
    clicks INTEGER,
    conversions INTEGER,
    revenue DECIMAL(10,2),
    cost DECIMAL(10,2)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_fact_sales_time ON warehouse.fact_sales(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON warehouse.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON warehouse.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_campaign ON warehouse.fact_sales(campaign_key);
CREATE INDEX IF NOT EXISTS idx_dim_time_date ON warehouse.dim_time(date_actual);
CREATE INDEX IF NOT EXISTS idx_dim_customer_user_id ON warehouse.dim_customer(user_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_product_id ON warehouse.dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_campaign_campaign_id ON warehouse.dim_campaign(campaign_id);
