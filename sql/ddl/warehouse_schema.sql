-- Dimension Tables

CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT UNIQUE NOT NULL,
    name VARCHAR(255),
    job_title VARCHAR(100),
    job_level VARCHAR(50),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INT UNIQUE NOT NULL,
    product_name VARCHAR(255),
    product_type VARCHAR(100),
    price DECIMAL(10,2),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_merchant (
    merchant_key SERIAL PRIMARY KEY,
    merchant_id INT UNIQUE NOT NULL,
    merchant_name VARCHAR(255),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_staff (
    staff_key SERIAL PRIMARY KEY,
    staff_id INT UNIQUE NOT NULL,
    staff_name VARCHAR(255),
    role VARCHAR(100),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_campaign (
    campaign_key SERIAL PRIMARY KEY,
    campaign_id INT UNIQUE NOT NULL,
    campaign_name VARCHAR(255),
    campaign_description TEXT,
    discount VARCHAR(50),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    week INT,
    day INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Fact Tables

CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    customer_key INT REFERENCES warehouse.dim_customer(customer_key),
    product_key INT REFERENCES warehouse.dim_product(product_key),
    merchant_key INT REFERENCES warehouse.dim_merchant(merchant_key),
    staff_key INT REFERENCES warehouse.dim_staff(staff_key),
    order_date_key INT REFERENCES warehouse.dim_date(date_key),
    quantity INT,
    price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    net_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.fact_operations (
    operations_key SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    date_key INT REFERENCES warehouse.dim_date(date_key),
    delivery_status VARCHAR(50),
    logistics_provider VARCHAR(100),
    warehouse VARCHAR(100),
    processing_time INT,
    on_time_delivery BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.fact_campaign_performance (
    campaign_perf_key SERIAL PRIMARY KEY,
    campaign_key INT REFERENCES warehouse.dim_campaign(campaign_key),
    order_id VARCHAR(100),  -- Changed to VARCHAR to match fact_orders
    transaction_date_key INT REFERENCES warehouse.dim_date(date_key),
    availed BOOLEAN,
    estimated_arrival DATE,
    conversion_flag BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_sales_order_date ON warehouse.fact_sales(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON warehouse.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_campaign_perf_campaign ON warehouse.fact_campaign_performance(campaign_key);

