-- Staging Layer - Raw data landing zone
-- Tables mirror source data exactly as ingested

-- Business Department
CREATE TABLE IF NOT EXISTS staging.staging_business_products (
    unnamed_0 INT,
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    product_type VARCHAR(100),
    price DECIMAL(10,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer Management Department
CREATE TABLE IF NOT EXISTS staging.staging_customer_cards (
    user_id VARCHAR(50),
    name VARCHAR(255),
    credit_card_number BIGINT,
    issuing_bank VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.staging_customer_profiles (
    user_id VARCHAR(50),
    registration_date TIMESTAMP,
    name VARCHAR(255),
    street VARCHAR(255),
    state VARCHAR(100),
    country VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.staging_customer_jobs (
    unnamed_0 INT,
    user_id VARCHAR(50),
    name VARCHAR(255),
    job_title VARCHAR(100),
    job_level VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enterprise Department
CREATE TABLE IF NOT EXISTS staging.staging_enterprise_merchants (
    unnamed_0 INT,
    merchant_id VARCHAR(50),
    creation_date TIMESTAMP,
    name VARCHAR(255),
    street VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    contact_number VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.staging_enterprise_staff (
    unnamed_0 INT,
    staff_id VARCHAR(50),
    name VARCHAR(255),
    job_level VARCHAR(100),
    street VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    contact_number VARCHAR(50),
    creation_date TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.staging_enterprise_orders (
    unnamed_0 INT,
    order_id VARCHAR(100),
    merchant_id VARCHAR(50),
    staff_id VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Marketing Department
CREATE TABLE IF NOT EXISTS staging.staging_marketing_campaigns (
    campaign_id VARCHAR(50),
    campaign_name VARCHAR(255),
    campaign_description TEXT,
    discount VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.staging_marketing_transactions (
    unnamed_0 INT,
    transaction_date DATE,
    campaign_id VARCHAR(50),
    order_id VARCHAR(100),
    estimated_arrival VARCHAR(50),
    availed INT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Operations Department
CREATE TABLE IF NOT EXISTS staging.staging_operations_line_items_prices (
    unnamed_0 INT,
    order_id VARCHAR(100),
    price DECIMAL(10,2),
    quantity VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.staging_operations_line_items_products (
    unnamed_0 INT,
    order_id VARCHAR(100),
    product_name VARCHAR(255),
    product_id VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.staging_operations_order_headers (
    order_id VARCHAR(100),
    user_id VARCHAR(50),
    estimated_arrival VARCHAR(50),
    transaction_date DATE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.staging_operations_delivery_delays (
    unnamed_0 INT,
    order_id VARCHAR(100),
    delay_in_days INT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality Issues table
CREATE TABLE IF NOT EXISTS staging.data_quality_issues (
    table_name VARCHAR(100),
    issue_type VARCHAR(100),
    issue_description TEXT,
    affected_rows INT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

