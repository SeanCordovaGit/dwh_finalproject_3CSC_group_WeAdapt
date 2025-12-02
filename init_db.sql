-- Database initialization script for ShopZada DWH

-- Create schemas
\i sql/staging_schema.sql
\i sql/warehouse_schema.sql

-- Create presentation schema
CREATE SCHEMA IF NOT EXISTS presentation;

-- Create presentation views
\i sql/presentation_views.sql

-- Insert unknown dimension records for handling missing data
INSERT INTO warehouse.dim_customer (user_id, name, job_title, job_level, effective_date, expiry_date, is_current)
VALUES ('UNKNOWN', 'Unknown Customer', 'Unknown', 'Unknown', CURRENT_DATE, '9999-12-31', TRUE)
ON CONFLICT (user_id) DO NOTHING;

INSERT INTO warehouse.dim_product (product_id, product_name, product_type, product_category, price, effective_date, expiry_date, is_current)
VALUES ('UNKNOWN', 'Unknown Product', 'Unknown', 'Unknown', 0.0, CURRENT_DATE, '9999-12-31', TRUE)
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO warehouse.dim_staff (staff_id, name, department, position, hire_date, effective_date, expiry_date, is_current)
VALUES ('UNKNOWN', 'Unknown Staff', 'Unknown', 'Unknown', CURRENT_DATE, CURRENT_DATE, '9999-12-31', TRUE)
ON CONFLICT (staff_id) DO NOTHING;

-- Create indexes for better performance (if not already created)
CREATE INDEX IF NOT EXISTS idx_fact_sales_time ON warehouse.fact_sales(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON warehouse.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON warehouse.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_campaign ON warehouse.fact_sales(campaign_key);
CREATE INDEX IF NOT EXISTS idx_dim_time_date ON warehouse.dim_time(date_actual);
CREATE INDEX IF NOT EXISTS idx_dim_customer_user_id ON warehouse.dim_customer(user_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_product_id ON warehouse.dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_campaign_campaign_id ON warehouse.dim_campaign(campaign_id);
