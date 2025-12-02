-- Presentation Layer Views for ShopZada DWH
-- Business intelligence and analytical views

-- Business Question 1: What kinds of campaigns drive the highest order volume?

CREATE OR REPLACE VIEW presentation.vw_campaign_order_volume AS
SELECT
    dc.campaign_name,
    dc.discount_percentage,
    COUNT(fs.sales_key) as total_orders,
    SUM(fs.quantity) as total_quantity_sold,
    SUM(fs.total_amount) as total_revenue,
    AVG(fs.total_amount) as avg_order_value
FROM warehouse.fact_sales fs
LEFT JOIN warehouse.dim_campaign dc ON fs.campaign_key = dc.campaign_key
WHERE fs.campaign_key IS NOT NULL
GROUP BY dc.campaign_name, dc.discount_percentage
ORDER BY total_orders DESC;

-- Business Question 2: How do merchant performance metrics affect sales?

CREATE OR REPLACE VIEW presentation.vw_merchant_performance AS
SELECT
    ds.name as merchant_name,
    ds.department,
    COUNT(fs.sales_key) as total_orders,
    SUM(fs.quantity) as total_items_sold,
    SUM(fs.total_amount) as total_revenue,
    AVG(fs.total_amount) as avg_order_value,
    COUNT(DISTINCT dc.customer_key) as unique_customers
FROM warehouse.fact_sales fs
JOIN warehouse.dim_staff ds ON fs.staff_key = ds.staff_key
LEFT JOIN warehouse.dim_customer dc ON fs.customer_key = dc.customer_key
GROUP BY ds.name, ds.department
ORDER BY total_revenue DESC;

-- Business Question 3: What customer segments contribute most to revenue?

CREATE OR REPLACE VIEW presentation.vw_customer_segment_revenue AS
SELECT
    dc.job_level,
    dc.job_title,
    COUNT(fs.sales_key) as total_orders,
    SUM(fs.quantity) as total_items_purchased,
    SUM(fs.total_amount) as total_revenue,
    AVG(fs.total_amount) as avg_order_value,
    COUNT(DISTINCT dc.customer_key) as unique_customers
FROM warehouse.fact_sales fs
JOIN warehouse.dim_customer dc ON fs.customer_key = dc.customer_key
GROUP BY dc.job_level, dc.job_title
ORDER BY total_revenue DESC;

-- Product Performance View
CREATE OR REPLACE VIEW presentation.vw_product_performance AS
SELECT
    dp.product_name,
    dp.product_type,
    dp.product_category,
    dp.price,
    COUNT(fs.sales_key) as total_orders,
    SUM(fs.quantity) as total_quantity_sold,
    SUM(fs.total_amount) as total_revenue,
    AVG(fs.unit_price) as avg_selling_price
FROM warehouse.fact_sales fs
JOIN warehouse.dim_product dp ON fs.product_key = dp.product_key
GROUP BY dp.product_name, dp.product_type, dp.product_category, dp.price
ORDER BY total_revenue DESC;

-- Time-based Sales Analysis
CREATE OR REPLACE VIEW presentation.vw_sales_by_time AS
SELECT
    dt.year_actual,
    dt.month_name,
    dt.quarter_actual,
    COUNT(fs.sales_key) as total_orders,
    SUM(fs.quantity) as total_quantity_sold,
    SUM(fs.total_amount) as total_revenue,
    COUNT(DISTINCT fs.customer_key) as unique_customers
FROM warehouse.fact_sales fs
JOIN warehouse.dim_time dt ON fs.time_key = dt.time_key
GROUP BY dt.year_actual, dt.month_name, dt.quarter_actual
ORDER BY dt.year_actual, dt.quarter_actual;

-- Campaign Performance Overview
CREATE OR REPLACE VIEW presentation.vw_campaign_overview AS
SELECT
    dc.campaign_name,
    dc.discount_percentage,
    fcp.impressions,
    fcp.clicks,
    fcp.conversions,
    fcp.revenue,
    fcp.cost,
    CASE
        WHEN fcp.impressions > 0 THEN (fcp.clicks::DECIMAL / fcp.impressions) * 100
        ELSE 0
    END as ctr_percentage,
    CASE
        WHEN fcp.cost > 0 THEN (fcp.revenue - fcp.cost) / fcp.cost
        ELSE 0
    END as roi
FROM warehouse.fact_campaign_performance fcp
JOIN warehouse.dim_campaign dc ON fcp.campaign_key = dc.campaign_key
ORDER BY fcp.revenue DESC;

-- Customer Lifetime Value
CREATE OR REPLACE VIEW presentation.vw_customer_lifetime_value AS
SELECT
    dc.name,
    dc.user_id,
    dc.job_level,
    MIN(fs.order_date) as first_order_date,
    MAX(fs.order_date) as last_order_date,
    COUNT(fs.sales_key) as total_orders,
    SUM(fs.total_amount) as total_revenue,
    AVG(fs.total_amount) as avg_order_value,
    SUM(fs.quantity) as total_items_purchased
FROM warehouse.fact_sales fs
JOIN warehouse.dim_customer dc ON fs.customer_key = dc.customer_key
GROUP BY dc.name, dc.user_id, dc.job_level
ORDER BY total_revenue DESC;

-- Daily Sales Summary
CREATE OR REPLACE VIEW presentation.vw_daily_sales_summary AS
SELECT
    dt.date_actual,
    dt.day_name,
    dt.is_weekend,
    COUNT(fs.sales_key) as orders_count,
    SUM(fs.quantity) as items_sold,
    SUM(fs.total_amount) as daily_revenue,
    COUNT(DISTINCT fs.customer_key) as unique_customers
FROM warehouse.fact_sales fs
JOIN warehouse.dim_time dt ON fs.time_key = dt.time_key
GROUP BY dt.date_actual, dt.day_name, dt.is_weekend
ORDER BY dt.date_actual;
