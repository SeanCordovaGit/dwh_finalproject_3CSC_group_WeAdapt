-- ============================================================
-- OPTIMIZED FACT ORDERS LOADING (FIXED PERFORMANCE ISSUES)
-- Eliminates correlated subqueries and optimizes all steps
-- ============================================================

-- STEP 0: CREATE INDEXES ON STAGING TABLES
CREATE INDEX IF NOT EXISTS idx_staging_order_headers_order_id ON staging.staging_operations_order_headers(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_order_headers_user_id ON staging.staging_operations_order_headers(user_id);
CREATE INDEX IF NOT EXISTS idx_staging_products_order_id ON staging.staging_operations_line_items_products(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_prices_order_id ON staging.staging_operations_line_items_prices(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_enterprise_orders_order_id ON staging.staging_enterprise_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_marketing_order_id ON staging.staging_marketing_transactions(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_delays_order_id ON staging.staging_operations_delivery_delays(order_id);

-- STEP 0.5: CREATE INDEXES ON DIMENSION TABLES (CRITICAL!)
CREATE INDEX IF NOT EXISTS idx_dim_customer_id_current ON warehouse.dim_customer(customer_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_product_id_current ON warehouse.dim_product(product_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_merchant_id_current ON warehouse.dim_merchant(merchant_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_staff_id_current ON warehouse.dim_staff(staff_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_campaign_id_current ON warehouse.dim_campaign(campaign_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON warehouse.dim_date(full_date);

-- STEP 1: CREATE FACT TABLE IF NOT EXISTS
CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_id                VARCHAR(100) PRIMARY KEY,
    customer_key            INT,
    product_key             INT,
    merchant_key            INT,
    staff_key               INT,
    campaign_key            INT,
    order_date_key          INT,
    estimated_arrival_key   INT,
    quantity                INT,
    unit_price              NUMERIC(12,2),
    gross_amount            NUMERIC(14,2),
    discount_amount         NUMERIC(14,2),
    net_amount              NUMERIC(14,2),
    availed                 BOOLEAN,
    delivery_status         VARCHAR(50),
    logistics_provider      VARCHAR(100),
    processing_time_hours   INT,
    on_time_delivery        BOOLEAN,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STEP 2: TRUNCATE FACT TABLE
TRUNCATE TABLE warehouse.fact_orders;

-- ============================================================
-- STEP 3: PRE-AGGREGATE DATA IN SEPARATE TEMP TABLES
-- This eliminates correlated subqueries and inline aggregations
-- ============================================================

-- 3A: Get first product per order (REPLACES CORRELATED SUBQUERY)
DROP TABLE IF EXISTS temp_first_products;
CREATE TEMP TABLE temp_first_products AS
SELECT DISTINCT ON (order_id)
    order_id,
    CAST(SUBSTRING(product_id FROM 'PRODUCT([0-9]+)') AS INT) AS product_id
FROM staging.staging_operations_line_items_products
WHERE product_id ~ '^PRODUCT[0-9]+$'  -- Only valid product IDs
ORDER BY order_id, product_id;

CREATE INDEX idx_temp_products ON temp_first_products(order_id);

-- 3B: Pre-aggregate line items (REPLACES INLINE SUBQUERY)
DROP TABLE IF EXISTS temp_line_aggregates;
CREATE TEMP TABLE temp_line_aggregates AS
SELECT
    order_id,
    SUM(CAST(NULLIF(REGEXP_REPLACE(quantity, '[^0-9]', '', 'g'), '') AS INT)) AS total_quantity,
    AVG(price) AS avg_unit_price
FROM staging.staging_operations_line_items_prices
WHERE quantity IS NOT NULL AND price IS NOT NULL
GROUP BY order_id;

CREATE INDEX idx_temp_line_agg ON temp_line_aggregates(order_id);

-- ============================================================
-- STEP 4: CREATE MAIN TEMP TABLE WITH SIMPLE JOINS
-- All complex operations are pre-calculated
-- ============================================================

DROP TABLE IF EXISTS temp_orders_prepared;
CREATE TEMP TABLE temp_orders_prepared AS
SELECT
    oh.order_id,
    oh.transaction_date,
    oh.user_id,
    oh.estimated_arrival,

    -- Extract customer ID
    CASE WHEN oh.user_id ~ '^USER[0-9]+$' THEN
        CAST(SUBSTRING(oh.user_id FROM 'USER([0-9]+)') AS INT)
    END AS customer_id,

    -- Product from pre-aggregated temp table (NO SUBQUERY!)
    tfp.product_id,

    -- Merchant/Staff IDs
    CASE WHEN eo.merchant_id ~ '^MERCHANT[0-9]+$' THEN
        CAST(SUBSTRING(eo.merchant_id FROM 'MERCHANT([0-9]+)') AS INT)
    END AS merchant_id,
    
    CASE WHEN eo.staff_id ~ '^STAFF[0-9]+$' THEN
        CAST(SUBSTRING(eo.staff_id FROM 'STAFF([0-9]+)') AS INT)
    END AS staff_id,

    -- Campaign ID extraction
    CASE
        WHEN mt.campaign_id ~ '^[0-9]+$' THEN CAST(mt.campaign_id AS INT)
        WHEN mt.campaign_id ~ '([0-9]+)' THEN CAST((REGEXP_MATCH(mt.campaign_id, '([0-9]+)'))[1] AS INT)
    END AS campaign_id,

    mt.availed::BOOLEAN AS availed,

    -- Aggregated quantities from pre-calculated temp table
    COALESCE(tla.total_quantity, 1) AS quantity,
    COALESCE(tla.avg_unit_price, 0) AS unit_price,

    -- Delivery info
    COALESCE(dd.delay_in_days, 0) AS delay_in_days,

    -- Calculate delivery status
    CASE
        WHEN COALESCE(dd.delay_in_days, 0) = 0 THEN 'Delivered'
        WHEN dd.delay_in_days > 0 THEN 'Delayed'
        ELSE 'Unknown'
    END AS delivery_status,

    -- Calculate processing time
    CASE
        WHEN COALESCE(dd.delay_in_days, 0) = 0 THEN 24
        ELSE 24 + (dd.delay_in_days * 24)
    END AS processing_time_hours,

    -- Process estimated arrival date
    CASE
        WHEN oh.estimated_arrival ~ '^[0-9]+days$' THEN
            oh.transaction_date + (SUBSTRING(oh.estimated_arrival FROM '([0-9]+)') || ' days')::INTERVAL
        WHEN oh.estimated_arrival ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
            oh.estimated_arrival::DATE
    END AS estimated_arrival_date

FROM staging.staging_operations_order_headers oh
LEFT JOIN temp_first_products tfp ON tfp.order_id = oh.order_id
LEFT JOIN staging.staging_enterprise_orders eo ON eo.order_id = oh.order_id
LEFT JOIN staging.staging_marketing_transactions mt ON mt.order_id = oh.order_id
LEFT JOIN staging.staging_operations_delivery_delays dd ON dd.order_id = oh.order_id
LEFT JOIN temp_line_aggregates tla ON tla.order_id = oh.order_id
WHERE oh.transaction_date IS NOT NULL;  -- Only valid orders

-- Optional: Create index if temp table is large
-- CREATE INDEX idx_temp_orders_prep ON temp_orders_prepared(customer_id, product_id, merchant_id, staff_id, campaign_id);

-- ============================================================
-- STEP 5: INSERT INTO FACT TABLE WITH DIMENSION LOOKUPS
-- All heavy lifting is done - this should be fast
-- ============================================================

INSERT INTO warehouse.fact_orders (
    order_id, customer_key, product_key, merchant_key, staff_key, campaign_key,
    order_date_key, estimated_arrival_key, quantity, unit_price, gross_amount,
    discount_amount, net_amount, availed, delivery_status, logistics_provider,
    processing_time_hours, on_time_delivery, updated_at
)
SELECT
    top.order_id,
    dc.customer_key,
    dp.product_key,
    dm.merchant_key,
    ds.staff_key,
    camp.campaign_key,
    od.date_key AS order_date_key,
    ea.date_key AS estimated_arrival_key,
    top.quantity,
    top.unit_price,
    top.quantity * top.unit_price AS gross_amount,
    0::NUMERIC AS discount_amount,
    top.quantity * top.unit_price AS net_amount,
    top.availed,
    top.delivery_status,
    'Standard' AS logistics_provider,
    top.processing_time_hours,
    CASE
        WHEN top.processing_time_hours <= 48 AND top.delivery_status = 'Delivered' THEN TRUE
        ELSE FALSE
    END AS on_time_delivery,
    CURRENT_TIMESTAMP
FROM temp_orders_prepared top
LEFT JOIN warehouse.dim_customer dc ON dc.customer_id = top.customer_id AND dc.is_current
LEFT JOIN warehouse.dim_product dp ON dp.product_id = top.product_id AND dp.is_current
LEFT JOIN warehouse.dim_merchant dm ON dm.merchant_id = top.merchant_id AND dm.is_current
LEFT JOIN warehouse.dim_staff ds ON ds.staff_id = top.staff_id AND ds.is_current
LEFT JOIN warehouse.dim_campaign camp ON camp.campaign_id = top.campaign_id AND camp.is_current
LEFT JOIN warehouse.dim_date od ON od.full_date = top.transaction_date
LEFT JOIN warehouse.dim_date ea ON ea.full_date = top.estimated_arrival_date;

-- ============================================================
-- STEP 6: CLEANUP TEMP TABLES
-- ============================================================

DROP TABLE IF EXISTS temp_first_products;
DROP TABLE IF EXISTS temp_line_aggregates;
DROP TABLE IF EXISTS temp_orders_prepared;

-- ============================================================
-- STEP 7: CREATE INDEXES ON FACT TABLE
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_fact_orders_order_date ON warehouse.fact_orders(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON warehouse.fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product ON warehouse.fact_orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_merchant ON warehouse.fact_orders(merchant_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_campaign ON warehouse.fact_orders(campaign_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_delivery_status ON warehouse.fact_orders(delivery_status);

-- ============================================================
-- STEP 8: ANALYZE FOR STATISTICS
-- ============================================================

ANALYZE warehouse.fact_orders;

-- ============================================================
-- STEP 9: REPORT SUCCESS
-- ============================================================

SELECT
    'Fact Orders Load Complete' as status,
    COUNT(*) as total_orders,
    COUNT(DISTINCT order_date_key) as distinct_dates,
    COUNT(DISTINCT customer_key) as distinct_customers,
    COUNT(CASE WHEN on_time_delivery THEN 1 END) as on_time_deliveries,
    ROUND(100.0 * COUNT(CASE WHEN on_time_delivery THEN 1 END) / NULLIF(COUNT(*), 0), 2) as on_time_percentage
FROM warehouse.fact_orders;
