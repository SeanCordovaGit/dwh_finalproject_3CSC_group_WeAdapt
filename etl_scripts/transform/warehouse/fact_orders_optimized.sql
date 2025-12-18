-- ============================================================
-- OPTIMIZED FACT ORDERS LOADING SCRIPT
-- Fixes: FK constraints, regex joins, indexing, incremental loading
-- ============================================================

-- STEP 1: CREATE INDEXES ON STAGING TABLES (RUN ONCE)
-- These indexes are critical for performance
CREATE INDEX IF NOT EXISTS idx_staging_order_headers_order_id ON staging.staging_operations_order_headers(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_order_headers_user_id ON staging.staging_operations_order_headers(user_id);
CREATE INDEX IF NOT EXISTS idx_staging_products_order_id ON staging.staging_operations_line_items_products(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_prices_order_id ON staging.staging_operations_line_items_prices(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_enterprise_orders_order_id ON staging.staging_enterprise_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_marketing_order_id ON staging.staging_marketing_transactions(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_delays_order_id ON staging.staging_operations_delivery_delays(order_id);

-- STEP 2: CREATE FACT TABLE WITHOUT FK CONSTRAINTS
-- Foreign keys on fact tables are an anti-pattern in data warehousing
-- NOTE: If table already exists with FKs, you'll need to drop and recreate it
-- For production, consider: ALTER TABLE warehouse.fact_orders DROP CONSTRAINT constraint_name;
DROP TABLE IF EXISTS warehouse.fact_orders_new;
CREATE TABLE warehouse.fact_orders_new (
    order_id                VARCHAR(100) PRIMARY KEY,
    customer_key            INT,  -- No FK constraint
    product_key             INT,  -- No FK constraint
    merchant_key            INT,  -- No FK constraint
    staff_key               INT,  -- No FK constraint
    campaign_key            INT,  -- No FK constraint
    order_date_key          INT,  -- No FK constraint
    estimated_arrival_key   INT,  -- No FK constraint
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

-- Rename tables (safe switch)
DROP TABLE IF EXISTS warehouse.fact_orders_old;
ALTER TABLE IF EXISTS warehouse.fact_orders RENAME TO fact_orders_old;
ALTER TABLE warehouse.fact_orders_new RENAME TO fact_orders;
DROP TABLE IF EXISTS warehouse.fact_orders_old;

-- STEP 3: PRE-COMPUTE DIMENSION KEYS (ELIMINATE REGEX IN JOINS)
-- This step extracts clean IDs once and stores them for fast joins
DROP TABLE IF EXISTS temp_dimension_keys;
CREATE TEMP TABLE temp_dimension_keys AS
SELECT
    order_id,
    -- Extract clean IDs using regex once, not in joins
    CASE
        WHEN user_id ~ '^USER[0-9]+$'
        THEN CAST(SUBSTRING(user_id FROM 'USER([0-9]+)') AS INT)
        ELSE NULL
    END AS customer_id_clean,

    CASE
        WHEN product_id ~ '^PRODUCT[0-9]+$'
        THEN CAST(SUBSTRING(product_id FROM 'PRODUCT([0-9]+)') AS INT)
        ELSE NULL
    END AS product_id_clean,

    CASE
        WHEN merchant_id ~ '^MERCHANT[0-9]+$'
        THEN CAST(SUBSTRING(merchant_id FROM 'MERCHANT([0-9]+)') AS INT)
        ELSE NULL
    END AS merchant_id_clean,

    CASE
        WHEN staff_id ~ '^STAFF[0-9]+$'
        THEN CAST(SUBSTRING(staff_id FROM 'STAFF([0-9]+)') AS INT)
        ELSE NULL
    END AS staff_id_clean,

    CASE
        WHEN campaign_id ~ '^[0-9]+$'
        THEN CAST(campaign_id AS INT)
        WHEN campaign_id ~ '([0-9]+)'
        THEN CAST((REGEXP_MATCH(campaign_id, '([0-9]+)'))[1] AS INT)
        ELSE NULL
    END AS campaign_id_clean,

    transaction_date,
    estimated_arrival
FROM (
    SELECT DISTINCT
        oh.order_id,
        oh.user_id,
        pi.product_id,
        eo.merchant_id,
        eo.staff_id,
        mt.campaign_id,
        oh.transaction_date,
        oh.estimated_arrival
    FROM staging.staging_operations_order_headers oh
    LEFT JOIN (
        SELECT order_id, MIN(product_id) AS product_id
        FROM staging.staging_operations_line_items_products
        GROUP BY order_id
    ) pi ON pi.order_id = oh.order_id
    LEFT JOIN staging.staging_enterprise_orders eo ON eo.order_id = oh.order_id
    LEFT JOIN staging.staging_marketing_transactions mt ON mt.order_id = oh.order_id
) source_data;

-- Create index on temp table for performance
CREATE INDEX idx_temp_keys_order_id ON temp_dimension_keys(order_id);

-- STEP 4: AGGREGATE LINE ITEMS SEPARATELY
DROP TABLE IF EXISTS temp_line_item_agg;
CREATE TEMP TABLE temp_line_item_agg AS
SELECT
    order_id,
    COALESCE(SUM(CAST(NULLIF(REGEXP_REPLACE(quantity, '[^0-9]', '', 'g'), '') AS INT)), 1) AS total_quantity,
    COALESCE(AVG(price), 0) AS avg_unit_price
FROM staging.staging_operations_line_items_prices
GROUP BY order_id;

CREATE INDEX idx_temp_agg_order_id ON temp_line_item_agg(order_id);

-- STEP 5: COMPUTE DELIVERY METRICS
DROP TABLE IF EXISTS temp_delivery_metrics;
CREATE TEMP TABLE temp_delivery_metrics AS
SELECT
    oh.order_id,
    oh.transaction_date,
    CASE
        WHEN oh.estimated_arrival ~ '^[0-9]+days$' THEN
            oh.transaction_date + (SUBSTRING(oh.estimated_arrival FROM '([0-9]+)') || ' days')::INTERVAL
        WHEN oh.estimated_arrival ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
            oh.estimated_arrival::DATE
        ELSE NULL
    END AS estimated_arrival_date,
    COALESCE(dd.delay_in_days, 0) AS delay_in_days,
    CASE
        WHEN COALESCE(dd.delay_in_days, 0) = 0 THEN 'Delivered'
        WHEN COALESCE(dd.delay_in_days, 0) > 0 THEN 'Delayed'
        ELSE 'Unknown'
    END AS delivery_status,
    CASE
        WHEN COALESCE(dd.delay_in_days, 0) = 0 THEN 24
        ELSE 24 + (COALESCE(dd.delay_in_days, 0) * 24)
    END AS processing_time_hours
FROM staging.staging_operations_order_headers oh
LEFT JOIN staging.staging_operations_delivery_delays dd ON dd.order_id = oh.order_id;

CREATE INDEX idx_temp_delivery_order_id ON temp_delivery_metrics(order_id);

-- STEP 6: RESOLVE ALL DIMENSION KEYS IN ONE PASS
DROP TABLE IF EXISTS temp_resolved_orders;
CREATE TEMP TABLE temp_resolved_orders AS
SELECT
    tdk.order_id,
    tdk.transaction_date,
    tdm.estimated_arrival_date,
    tdm.delivery_status,
    tdm.processing_time_hours,
    tla.total_quantity AS quantity,
    tla.avg_unit_price AS unit_price,
    tla.total_quantity * tla.avg_unit_price AS gross_amount,
    0::NUMERIC AS discount_amount,

    -- Resolve dimension keys with simple integer joins (fast!)
    dc.customer_key,
    dp.product_key,
    dm.merchant_key,
    ds.staff_key,
    camp.campaign_key,

    -- Date keys
    od.date_key AS order_date_key,
    ea.date_key AS estimated_arrival_key,

    -- Campaign availed flag
    mt.availed::BOOLEAN AS availed

FROM temp_dimension_keys tdk
LEFT JOIN temp_delivery_metrics tdm ON tdm.order_id = tdk.order_id
LEFT JOIN temp_line_item_agg tla ON tla.order_id = tdk.order_id
LEFT JOIN staging.staging_marketing_transactions mt ON mt.order_id = tdk.order_id

-- Fast integer joins to dimensions (no regex!)
LEFT JOIN warehouse.dim_customer dc ON dc.customer_id = tdk.customer_id_clean AND dc.is_current
LEFT JOIN warehouse.dim_product dp ON dp.product_id = tdk.product_id_clean AND dp.is_current
LEFT JOIN warehouse.dim_merchant dm ON dm.merchant_id = tdk.merchant_id_clean AND dm.is_current
LEFT JOIN warehouse.dim_staff ds ON ds.staff_id = tdk.staff_id_clean AND ds.is_current
LEFT JOIN warehouse.dim_campaign camp ON camp.campaign_id = tdk.campaign_id_clean AND camp.is_current

-- Date joins
LEFT JOIN warehouse.dim_date od ON od.full_date = tdk.transaction_date
LEFT JOIN warehouse.dim_date ea ON ea.full_date = tdm.estimated_arrival_date

WHERE od.date_key IS NOT NULL;  -- Only process orders with valid dates

-- STEP 7: INCREMENTAL UPSERT (only update changed records)
-- Use a more selective ON CONFLICT strategy
INSERT INTO warehouse.fact_orders (
    order_id, customer_key, product_key, merchant_key, staff_key, campaign_key,
    order_date_key, estimated_arrival_key, quantity, unit_price, gross_amount,
    discount_amount, net_amount, availed, delivery_status, logistics_provider,
    processing_time_hours, on_time_delivery, updated_at
)
SELECT
    tro.order_id,
    tro.customer_key,
    tro.product_key,
    tro.merchant_key,
    tro.staff_key,
    tro.campaign_key,
    tro.order_date_key,
    tro.estimated_arrival_key,
    tro.quantity,
    tro.unit_price,
    tro.gross_amount,
    tro.discount_amount,
    tro.gross_amount - tro.discount_amount AS net_amount,
    tro.availed,
    tro.delivery_status,
    'Standard' AS logistics_provider,
    tro.processing_time_hours,
    CASE
        WHEN tro.processing_time_hours <= 48 AND tro.delivery_status = 'Delivered' THEN TRUE
        ELSE FALSE
    END AS on_time_delivery,
    CURRENT_TIMESTAMP
FROM temp_resolved_orders tro
ON CONFLICT (order_id) DO UPDATE SET
    -- Only update fields that might have changed
    customer_key = EXCLUDED.customer_key,
    product_key = EXCLUDED.product_key,
    merchant_key = EXCLUDED.merchant_key,
    staff_key = EXCLUDED.staff_key,
    campaign_key = EXCLUDED.campaign_key,
    estimated_arrival_key = EXCLUDED.estimated_arrival_key,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    gross_amount = EXCLUDED.gross_amount,
    discount_amount = EXCLUDED.discount_amount,
    net_amount = EXCLUDED.net_amount,
    availed = EXCLUDED.availed,
    delivery_status = EXCLUDED.delivery_status,
    logistics_provider = EXCLUDED.logistics_provider,
    processing_time_hours = EXCLUDED.processing_time_hours,
    on_time_delivery = EXCLUDED.on_time_delivery,
    updated_at = CURRENT_TIMESTAMP
WHERE
    -- Only update if any significant field changed (prevents unnecessary updates)
    warehouse.fact_orders.customer_key IS DISTINCT FROM EXCLUDED.customer_key OR
    warehouse.fact_orders.product_key IS DISTINCT FROM EXCLUDED.product_key OR
    warehouse.fact_orders.merchant_key IS DISTINCT FROM EXCLUDED.merchant_key OR
    warehouse.fact_orders.staff_key IS DISTINCT FROM EXCLUDED.staff_key OR
    warehouse.fact_orders.campaign_key IS DISTINCT FROM EXCLUDED.campaign_key OR
    warehouse.fact_orders.delivery_status IS DISTINCT FROM EXCLUDED.delivery_status OR
    warehouse.fact_orders.quantity IS DISTINCT FROM EXCLUDED.quantity;

-- STEP 8: CLEANUP TEMP TABLES
DROP TABLE IF EXISTS temp_dimension_keys;
DROP TABLE IF EXISTS temp_line_item_agg;
DROP TABLE IF EXISTS temp_delivery_metrics;
DROP TABLE IF EXISTS temp_resolved_orders;

-- STEP 9: CREATE INDEXES ON FACT TABLE (for query performance)
CREATE INDEX IF NOT EXISTS idx_fact_orders_order_date ON warehouse.fact_orders(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON warehouse.fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product ON warehouse.fact_orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_merchant ON warehouse.fact_orders(merchant_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_updated ON warehouse.fact_orders(updated_at);

-- Optional: Analyze table for query optimizer
ANALYZE warehouse.fact_orders;

