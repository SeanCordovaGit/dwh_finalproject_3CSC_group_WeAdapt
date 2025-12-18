-- ============================================================
-- LIGHTNING-FAST FACT ORDERS LOADING
-- Simplified approach: avoid expensive subqueries
-- ============================================================

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

-- STEP 2: TRUNCATE FOR FRESH LOAD
TRUNCATE TABLE warehouse.fact_orders;

-- STEP 3: SINGLE FAST QUERY WITH ALL JOINS
INSERT INTO warehouse.fact_orders (
    order_id, customer_key, product_key, merchant_key, staff_key, campaign_key,
    order_date_key, estimated_arrival_key, quantity, unit_price, gross_amount,
    discount_amount, net_amount, availed, delivery_status, logistics_provider,
    processing_time_hours, on_time_delivery, updated_at
)
SELECT
    oh.order_id,

    -- Fast customer lookup
    dc.customer_key,

    -- Fast product lookup (simplified - take any product for order)
    COALESCE(dp.product_key, NULL),

    -- Merchant and staff from enterprise orders
    dm.merchant_key,
    ds.staff_key,

    -- Campaign lookup
    camp.campaign_key,

    -- Date keys
    od.date_key AS order_date_key,
    ea.date_key AS estimated_arrival_key,

    -- Simplified quantity/price (can be enhanced later)
    COALESCE(agg.total_quantity, 1) AS quantity,
    COALESCE(agg.avg_unit_price, 0) AS unit_price,
    COALESCE(agg.total_quantity * agg.avg_unit_price, 0) AS gross_amount,
    0::NUMERIC AS discount_amount,
    COALESCE(agg.total_quantity * agg.avg_unit_price, 0) AS net_amount,

    -- Campaign availed
    mt.availed::BOOLEAN AS availed,

    -- Delivery status
    CASE
        WHEN COALESCE(dd.delay_in_days, 0) = 0 THEN 'Delivered'
        WHEN COALESCE(dd.delay_in_days, 0) > 0 THEN 'Delayed'
        ELSE 'Unknown'
    END AS delivery_status,

    'Standard' AS logistics_provider,

    -- Processing time
    CASE
        WHEN COALESCE(dd.delay_in_days, 0) = 0 THEN 24
        ELSE 24 + (COALESCE(dd.delay_in_days, 0) * 24)
    END AS processing_time_hours,

    -- On-time delivery
    CASE
        WHEN (CASE WHEN COALESCE(dd.delay_in_days, 0) = 0 THEN 24
                   ELSE 24 + (COALESCE(dd.delay_in_days, 0) * 24) END) <= 48
             AND (CASE WHEN COALESCE(dd.delay_in_days, 0) = 0 THEN 'Delivered'
                       WHEN COALESCE(dd.delay_in_days, 0) > 0 THEN 'Delayed'
                       ELSE 'Unknown' END) = 'Delivered' THEN TRUE
        ELSE FALSE
    END AS on_time_delivery,

    CURRENT_TIMESTAMP

FROM staging.staging_operations_order_headers oh

-- Pre-aggregated line items (fast join)
LEFT JOIN (
    SELECT
        order_id,
        SUM(CAST(NULLIF(REGEXP_REPLACE(quantity, '[^0-9]', '', 'g'), '') AS INT)) AS total_quantity,
        AVG(price) AS avg_unit_price
    FROM staging.staging_operations_line_items_prices
    GROUP BY order_id
) agg ON agg.order_id = oh.order_id

-- Enterprise data
LEFT JOIN staging.staging_enterprise_orders eo ON eo.order_id = oh.order_id

-- Marketing data
LEFT JOIN staging.staging_marketing_transactions mt ON mt.order_id = oh.order_id

-- Delivery delays
LEFT JOIN staging.staging_operations_delivery_delays dd ON dd.order_id = oh.order_id

-- One product per order (simplified)
LEFT JOIN (
    SELECT DISTINCT ON (order_id)
        order_id,
        CAST(SUBSTRING(product_id FROM 'PRODUCT([0-9]+)') AS INT) AS product_id_num
    FROM staging.staging_operations_line_items_products
    ORDER BY order_id, product_id  -- Take first product
) prod ON prod.order_id = oh.order_id

-- DIMENSION LOOKUPS (fast integer joins)
LEFT JOIN warehouse.dim_customer dc ON dc.customer_id =
    CASE WHEN oh.user_id ~ '^USER[0-9]+$' THEN
        CAST(SUBSTRING(oh.user_id FROM 'USER([0-9]+)') AS INT)
    END AND dc.is_current

LEFT JOIN warehouse.dim_product dp ON dp.product_id = prod.product_id_num AND dp.is_current

LEFT JOIN warehouse.dim_merchant dm ON dm.merchant_id =
    CASE WHEN eo.merchant_id ~ '^MERCHANT[0-9]+$' THEN
        CAST(SUBSTRING(eo.merchant_id FROM 'MERCHANT([0-9]+)') AS INT)
    END AND dm.is_current

LEFT JOIN warehouse.dim_staff ds ON ds.staff_id =
    CASE WHEN eo.staff_id ~ '^STAFF[0-9]+$' THEN
        CAST(SUBSTRING(eo.staff_id FROM 'STAFF([0-9]+)') AS INT)
    END AND ds.is_current

LEFT JOIN warehouse.dim_campaign camp ON camp.campaign_id =
    CASE
        WHEN mt.campaign_id ~ '^[0-9]+$' THEN CAST(mt.campaign_id AS INT)
        WHEN mt.campaign_id ~ '([0-9]+)' THEN CAST((REGEXP_MATCH(mt.campaign_id, '([0-9]+)'))[1] AS INT)
        ELSE NULL
    END AND camp.is_current

-- Date lookups
LEFT JOIN warehouse.dim_date od ON od.full_date = oh.transaction_date

LEFT JOIN warehouse.dim_date ea ON ea.full_date =
    CASE
        WHEN oh.estimated_arrival ~ '^[0-9]+days$' THEN
            oh.transaction_date + (SUBSTRING(oh.estimated_arrival FROM '([0-9]+)') || ' days')::INTERVAL
        WHEN oh.estimated_arrival ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
            oh.estimated_arrival::DATE
        ELSE NULL
    END

WHERE oh.transaction_date IS NOT NULL;  -- Only valid orders

-- STEP 4: CREATE INDEXES
CREATE INDEX IF NOT EXISTS idx_fact_orders_order_date ON warehouse.fact_orders(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON warehouse.fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product ON warehouse.fact_orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_merchant ON warehouse.fact_orders(merchant_key);

-- STEP 5: ANALYZE
ANALYZE warehouse.fact_orders;

-- STEP 6: REPORT SUCCESS
SELECT
    '🚀 LIGHTNING LOAD COMPLETE!' as status,
    COUNT(*) as total_orders_loaded,
    COUNT(DISTINCT order_date_key) as distinct_dates,
    COUNT(DISTINCT customer_key) as distinct_customers,
    COUNT(DISTINCT merchant_key) as distinct_merchants,
    'Fast!' as performance_rating
FROM warehouse.fact_orders;

