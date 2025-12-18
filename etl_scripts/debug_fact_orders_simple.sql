-- ==========================================
-- SIMPLE DEBUG: Check data volumes first
-- ==========================================

-- Check row counts in staging tables
SELECT
    'staging_operations_order_headers' as table_name,
    COUNT(*) as row_count
FROM staging.staging_operations_order_headers

UNION ALL

SELECT
    'staging_operations_line_items_products' as table_name,
    COUNT(*) as row_count
FROM staging.staging_operations_line_items_products

UNION ALL

SELECT
    'staging_operations_line_items_prices' as table_name,
    COUNT(*) as row_count
FROM staging.staging_operations_line_items_prices

UNION ALL

SELECT
    'staging_enterprise_orders' as table_name,
    COUNT(*) as row_count
FROM staging.staging_enterprise_orders

UNION ALL

SELECT
    'staging_marketing_transactions' as table_name,
    COUNT(*) as row_count
FROM staging.staging_marketing_transactions

UNION ALL

SELECT
    'warehouse_fact_orders' as table_name,
    COUNT(*) as row_count
FROM warehouse.fact_orders

ORDER BY row_count DESC;

-- Check if fact_orders has any existing data
SELECT
    COUNT(*) as existing_fact_rows,
    COUNT(DISTINCT order_id) as distinct_orders,
    MIN(updated_at) as oldest_record,
    MAX(updated_at) as newest_record
FROM warehouse.fact_orders;

-- Check dimension table sizes
SELECT
    'dim_customer' as table_name, COUNT(*) as row_count FROM warehouse.dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM warehouse.dim_product
UNION ALL
SELECT 'dim_merchant', COUNT(*) FROM warehouse.dim_merchant
UNION ALL
SELECT 'dim_staff', COUNT(*) FROM warehouse.dim_staff
UNION ALL
SELECT 'dim_campaign', COUNT(*) FROM warehouse.dim_campaign
UNION ALL
SELECT 'dim_date', COUNT(*) FROM warehouse.dim_date
ORDER BY row_count DESC;

