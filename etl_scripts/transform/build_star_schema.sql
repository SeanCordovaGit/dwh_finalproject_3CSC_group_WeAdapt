-- Build Star Schema Script
-- Orchestrates the creation of all dimensions and facts for the weadapt DWH

-- This script should be run after staging data has been loaded
-- It creates the Kimball star schema with proper referential integrity

-- Enable timing for performance monitoring
\timing

-- Create dimensions first (no dependencies)
\i warehouse/dim_date.sql
\i warehouse/dim_customer.sql
\i warehouse/dim_product.sql
\i warehouse/dim_merchant.sql
\i warehouse/dim_staff.sql
\i warehouse/dim_campaign.sql

-- Create facts (depend on dimensions)
\i warehouse/fact_orders.sql
\i warehouse/fact_sales.sql
\i warehouse/fact_campaign_performance.sql
\i warehouse/fact_operations.sql

-- Create presentation layer views and materialized views
\i ../views/view_campaign_effectiveness.sql
\i ../views/view_customer_segments.sql
\i ../materialized/mat_agg_daily_sales.sql

-- Validate schema integrity
DO $$
DECLARE
    dim_count INTEGER;
    fact_count INTEGER;
BEGIN
    -- Check that all dimension tables have data
    SELECT COUNT(*) INTO dim_count
    FROM (
        SELECT 'dim_customer' as table_name, COUNT(*) as cnt FROM warehouse.dim_customer
        UNION ALL
        SELECT 'dim_product', COUNT(*) FROM warehouse.dim_product
        UNION ALL
        SELECT 'dim_date', COUNT(*) FROM warehouse.dim_date
    ) dims
    WHERE cnt = 0;

    IF dim_count > 0 THEN
        RAISE WARNING 'Some dimension tables are empty. Check data loading.';
    END IF;

    -- Check that fact tables have referential integrity
    SELECT COUNT(*) INTO fact_count FROM warehouse.fact_orders LIMIT 1;

    IF fact_count = 0 THEN
        RAISE WARNING 'No fact data loaded. Check transformation logic.';
    END IF;

    RAISE NOTICE 'Star schema build completed. Dimensions: %, Facts: %', dim_count, fact_count;
END $$;

-- Refresh materialized views
SELECT presentation.refresh_daily_sales_agg();

-- Final validation
SELECT
    'Dimensions' as layer,
    COUNT(*) as table_count
FROM information_schema.tables
WHERE table_schema = 'warehouse'
AND table_name LIKE 'dim_%'
UNION ALL
SELECT
    'Facts' as layer,
    COUNT(*) as table_count
FROM information_schema.tables
WHERE table_schema = 'warehouse'
AND table_name LIKE 'fact_%'
UNION ALL
SELECT
    'Views' as layer,
    COUNT(*) as table_count
FROM information_schema.views
WHERE table_schema = 'presentation'
UNION ALL
SELECT
    'Materialized Views' as layer,
    COUNT(*) as table_count
FROM information_schema.tables
WHERE table_schema = 'presentation'
AND table_type = 'MATERIALIZED VIEW';

COMMENT ON DATABASE weadapt_dwh IS 'weadapt Kimball star schema data warehouse - Built on ' || CURRENT_TIMESTAMP;

