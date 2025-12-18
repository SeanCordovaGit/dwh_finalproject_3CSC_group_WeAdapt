-- Staging Layer Transformations
-- Basic validation and cleansing of raw data

-- Validate that key tables have data
DO $$
DECLARE
    customer_count INT;
    product_count INT;
    order_count INT;
BEGIN
    SELECT COUNT(*) INTO customer_count FROM staging.staging_customer_profiles;
    SELECT COUNT(*) INTO product_count FROM staging.staging_business_products;
    SELECT COUNT(*) INTO order_count FROM staging.staging_operations_order_headers;

    RAISE NOTICE 'Staging validation - Customers: %, Products: %, Orders: %',
        customer_count, product_count, order_count;

    -- Basic data quality checks - be more lenient for development
    IF product_count = 0 THEN
        RAISE EXCEPTION 'No product data found in staging_business_products';
    END IF;

    -- Warn about missing data but don't fail
    IF customer_count = 0 THEN
        RAISE WARNING 'No customer profile data found in staging_customer_profiles';
    END IF;

    IF order_count = 0 THEN
        RAISE WARNING 'No order header data found in staging_operations_order_headers';
    END IF;
END $$;

-- Could add more transformations here like:
-- - Standardizing formats
-- - Deduplication
-- - Basic cleansing

SELECT 'Staging transformations completed successfully' as message;

