-- ============================================================
-- DIMENSION: Product (SCD Type 2)
-- ============================================================

-- STEP 1: Create table with proper structure
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key         SERIAL PRIMARY KEY,
    product_id          INTEGER NOT NULL,
    product_name        VARCHAR(255),
    product_type        VARCHAR(100),
    price               NUMERIC(12,2),
    effective_date      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date            TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ensure table has all required columns (for existing tables)
ALTER TABLE warehouse.dim_product ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- STEP 2: Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dim_product_id ON warehouse.dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_id_current ON warehouse.dim_product(product_id, is_current);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_product_id_current_unique 
    ON warehouse.dim_product(product_id) WHERE is_current = TRUE;

-- STEP 3: Load/Update data with deduplication
-- First, update existing current records
UPDATE warehouse.dim_product
SET
    product_name = source_data.product_name,
    product_type = source_data.product_type,
    price = source_data.price,
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT DISTINCT
        CAST(SUBSTRING(product_id FROM 'PRODUCT([0-9]+)') AS INTEGER) as product_id,
        product_name,
        product_type,
        price
    FROM staging.staging_business_products
    WHERE product_id IS NOT NULL
        AND product_id ~ '^PRODUCT[0-9]+$'
) source_data
WHERE dim_product.product_id = source_data.product_id
    AND dim_product.is_current = TRUE;

-- Then, insert new records that don't exist
INSERT INTO warehouse.dim_product (product_id, product_name, product_type, price)
SELECT
    product_id,
    product_name,
    product_type,
    price
FROM (
    SELECT DISTINCT
        CAST(SUBSTRING(product_id FROM 'PRODUCT([0-9]+)') AS INTEGER) as product_id,
        product_name,
        product_type,
        price,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(SUBSTRING(product_id FROM 'PRODUCT([0-9]+)') AS INTEGER)
            ORDER BY product_id
        ) as rn
    FROM staging.staging_business_products
    WHERE product_id IS NOT NULL
        AND product_id ~ '^PRODUCT[0-9]+$'
) deduped
WHERE rn = 1
    AND NOT EXISTS (
        SELECT 1 FROM warehouse.dim_product
        WHERE product_id = deduped.product_id
    );

-- STEP 4: Analyze for query optimization
ANALYZE warehouse.dim_product;

-- STEP 5: Log results
SELECT 
    'Dim product loaded' as status,
    COUNT(*) as total_records,
    COUNT(CASE WHEN is_current THEN 1 END) as current_records
FROM warehouse.dim_product;

