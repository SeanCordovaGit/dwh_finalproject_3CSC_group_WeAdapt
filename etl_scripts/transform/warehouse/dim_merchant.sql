-- ============================================================
-- DIMENSION: Merchant (SCD Type 2)
-- ============================================================

-- STEP 1: Create table with proper structure
CREATE TABLE IF NOT EXISTS warehouse.dim_merchant (
    merchant_key        SERIAL PRIMARY KEY,
    merchant_id         INTEGER NOT NULL,
    merchant_name       VARCHAR(255),
    effective_date      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date            TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ensure table has all required columns (for existing tables)
ALTER TABLE warehouse.dim_merchant ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- STEP 2: Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dim_merchant_id ON warehouse.dim_merchant(merchant_id);
CREATE INDEX IF NOT EXISTS idx_dim_merchant_id_current ON warehouse.dim_merchant(merchant_id, is_current);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_merchant_id_current_unique 
    ON warehouse.dim_merchant(merchant_id) WHERE is_current = TRUE;

-- STEP 3: Load/Update data (adjust source table as needed)
-- First, update existing current records
UPDATE warehouse.dim_merchant
SET
    merchant_name = source_data.merchant_name,
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT DISTINCT
        CAST(SUBSTRING(o.merchant_id FROM 'MERCHANT([0-9]+)') AS INTEGER) as merchant_id,
        COALESCE(m.name, 'Unknown Merchant') as merchant_name
    FROM staging.staging_enterprise_orders o
    LEFT JOIN staging.staging_enterprise_merchants m ON m.merchant_id = o.merchant_id
    WHERE o.merchant_id IS NOT NULL
        AND o.merchant_id ~ '^MERCHANT[0-9]+$'
) source_data
WHERE dim_merchant.merchant_id = source_data.merchant_id
    AND dim_merchant.is_current = TRUE;

-- Then, insert new records that don't exist
INSERT INTO warehouse.dim_merchant (merchant_id, merchant_name)
SELECT
    merchant_id,
    merchant_name
FROM (
    SELECT DISTINCT
        CAST(SUBSTRING(o.merchant_id FROM 'MERCHANT([0-9]+)') AS INTEGER) as merchant_id,
        COALESCE(m.name, 'Unknown Merchant') as merchant_name,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(SUBSTRING(o.merchant_id FROM 'MERCHANT([0-9]+)') AS INTEGER)
            ORDER BY o.merchant_id
        ) as rn
    FROM staging.staging_enterprise_orders o
    LEFT JOIN staging.staging_enterprise_merchants m ON m.merchant_id = o.merchant_id
    WHERE o.merchant_id IS NOT NULL
        AND o.merchant_id ~ '^MERCHANT[0-9]+$'
) deduped
WHERE rn = 1
    AND NOT EXISTS (
        SELECT 1 FROM warehouse.dim_merchant
        WHERE merchant_id = deduped.merchant_id
    );

-- STEP 4: Analyze for query optimization
ANALYZE warehouse.dim_merchant;

-- STEP 5: Log results
SELECT 
    'Dim merchant loaded' as status,
    COUNT(*) as total_records,
    COUNT(CASE WHEN is_current THEN 1 END) as current_records
FROM warehouse.dim_merchant;

