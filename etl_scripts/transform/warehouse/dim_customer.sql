-- ============================================================
-- DIMENSION: Customer (SCD Type 2)
-- ============================================================

-- STEP 1: Create table with proper structure
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key        SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    name                VARCHAR(255),
    job_title           VARCHAR(255),
    job_level           VARCHAR(100),
    effective_date      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date            TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ensure table has all required columns (for existing tables)
ALTER TABLE warehouse.dim_customer ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- STEP 2: Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON warehouse.dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_id_current ON warehouse.dim_customer(customer_id, is_current);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_customer_id_current_unique 
    ON warehouse.dim_customer(customer_id) WHERE is_current = TRUE;

-- STEP 3: Load/Update data with deduplication
-- First, update existing current records
UPDATE warehouse.dim_customer
SET
    name = source_data.name,
    job_title = source_data.job_title,
    job_level = source_data.job_level,
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT DISTINCT
        CAST(SUBSTRING(COALESCE(p.user_id, j.user_id, c.user_id) FROM 'USER([0-9]+)') AS INTEGER) as customer_id,
        COALESCE(p.name, c.name, j.name) as name,
        COALESCE(j.job_title, '') as job_title,
        COALESCE(j.job_level, '') as job_level
    FROM staging.staging_customer_profiles p
    FULL OUTER JOIN staging.staging_customer_jobs j ON p.user_id = j.user_id
    FULL OUTER JOIN staging.staging_customer_cards c ON COALESCE(p.user_id, j.user_id) = c.user_id
    WHERE COALESCE(p.user_id, j.user_id, c.user_id) IS NOT NULL
        AND COALESCE(p.user_id, j.user_id, c.user_id) ~ '^USER[0-9]+$'  -- Validate format
) source_data
WHERE dim_customer.customer_id = source_data.customer_id
    AND dim_customer.is_current = TRUE;

-- Then, insert new records that don't exist
INSERT INTO warehouse.dim_customer (customer_id, name, job_title, job_level)
SELECT
    customer_id,
    name,
    job_title,
    job_level
FROM (
    SELECT DISTINCT
        CAST(SUBSTRING(COALESCE(p.user_id, j.user_id, c.user_id) FROM 'USER([0-9]+)') AS INTEGER) as customer_id,
        COALESCE(p.name, c.name, j.name) as name,
        COALESCE(j.job_title, '') as job_title,
        COALESCE(j.job_level, '') as job_level,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(SUBSTRING(COALESCE(p.user_id, j.user_id, c.user_id) FROM 'USER([0-9]+)') AS INTEGER)
            ORDER BY p.user_id NULLS LAST, j.user_id NULLS LAST, c.user_id NULLS LAST
        ) as rn
    FROM staging.staging_customer_profiles p
    FULL OUTER JOIN staging.staging_customer_jobs j ON p.user_id = j.user_id
    FULL OUTER JOIN staging.staging_customer_cards c ON COALESCE(p.user_id, j.user_id) = c.user_id
    WHERE COALESCE(p.user_id, j.user_id, c.user_id) IS NOT NULL
        AND COALESCE(p.user_id, j.user_id, c.user_id) ~ '^USER[0-9]+$'  -- Validate format
) deduped
WHERE rn = 1
    AND NOT EXISTS (
        SELECT 1 FROM warehouse.dim_customer
        WHERE customer_id = deduped.customer_id
    );

-- STEP 4: Analyze for query optimization
ANALYZE warehouse.dim_customer;

-- STEP 5: Log results
SELECT 
    'Dim customer loaded' as status,
    COUNT(*) as total_records,
    COUNT(CASE WHEN is_current THEN 1 END) as current_records
FROM warehouse.dim_customer;

