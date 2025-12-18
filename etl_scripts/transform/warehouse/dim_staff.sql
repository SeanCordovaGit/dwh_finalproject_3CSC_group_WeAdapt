-- ============================================================
-- DIMENSION: Staff (SCD Type 2)
-- ============================================================

-- STEP 1: Create table with proper structure
CREATE TABLE IF NOT EXISTS warehouse.dim_staff (
    staff_key           SERIAL PRIMARY KEY,
    staff_id            INTEGER NOT NULL,
    staff_name          VARCHAR(255),
    effective_date      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date            TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ensure table has all required columns (for existing tables)
ALTER TABLE warehouse.dim_staff ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- STEP 2: Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dim_staff_id ON warehouse.dim_staff(staff_id);
CREATE INDEX IF NOT EXISTS idx_dim_staff_id_current ON warehouse.dim_staff(staff_id, is_current);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_staff_id_current_unique 
    ON warehouse.dim_staff(staff_id) WHERE is_current = TRUE;

-- STEP 3: Load/Update data from staging
-- First, update existing current records
UPDATE warehouse.dim_staff
SET
    staff_name = source_data.name,
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT DISTINCT
        CAST(SUBSTRING(s.staff_id FROM 'STAFF([0-9]+)') AS INTEGER) as staff_id,
        s.name
    FROM staging.staging_enterprise_staff s
    WHERE s.staff_id IS NOT NULL
        AND s.staff_id ~ '^STAFF[0-9]+$'
) source_data
WHERE dim_staff.staff_id = source_data.staff_id
    AND dim_staff.is_current = TRUE;

-- Then, insert new records that don't exist
INSERT INTO warehouse.dim_staff (staff_id, staff_name)
SELECT
    staff_id,
    name as staff_name
FROM (
    SELECT DISTINCT
        CAST(SUBSTRING(s.staff_id FROM 'STAFF([0-9]+)') AS INTEGER) as staff_id,
        s.name,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(SUBSTRING(s.staff_id FROM 'STAFF([0-9]+)') AS INTEGER)
            ORDER BY s.staff_id
        ) as rn
    FROM staging.staging_enterprise_staff s
    WHERE s.staff_id IS NOT NULL
        AND s.staff_id ~ '^STAFF[0-9]+$'
) deduped
WHERE rn = 1
    AND NOT EXISTS (
        SELECT 1 FROM warehouse.dim_staff
        WHERE staff_id = deduped.staff_id
    );

-- STEP 4: Analyze for query optimization
ANALYZE warehouse.dim_staff;

-- STEP 5: Log results
SELECT 
    'Dim staff loaded' as status,
    COUNT(*) as total_records,
    COUNT(CASE WHEN is_current THEN 1 END) as current_records
FROM warehouse.dim_staff;

