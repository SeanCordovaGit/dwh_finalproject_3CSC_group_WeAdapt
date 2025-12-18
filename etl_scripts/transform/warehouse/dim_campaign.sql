-- ============================================================
-- DIMENSION: Campaign (SCD Type 2)
-- ============================================================

-- STEP 1: Create table with proper structure
CREATE TABLE IF NOT EXISTS warehouse.dim_campaign (
    campaign_key        SERIAL PRIMARY KEY,
    campaign_id         INTEGER NOT NULL,
    campaign_name       VARCHAR(255),
    campaign_type       VARCHAR(100),
    effective_date      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date            TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ensure table has all required columns (for existing tables)
ALTER TABLE warehouse.dim_campaign ADD COLUMN IF NOT EXISTS campaign_type VARCHAR(100);
ALTER TABLE warehouse.dim_campaign ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- STEP 2: Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dim_campaign_id ON warehouse.dim_campaign(campaign_id);
CREATE INDEX IF NOT EXISTS idx_dim_campaign_id_current ON warehouse.dim_campaign(campaign_id, is_current);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_campaign_id_current_unique 
    ON warehouse.dim_campaign(campaign_id) WHERE is_current = TRUE;

-- STEP 3: Load/Update data (adjust source table as needed)
-- First, update existing current records
UPDATE warehouse.dim_campaign
SET
    campaign_name = source_data.campaign_name,
    campaign_type = 'Marketing',
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT DISTINCT
        CASE
            WHEN t.campaign_id ~ '^[0-9]+$' THEN CAST(t.campaign_id AS INTEGER)
            WHEN t.campaign_id ~ '([0-9]+)' THEN CAST((REGEXP_MATCH(t.campaign_id, '([0-9]+)'))[1] AS INTEGER)
        END as campaign_id,
        COALESCE(c.campaign_name, 'Unknown Campaign') as campaign_name
    FROM staging.staging_marketing_transactions t
    LEFT JOIN staging.staging_marketing_campaigns c ON c.campaign_id = t.campaign_id
    WHERE t.campaign_id IS NOT NULL
        AND t.campaign_id ~ '[0-9]+'
) source_data
WHERE dim_campaign.campaign_id = source_data.campaign_id
    AND dim_campaign.is_current = TRUE;

-- Then, insert new records that don't exist
INSERT INTO warehouse.dim_campaign (campaign_id, campaign_name, campaign_type)
SELECT
    campaign_id,
    campaign_name,
    'Marketing' as campaign_type
FROM (
    SELECT DISTINCT
        CASE
            WHEN t.campaign_id ~ '^[0-9]+$' THEN CAST(t.campaign_id AS INTEGER)
            WHEN t.campaign_id ~ '([0-9]+)' THEN CAST((REGEXP_MATCH(t.campaign_id, '([0-9]+)'))[1] AS INTEGER)
        END as campaign_id,
        COALESCE(c.campaign_name, 'Unknown Campaign') as campaign_name,
        ROW_NUMBER() OVER (
            PARTITION BY CASE
                WHEN t.campaign_id ~ '^[0-9]+$' THEN CAST(t.campaign_id AS INTEGER)
                WHEN t.campaign_id ~ '([0-9]+)' THEN CAST((REGEXP_MATCH(t.campaign_id, '([0-9]+)'))[1] AS INTEGER)
            END
            ORDER BY t.campaign_id
        ) as rn
    FROM staging.staging_marketing_transactions t
    LEFT JOIN staging.staging_marketing_campaigns c ON c.campaign_id = t.campaign_id
    WHERE t.campaign_id IS NOT NULL
        AND t.campaign_id ~ '[0-9]+'
) deduped
WHERE rn = 1 AND campaign_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM warehouse.dim_campaign
        WHERE campaign_id = deduped.campaign_id
    );

-- STEP 4: Analyze for query optimization
ANALYZE warehouse.dim_campaign;

-- STEP 5: Log results
SELECT 
    'Dim campaign loaded' as status,
    COUNT(*) as total_records,
    COUNT(CASE WHEN is_current THEN 1 END) as current_records
FROM warehouse.dim_campaign;

