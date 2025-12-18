-- ============================================================
-- DIMENSION: Date (No SCD - Static Reference)
-- ============================================================

-- STEP 1: Create table with proper structure
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key            SERIAL PRIMARY KEY,
    full_date           DATE NOT NULL UNIQUE,
    year                INTEGER,
    quarter             INTEGER,
    month               INTEGER,
    month_name          VARCHAR(20),
    week                INTEGER,
    day                 INTEGER,
    day_of_week         INTEGER,
    day_name            VARCHAR(20),
    is_weekend          BOOLEAN,
    is_holiday          BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STEP 2: Create indexes for performance
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_date_full_date ON warehouse.dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON warehouse.dim_date(year, month);

-- STEP 3: Load date range from 2020-01-01 to 2025-12-31
INSERT INTO warehouse.dim_date (
    full_date, year, quarter, month, month_name, week, day, day_of_week, day_name,
    is_weekend, is_holiday
)
SELECT
    full_date,
    EXTRACT(YEAR FROM full_date)::INTEGER as year,
    EXTRACT(QUARTER FROM full_date)::INTEGER as quarter,
    EXTRACT(MONTH FROM full_date)::INTEGER as month,
    TRIM(TO_CHAR(full_date, 'Month')) as month_name,
    EXTRACT(WEEK FROM full_date)::INTEGER as week,
    EXTRACT(DAY FROM full_date)::INTEGER as day,
    EXTRACT(DOW FROM full_date)::INTEGER as day_of_week,
    TRIM(TO_CHAR(full_date, 'Day')) as day_name,
    CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,
    FALSE as is_holiday  -- Can be updated later with holiday logic
FROM generate_series(
    '2020-01-01'::date,
    '2025-12-31'::date,
    '1 day'::interval
) AS full_date
ON CONFLICT (full_date) DO NOTHING;

-- STEP 4: Analyze for query optimization
ANALYZE warehouse.dim_date;

-- STEP 5: Log results
SELECT 
    'Dim date loaded' as status,
    COUNT(*) as total_records,
    MIN(full_date) as min_date,
    MAX(full_date) as max_date
FROM warehouse.dim_date;
