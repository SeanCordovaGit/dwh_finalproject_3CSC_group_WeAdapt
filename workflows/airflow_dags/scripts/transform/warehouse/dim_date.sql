-- Generate date dimension for 5 years
INSERT INTO warehouse.dim_date (full_date, year, quarter, month, month_name, week, day, day_of_week, day_name, is_weekend, is_holiday)
SELECT 
    date_series::DATE as full_date,
    EXTRACT(YEAR FROM date_series)::INT as year,
    EXTRACT(QUARTER FROM date_series)::INT as quarter,
    EXTRACT(MONTH FROM date_series)::INT as month,
    TO_CHAR(date_series, 'Month') as month_name,
    EXTRACT(WEEK FROM date_series)::INT as week,
    EXTRACT(DAY FROM date_series)::INT as day,
    EXTRACT(DOW FROM date_series)::INT as day_of_week,
    TO_CHAR(date_series, 'Day') as day_name,
    CASE WHEN EXTRACT(DOW FROM date_series) IN (0,6) THEN TRUE ELSE FALSE END as is_weekend,
    FALSE as is_holiday
FROM generate_series('2020-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) as date_series
ON CONFLICT (full_date) DO NOTHING;