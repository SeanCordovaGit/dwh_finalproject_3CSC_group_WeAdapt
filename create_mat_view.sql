-- Create the missing materialized view for daily sales aggregation
-- Run this directly in your PostgreSQL database

-- First, ensure the presentation schema exists
CREATE SCHEMA IF NOT EXISTS presentation;

-- Create the materialized view (simplified version first)
CREATE MATERIALIZED VIEW IF NOT EXISTS presentation.mat_agg_daily_sales AS
SELECT
    dd.full_date,
    dd.year,
    dd.month,
    COUNT(DISTINCT fo.order_id) as daily_orders,
    COUNT(DISTINCT fo.customer_key) as daily_customers,
    SUM(fo.gross_amount) as daily_revenue,
    ROUND(AVG(fo.gross_amount), 2) as avg_order_value,
    SUM(fo.quantity) as daily_quantity_sold
FROM warehouse.fact_orders fo
JOIN warehouse.dim_date dd ON fo.order_date_key = dd.date_key
WHERE dd.full_date >= '2020-01-01'  -- Limit to recent dates
GROUP BY dd.full_date, dd.year, dd.month
ORDER BY dd.full_date;

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_mat_daily_sales_date ON presentation.mat_agg_daily_sales(full_date);
CREATE INDEX IF NOT EXISTS idx_mat_daily_sales_month ON presentation.mat_agg_daily_sales(year, month);

-- Add comment
COMMENT ON MATERIALIZED VIEW presentation.mat_agg_daily_sales IS 'Materialized view of daily sales aggregations for fast reporting access';

-- Create refresh function
CREATE OR REPLACE FUNCTION presentation.refresh_daily_sales_agg()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW presentation.mat_agg_daily_sales;
    RAISE NOTICE 'Materialized view mat_agg_daily_sales refreshed successfully';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION presentation.refresh_daily_sales_agg() IS 'Function to refresh the daily sales aggregation materialized view';
