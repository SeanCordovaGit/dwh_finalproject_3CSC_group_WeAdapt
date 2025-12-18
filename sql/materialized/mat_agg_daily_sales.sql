-- Create materialized view for daily sales aggregation
-- This provides fast access to daily sales metrics for reporting and dashboards

CREATE MATERIALIZED VIEW IF NOT EXISTS presentation.mat_agg_daily_sales AS
SELECT
    dd.full_date,
    dd.year,
    dd.month,
    dd.month_name,
    dd.quarter,
    COUNT(DISTINCT fo.order_id) as daily_orders,
    COUNT(DISTINCT fo.customer_key) as daily_customers,
    SUM(fo.net_amount) as daily_revenue,
    ROUND(AVG(fo.net_amount), 2) as avg_order_value,
    SUM(fo.quantity) as daily_quantity_sold,
    SUM(fo.discount_amount) as daily_discounts,
    CASE
        WHEN dd.day_of_week IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    CASE
        WHEN dd.is_holiday THEN 'Holiday'
        ELSE 'Regular'
    END as date_category
FROM warehouse.fact_orders fo
JOIN warehouse.dim_date dd ON fo.order_date_key = dd.date_key
GROUP BY
    dd.full_date, dd.year, dd.month, dd.month_name, dd.quarter,
    dd.day_of_week, dd.is_holiday
ORDER BY dd.full_date;

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_mat_daily_sales_date ON presentation.mat_agg_daily_sales(full_date);
CREATE INDEX IF NOT EXISTS idx_mat_daily_sales_month ON presentation.mat_agg_daily_sales(year, month);

COMMENT ON MATERIALIZED VIEW presentation.mat_agg_daily_sales IS 'Materialized view of daily sales aggregations for fast reporting access';

-- Function to refresh the materialized view
CREATE OR REPLACE FUNCTION presentation.refresh_daily_sales_agg()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW presentation.mat_agg_daily_sales;
    RAISE NOTICE 'Materialized view mat_agg_daily_sales refreshed successfully';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION presentation.refresh_daily_sales_agg() IS 'Function to refresh the daily sales aggregation materialized view';

