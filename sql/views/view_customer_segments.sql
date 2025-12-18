CREATE OR REPLACE VIEW presentation.view_customer_segments AS
SELECT
    dc.job_level,
    COUNT(DISTINCT fo.customer_key) as customer_count,
    COUNT(fo.order_id) as total_orders,
    SUM(fo.gross_amount) as total_revenue,
    ROUND(AVG(fo.gross_amount), 2) as avg_order_value,
    ROUND(AVG(fo.quantity), 2) as avg_quantity_per_order,
    ROUND(100.0 * SUM(fo.gross_amount) / SUM(SUM(fo.gross_amount)) OVER (), 2) as revenue_percentage,
    ROUND(100.0 * COUNT(DISTINCT fo.customer_key) / SUM(COUNT(DISTINCT fo.customer_key)) OVER (), 2) as customer_percentage
FROM warehouse.fact_orders fo
JOIN warehouse.dim_customer dc ON fo.customer_key = dc.customer_key
WHERE dc.is_current = true
GROUP BY dc.job_level
ORDER BY total_revenue DESC;

COMMENT ON VIEW presentation.view_customer_segments IS 'Customer segmentation analysis by job level with revenue and order metrics';

