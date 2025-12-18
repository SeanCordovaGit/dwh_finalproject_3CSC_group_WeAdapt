-- Simplified dashboard summary view - daily aggregates with campaign metrics
CREATE OR REPLACE VIEW presentation.view_dashboard_summary AS
SELECT
    -- Revenue metrics from fact_orders (aggregated daily)
    dd.full_date,
    SUM(fo.net_amount) as daily_revenue,
    COUNT(DISTINCT fo.order_id) as daily_orders,
    COUNT(DISTINCT fo.customer_key) as daily_customers,
    ROUND(AVG(fo.net_amount), 2) as avg_order_value,
    SUM(fo.quantity) as daily_quantity_sold,
    SUM(fo.discount_amount) as daily_discounts,

    -- Campaign effectiveness metrics (aggregated daily)
    COUNT(DISTINCT CASE WHEN fcp.availed THEN fcp.order_id END) as daily_campaigns_availed,
    COUNT(DISTINCT fcp.order_id) as daily_campaign_orders,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN fcp.availed THEN fcp.order_id END) / NULLIF(COUNT(DISTINCT fcp.order_id), 0), 2) as daily_campaign_avail_rate,
    COALESCE(SUM(CASE WHEN fcp.availed THEN fo.net_amount ELSE 0 END), 0) as daily_campaign_revenue

FROM warehouse.fact_orders fo
LEFT JOIN warehouse.dim_date dd ON fo.order_date_key = dd.date_key
LEFT JOIN warehouse.fact_campaign_performance fcp ON fcp.order_id = fo.order_id
WHERE dd.full_date IS NOT NULL
GROUP BY dd.full_date
ORDER BY dd.full_date DESC;

COMMENT ON VIEW presentation.view_dashboard_summary IS 'Daily dashboard summary with revenue and campaign metrics for Power BI consumption';

