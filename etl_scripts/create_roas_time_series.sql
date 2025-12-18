-- Create time-series ROAS data for line chart visualization
-- This creates monthly ROAS trends for campaign performance over time

-- =====================================================
-- STEP 1: Create Monthly ROAS View
-- =====================================================

CREATE OR REPLACE VIEW presentation.view_monthly_roas AS
WITH monthly_campaign_data AS (
    -- Aggregate campaign data by month
    SELECT
        DATE_TRUNC('month', fo.order_date) as month_year,
        c.campaign_name,
        COUNT(DISTINCT fo.order_id) as monthly_orders,
        SUM(fo.gross_amount) as monthly_revenue,
        AVG(c.campaign_cost) as avg_monthly_cost,
        COUNT(DISTINCT CASE WHEN fcp.availed THEN fo.order_id END) as availed_orders,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN fcp.availed THEN fo.order_id END) / COUNT(DISTINCT fo.order_id), 2) as monthly_conversion_rate
    FROM warehouse.fact_orders fo
    JOIN warehouse.fact_campaign_performance fcp ON fo.order_id = fcp.order_id
    JOIN warehouse.dim_campaign c ON fcp.campaign_key = c.campaign_key
    WHERE c.is_current = true AND c.campaign_cost > 0
    GROUP BY DATE_TRUNC('month', fo.order_date), c.campaign_name, c.campaign_cost
)
SELECT
    month_year,
    campaign_name,
    monthly_orders,
    monthly_revenue,
    avg_monthly_cost,
    CASE
        WHEN avg_monthly_cost > 0
        THEN ROUND(monthly_revenue / avg_monthly_cost, 2)
        ELSE NULL
    END as monthly_roas,
    monthly_conversion_rate,
    CASE
        WHEN avg_monthly_cost > 0
        THEN monthly_revenue - avg_monthly_cost
        ELSE 0
    END as monthly_profit,
    -- Add quarter and year for additional grouping
    DATE_TRUNC('quarter', month_year) as quarter_year,
    EXTRACT(YEAR FROM month_year) as year
FROM monthly_campaign_data
ORDER BY month_year DESC, monthly_roas DESC;

-- =====================================================
-- STEP 2: Create Rolling ROAS Trends View
-- =====================================================

CREATE OR REPLACE VIEW presentation.view_roas_trends AS
SELECT
    month_year,
    campaign_name,
    monthly_roas,
    monthly_revenue,
    monthly_orders,

    -- 3-month rolling average ROAS
    ROUND(AVG(monthly_roas) OVER (
        PARTITION BY campaign_name
        ORDER BY month_year
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) as roas_3month_avg,

    -- Month-over-month ROAS change
    monthly_roas - LAG(monthly_roas) OVER (
        PARTITION BY campaign_name
        ORDER BY month_year
    ) as roas_mom_change,

    -- Month-over-month ROAS percentage change
    CASE
        WHEN LAG(monthly_roas) OVER (
            PARTITION BY campaign_name
            ORDER BY month_year
        ) > 0
        THEN ROUND(100.0 * (monthly_roas - LAG(monthly_roas) OVER (
            PARTITION BY campaign_name
            ORDER BY month_year
        )) / LAG(monthly_roas) OVER (
            PARTITION BY campaign_name
            ORDER BY month_year
        ), 1)
        ELSE NULL
    END as roas_mom_pct_change,

    -- Performance trend indicator
    CASE
        WHEN monthly_roas >= 20 THEN 'Excellent'
        WHEN monthly_roas >= 10 THEN 'Good'
        WHEN monthly_roas >= 5 THEN 'Fair'
        WHEN monthly_roas >= 1 THEN 'Poor'
        ELSE 'Very Poor'
    END as performance_level

FROM presentation.view_monthly_roas
ORDER BY campaign_name, month_year;

-- =====================================================
-- STEP 3: Create Campaign ROAS Summary with Trends
-- =====================================================

CREATE OR REPLACE VIEW presentation.view_campaign_roas_summary AS
SELECT
    campaign_name,
    COUNT(*) as months_active,
    ROUND(AVG(monthly_roas), 2) as avg_roas,
    ROUND(MAX(monthly_roas), 2) as best_roas,
    ROUND(MIN(monthly_roas), 2) as worst_roas,
    ROUND(STDDEV(monthly_roas), 2) as roas_volatility,
    SUM(monthly_revenue) as total_revenue,
    SUM(monthly_orders) as total_orders,
    ROUND(AVG(monthly_conversion_rate), 2) as avg_conversion_rate,

    -- Trend analysis
    CASE
        WHEN AVG(monthly_roas) >= 20 THEN 'High Performer'
        WHEN AVG(monthly_roas) >= 10 THEN 'Good Performer'
        WHEN AVG(monthly_roas) >= 5 THEN 'Moderate Performer'
        ELSE 'Low Performer'
    END as overall_rating,

    -- Consistency rating (lower volatility = more consistent)
    CASE
        WHEN STDDEV(monthly_roas) <= 5 THEN 'Very Consistent'
        WHEN STDDEV(monthly_roas) <= 10 THEN 'Consistent'
        WHEN STDDEV(monthly_roas) <= 20 THEN 'Variable'
        ELSE 'Highly Variable'
    END as consistency_rating

FROM presentation.view_monthly_roas
GROUP BY campaign_name
ORDER BY avg_roas DESC;

-- =====================================================
-- STEP 4: Add Documentation
-- =====================================================

COMMENT ON VIEW presentation.view_monthly_roas IS
'Monthly ROAS performance for each campaign. Shows revenue, costs, and ROAS calculations over time.';

COMMENT ON VIEW presentation.view_roas_trends IS
'ROAS trends with rolling averages and month-over-month changes. Includes performance level indicators.';

COMMENT ON VIEW presentation.view_campaign_roas_summary IS
'Campaign ROAS summary with averages, volatility, and overall performance ratings.';

-- =====================================================
-- STEP 5: Validation Query
-- =====================================================

SELECT
    'ROAS Time Series Views Created' as status,
    (SELECT COUNT(*) FROM presentation.view_monthly_roas) as monthly_records,
    (SELECT COUNT(*) FROM presentation.view_roas_trends) as trend_records,
    (SELECT COUNT(*) FROM presentation.view_campaign_roas_summary) as summary_records,
    (SELECT ROUND(AVG(monthly_roas), 2) FROM presentation.view_monthly_roas) as avg_monthly_roas;

