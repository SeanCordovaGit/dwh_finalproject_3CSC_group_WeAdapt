-- Add ROAS (Return on Ad Spend) functionality to weadapt Data Warehouse
-- This script adds campaign cost data and ROAS calculations

-- =====================================================
-- STEP 1: Add Campaign Cost Column to Dimension Table
-- =====================================================

-- Add campaign_cost column to dim_campaign
ALTER TABLE warehouse.dim_campaign
ADD COLUMN IF NOT EXISTS campaign_cost DECIMAL(12,2) DEFAULT 0;

-- Add cost_type for different spending categories
ALTER TABLE warehouse.dim_campaign
ADD COLUMN IF NOT EXISTS cost_type VARCHAR(50) DEFAULT 'marketing';

-- Add last_updated for cost tracking
ALTER TABLE warehouse.dim_campaign
ADD COLUMN IF NOT EXISTS cost_last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- =====================================================
-- STEP 2: Populate Sample Campaign Cost Data
-- =====================================================

-- Update existing campaigns with estimated costs
-- Based on revenue data, assuming 8-12% of revenue spent on marketing
UPDATE warehouse.dim_campaign
SET
    campaign_cost = CASE
        WHEN campaign_name = 'me neither' THEN 150000.00        -- ₱150K spent
        WHEN campaign_name = 'stick a fork in it' THEN 140000.00 -- ₱140K spent
        WHEN campaign_name = 'you must be new here' THEN 135000.00 -- ₱135K spent
        WHEN campaign_name = 'how do I get to the train station' THEN 145000.00 -- ₱145K spent
        WHEN campaign_name = 'mind your own beeswax' THEN 120000.00 -- ₱120K spent
        WHEN campaign_name = 'wouldn''t you know it' THEN 118000.00 -- ₱118K spent
        WHEN campaign_name = 'pound for pound' THEN 119000.00 -- ₱119K spent
        WHEN campaign_name = 'could be written on the back of a postage stamp' THEN 115000.00 -- ₱115K spent
        WHEN campaign_name = 'on the huh' THEN 110000.00 -- ₱110K spent
        WHEN campaign_name = 'would it hurt' THEN 117000.00 -- ₱117K spent
        ELSE 100000.00 -- Default ₱100K for other campaigns
    END,
    cost_type = 'digital_marketing',
    cost_last_updated = CURRENT_TIMESTAMP
WHERE is_current = true;

-- =====================================================
-- STEP 3: Create Campaign Cost Fact Table (Optional)
-- =====================================================

-- Create detailed campaign cost tracking table
CREATE TABLE IF NOT EXISTS warehouse.fact_campaign_cost (
    campaign_cost_key SERIAL PRIMARY KEY,
    campaign_key INT REFERENCES warehouse.dim_campaign(campaign_key),
    cost_date DATE NOT NULL,
    cost_amount DECIMAL(12,2) NOT NULL,
    cost_category VARCHAR(100), -- 'facebook_ads', 'google_ads', 'email', 'influencer', etc.
    cost_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_fact_campaign_cost_campaign_date
ON warehouse.fact_campaign_cost(campaign_key, cost_date);

-- Insert sample detailed cost data (aggregated by campaign)
INSERT INTO warehouse.fact_campaign_cost (campaign_key, cost_date, cost_amount, cost_category, cost_description)
SELECT
    dc.campaign_key,
    CURRENT_DATE - INTERVAL '30 days', -- Assume costs over last 30 days
    dc.campaign_cost * 0.8, -- 80% of total cost
    'digital_ads',
    'Facebook and Google Ads spending'
FROM warehouse.dim_campaign dc
WHERE dc.is_current = true AND dc.campaign_cost > 0
ON CONFLICT DO NOTHING;

-- =====================================================
-- STEP 4: Update Campaign Effectiveness View with ROAS
-- =====================================================

-- Recreate view with ROAS calculations
CREATE OR REPLACE VIEW presentation.view_campaign_effectiveness AS
SELECT
    c.campaign_name,
    c.campaign_description,
    COUNT(DISTINCT fcp.order_id) as total_orders,
    SUM(CASE WHEN fcp.availed THEN 1 ELSE 0 END) as campaigns_availed,
    ROUND(100.0 * SUM(CASE WHEN fcp.availed THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as avail_rate,
    SUM(COALESCE(fs.gross_amount, 0)) as total_revenue,
    COALESCE(c.campaign_cost, 0) as campaign_cost,
    COALESCE(fc.total_cost, 0) as detailed_cost,

    -- ROAS Calculations
    CASE
        WHEN COALESCE(c.campaign_cost, 0) > 0
        THEN ROUND(SUM(COALESCE(fs.gross_amount, 0)) / c.campaign_cost, 2)
        ELSE NULL
    END as roas_basic,

    CASE
        WHEN COALESCE(fc.total_cost, 0) > 0
        THEN ROUND(SUM(COALESCE(fs.gross_amount, 0)) / fc.total_cost, 2)
        ELSE NULL
    END as roas_detailed,

    -- Profitability indicators
    CASE
        WHEN COALESCE(c.campaign_cost, 0) > 0
        THEN SUM(COALESCE(fs.gross_amount, 0)) - c.campaign_cost
        ELSE 0
    END as campaign_profit,

    CASE
        WHEN COALESCE(c.campaign_cost, 0) > 0
        THEN ROUND(((SUM(COALESCE(fs.gross_amount, 0)) - c.campaign_cost) / c.campaign_cost) * 100, 2)
        ELSE NULL
    END as profit_margin_pct

FROM warehouse.fact_campaign_performance fcp
JOIN warehouse.dim_campaign c ON fcp.campaign_key = c.campaign_key
LEFT JOIN warehouse.fact_orders fs ON fcp.order_id = fs.order_id

-- Join with detailed cost data
LEFT JOIN (
    SELECT
        campaign_key,
        SUM(cost_amount) as total_cost,
        COUNT(*) as cost_entries
    FROM warehouse.fact_campaign_cost
    GROUP BY campaign_key
) fc ON fc.campaign_key = c.campaign_key

WHERE c.is_current = true
GROUP BY
    c.campaign_name,
    c.campaign_description,
    c.campaign_cost,
    fc.total_cost
ORDER BY total_revenue DESC;

-- =====================================================
-- STEP 5: Create ROAS Summary Views
-- =====================================================

-- Campaign ROAS summary for dashboard
CREATE OR REPLACE VIEW presentation.view_campaign_roas_summary AS
SELECT
    campaign_name,
    total_revenue,
    campaign_cost,
    roas_basic as roas,
    campaign_profit,
    profit_margin_pct,
    CASE
        WHEN roas_basic >= 3.0 THEN 'Excellent (3x+)'
        WHEN roas_basic >= 2.0 THEN 'Good (2-3x)'
        WHEN roas_basic >= 1.0 THEN 'Break-even (1-2x)'
        WHEN roas_basic < 1.0 THEN 'Poor (<1x)'
        ELSE 'No Data'
    END as performance_rating,
    CASE
        WHEN profit_margin_pct >= 200 THEN 'Highly Profitable'
        WHEN profit_margin_pct >= 100 THEN 'Profitable'
        WHEN profit_margin_pct >= 0 THEN 'Marginal'
        WHEN profit_margin_pct < 0 THEN 'Loss'
        ELSE 'No Data'
    END as profitability_status
FROM presentation.view_campaign_effectiveness
WHERE campaign_cost > 0
ORDER BY roas_basic DESC;

-- =====================================================
-- STEP 6: Create ROAS Performance Tracking
-- =====================================================

-- Monthly ROAS tracking view
CREATE OR REPLACE VIEW presentation.view_monthly_roas AS
SELECT
    DATE_TRUNC('month', fo.order_date) as month_year,
    c.campaign_name,
    COUNT(DISTINCT fo.order_id) as monthly_orders,
    SUM(fo.gross_amount) as monthly_revenue,
    AVG(c.campaign_cost) as avg_monthly_cost,
    ROUND(SUM(fo.gross_amount) / NULLIF(AVG(c.campaign_cost), 0), 2) as monthly_roas
FROM warehouse.fact_orders fo
JOIN warehouse.fact_campaign_performance fcp ON fo.order_id = fcp.order_id
JOIN warehouse.dim_campaign c ON fcp.campaign_key = c.campaign_key
WHERE c.is_current = true AND c.campaign_cost > 0
GROUP BY DATE_TRUNC('month', fo.order_date), c.campaign_name, c.campaign_cost
ORDER BY month_year DESC, monthly_roas DESC;

-- =====================================================
-- STEP 7: Add ROAS Validation Queries
-- =====================================================

-- Validate ROAS calculations
DO $$
DECLARE
    total_campaigns INT;
    campaigns_with_cost INT;
    campaigns_with_roas INT;
BEGIN
    SELECT COUNT(*) INTO total_campaigns
    FROM presentation.view_campaign_effectiveness;

    SELECT COUNT(*) INTO campaigns_with_cost
    FROM presentation.view_campaign_effectiveness
    WHERE campaign_cost > 0;

    SELECT COUNT(*) INTO campaigns_with_roas
    FROM presentation.view_campaign_effectiveness
    WHERE roas_basic IS NOT NULL;

    RAISE NOTICE 'ROAS Implementation Summary:';
    RAISE NOTICE 'Total campaigns: %', total_campaigns;
    RAISE NOTICE 'Campaigns with cost data: %', campaigns_with_cost;
    RAISE NOTICE 'Campaigns with ROAS calculated: %', campaigns_with_roas;
    RAISE NOTICE 'Coverage: %%%', ROUND((campaigns_with_roas::DECIMAL / NULLIF(total_campaigns, 0)) * 100, 1);
END $$;

-- =====================================================
-- STEP 8: Grant Permissions
-- =====================================================

-- Grant permissions for BI tools
GRANT SELECT ON presentation.view_campaign_effectiveness TO weadapt_readonly;
GRANT SELECT ON presentation.view_campaign_roas_summary TO weadapt_readonly;
GRANT SELECT ON presentation.view_monthly_roas TO weadapt_readonly;

-- =====================================================
-- STEP 9: Update Documentation Comments
-- =====================================================

COMMENT ON VIEW presentation.view_campaign_effectiveness IS
'Campaign performance metrics including revenue, conversion rates, costs, and ROAS calculations. ROAS = Revenue ÷ Cost.';

COMMENT ON VIEW presentation.view_campaign_roas_summary IS
'Simplified view of campaign ROAS performance with profitability ratings for dashboard consumption.';

COMMENT ON VIEW presentation.view_monthly_roas IS
'Monthly ROAS tracking to monitor campaign performance trends over time.';

COMMENT ON COLUMN warehouse.dim_campaign.campaign_cost IS
'Total marketing spend for this campaign in USD. Used for ROAS calculations.';

-- =====================================================
-- IMPLEMENTATION COMPLETE
-- =====================================================

-- Summary output
SELECT
    'ROAS Implementation Complete' as status,
    COUNT(*) as total_campaigns_with_roas
FROM presentation.view_campaign_roas_summary
WHERE roas IS NOT NULL;

