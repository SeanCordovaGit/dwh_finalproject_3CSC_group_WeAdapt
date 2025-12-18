-- Simple ROAS Implementation for weadapt
-- Adds campaign cost data and creates ROAS views

-- =====================================================
-- STEP 1: Add Campaign Cost Column to Dimension Table
-- =====================================================

-- Add campaign_cost column to dim_campaign
ALTER TABLE warehouse.dim_campaign
ADD COLUMN IF NOT EXISTS campaign_cost DECIMAL(12,2) DEFAULT 0;

-- Add cost tracking metadata
ALTER TABLE warehouse.dim_campaign
ADD COLUMN IF NOT EXISTS cost_last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- =====================================================
-- STEP 2: Populate Sample Campaign Cost Data
-- =====================================================

-- Update existing campaigns with estimated costs
-- Based on revenue data, assuming reasonable marketing spend
UPDATE warehouse.dim_campaign
SET
    campaign_cost = CASE
        WHEN campaign_name = 'me neither' THEN 150000.00
        WHEN campaign_name = 'stick a fork in it' THEN 140000.00
        WHEN campaign_name = 'you must be new here' THEN 135000.00
        WHEN campaign_name = 'how do I get to the train station' THEN 145000.00
        WHEN campaign_name = 'mind your own beeswax' THEN 120000.00
        WHEN campaign_name = 'wouldn''t you know it' THEN 118000.00
        WHEN campaign_name = 'pound for pound' THEN 119000.00
        WHEN campaign_name = 'could be written on the back of a postage stamp' THEN 115000.00
        WHEN campaign_name = 'on the huh' THEN 110000.00
        WHEN campaign_name = 'would it hurt' THEN 117000.00
        ELSE 100000.00 -- Default cost for other campaigns
    END,
    cost_last_updated = CURRENT_TIMESTAMP
WHERE is_current = true;

-- =====================================================
-- STEP 3: Create ROAS Summary View
-- =====================================================

-- Create a new view that joins campaign effectiveness with cost data
CREATE OR REPLACE VIEW presentation.view_campaign_roas AS
SELECT
    ce.campaign_name,
    ce.total_orders,
    ce.campaigns_availed,
    ce.avail_rate,
    ce.total_revenue,
    COALESCE(dc.campaign_cost, 0) as campaign_cost,

    -- ROAS Calculations
    CASE
        WHEN COALESCE(dc.campaign_cost, 0) > 0
        THEN ROUND(ce.total_revenue / dc.campaign_cost, 2)
        ELSE NULL
    END as roas,

    -- Profitability
    CASE
        WHEN COALESCE(dc.campaign_cost, 0) > 0
        THEN ce.total_revenue - dc.campaign_cost
        ELSE 0
    END as campaign_profit,

    -- Performance Rating
    CASE
        WHEN COALESCE(dc.campaign_cost, 0) > 0 AND ce.total_revenue / dc.campaign_cost >= 3.0 THEN 'Excellent (3x+)'
        WHEN COALESCE(dc.campaign_cost, 0) > 0 AND ce.total_revenue / dc.campaign_cost >= 2.0 THEN 'Good (2-3x)'
        WHEN COALESCE(dc.campaign_cost, 0) > 0 AND ce.total_revenue / dc.campaign_cost >= 1.0 THEN 'Break-even (1-2x)'
        WHEN COALESCE(dc.campaign_cost, 0) > 0 AND ce.total_revenue / dc.campaign_cost < 1.0 THEN 'Poor (<1x)'
        ELSE 'No Cost Data'
    END as performance_rating

FROM presentation.view_campaign_effectiveness ce
LEFT JOIN warehouse.dim_campaign dc ON dc.campaign_name = ce.campaign_name AND dc.is_current = true
ORDER BY
    CASE
        WHEN COALESCE(dc.campaign_cost, 0) > 0 THEN ce.total_revenue / dc.campaign_cost
        ELSE 0
    END DESC;

-- =====================================================
-- STEP 4: Add Documentation Comments
-- =====================================================

COMMENT ON VIEW presentation.view_campaign_roas IS
'Campaign performance with ROAS calculations. ROAS = Revenue ÷ Cost. Includes profitability analysis and performance ratings.';

COMMENT ON COLUMN warehouse.dim_campaign.campaign_cost IS
'Total marketing/advertising spend for this campaign in USD. Used for ROAS calculations.';

-- =====================================================
-- STEP 5: Validation Query
-- =====================================================

-- Check the results
SELECT
    'ROAS Implementation Results' as status,
    COUNT(*) as total_campaigns,
    COUNT(CASE WHEN roas IS NOT NULL THEN 1 END) as campaigns_with_roas,
    ROUND(AVG(CASE WHEN roas IS NOT NULL THEN roas END), 2) as avg_roas,
    SUM(campaign_profit) as total_profit
FROM presentation.view_campaign_roas;

