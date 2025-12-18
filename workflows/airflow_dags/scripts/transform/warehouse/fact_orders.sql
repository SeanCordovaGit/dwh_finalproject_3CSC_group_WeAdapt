-- Load fact_orders from staging tables
-- Combine operations, enterprise, and marketing data

-- Clear existing data for reprocessing (in production, handle incrementally)
TRUNCATE TABLE warehouse.fact_sales;

-- Insert fact data by joining staging tables
INSERT INTO warehouse.fact_sales (
    order_id, customer_key, product_key, merchant_key, staff_key, campaign_key,
    order_date_key, quantity, price, total_amount, discount_amount, net_amount
)
SELECT
    oh.order_id,
    COALESCE(dc.customer_key, -1) as customer_key,  -- Default for unknown customers
    COALESCE(dp.product_key, -1) as product_key,    -- Default for unknown products
    COALESCE(dm.merchant_key, -1) as merchant_key,  -- Default for unknown merchants
    COALESCE(ds.staff_key, -1) as staff_key,        -- Default for unknown staff
    COALESCE(dcamp.campaign_key, -1) as campaign_key, -- Default for no campaign
    COALESCE(dd.date_key, -1) as order_date_key,    -- Default for invalid dates
    COALESCE(lp.quantity::INT, 1) as quantity,      -- Default quantity
    COALESCE(lp.price, 0.0) as price,
    COALESCE(lp.price * lp.quantity::INT, 0.0) as total_amount,
    0.0 as discount_amount,  -- Placeholder for discount logic
    COALESCE(lp.price * lp.quantity::INT, 0.0) as net_amount
FROM staging.staging_operations_order_headers oh
LEFT JOIN staging.staging_operations_line_items_prices lp ON oh.order_id = lp.order_id
LEFT JOIN staging.staging_operations_line_items_products lprod ON oh.order_id = lprod.order_id
LEFT JOIN staging.staging_enterprise_orders eo ON oh.order_id = eo.order_id
LEFT JOIN staging.staging_marketing_transactions mt ON oh.order_id = mt.order_id
LEFT JOIN warehouse.dim_customer dc ON oh.user_id::VARCHAR = dc.customer_id
LEFT JOIN warehouse.dim_product dp ON COALESCE(lprod.product_id, '') = dp.product_id
LEFT JOIN warehouse.dim_merchant dm ON COALESCE(eo.merchant_id, '') = dm.merchant_id
LEFT JOIN warehouse.dim_staff ds ON COALESCE(eo.staff_id, '') = ds.staff_id
LEFT JOIN warehouse.dim_campaign dcamp ON COALESCE(mt.campaign_id, '') = dcamp.campaign_id
LEFT JOIN warehouse.dim_date dd ON oh.transaction_date = dd.full_date
WHERE oh.order_id IS NOT NULL
ON CONFLICT (order_id) DO NOTHING;

-- Update with delay information for operations fact
UPDATE warehouse.fact_sales
SET on_time_delivery = CASE
    WHEN dd.delay_in_days <= 3 THEN true
    ELSE false
END
FROM staging.staging_operations_delivery_delays dd
WHERE fact_sales.order_id = dd.order_id;

-- Log the results
SELECT 'Fact sales loaded: ' || COUNT(*) || ' records' as message
FROM warehouse.fact_sales;
