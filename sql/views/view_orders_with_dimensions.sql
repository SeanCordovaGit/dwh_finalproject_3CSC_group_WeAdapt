-- Orders fact table with all dimension attributes joined for Power BI
CREATE OR REPLACE VIEW presentation.view_orders_with_dimensions AS
SELECT
    -- Order facts
    fo.order_id,
    fo.quantity,
    fo.unit_price,
    fo.gross_amount,
    fo.discount_amount,
    fo.net_amount,
    fo.availed,
    fo.delivery_status,
    fo.logistics_provider,
    fo.processing_time_hours,
    fo.on_time_delivery,

    -- Date dimensions
    od.full_date as order_date,
    od.year as order_year,
    od.month as order_month,
    od.month_name as order_month_name,
    od.quarter as order_quarter,
    od.day_name as order_day_name,

    ea.full_date as estimated_arrival_date,
    ea.year as arrival_year,
    ea.month as arrival_month,
    ea.month_name as arrival_month_name,

    -- Customer dimensions
    dc.customer_id,
    dc.name as customer_name,
    dc.job_title,
    dc.job_level,

    -- Product dimensions
    dp.product_id,
    dp.product_name,
    dp.product_type,
    dp.price as product_base_price,

    -- Merchant dimensions
    dm.merchant_id,
    dm.merchant_name,

    -- Staff dimensions
    ds.staff_id,
    ds.staff_name,
    ds.role as staff_role,

    -- Campaign dimensions
    dcam.campaign_id,
    dcam.campaign_name,
    dcam.campaign_description,
    dcam.discount as campaign_discount

FROM warehouse.fact_orders fo
LEFT JOIN warehouse.dim_date od ON fo.order_date_key = od.date_key
LEFT JOIN warehouse.dim_date ea ON fo.estimated_arrival_key = ea.date_key
LEFT JOIN warehouse.dim_customer dc ON fo.customer_key = dc.customer_key AND dc.is_current = true
LEFT JOIN warehouse.dim_product dp ON fo.product_key = dp.product_key AND dp.is_current = true
LEFT JOIN warehouse.dim_merchant dm ON fo.merchant_key = dm.merchant_key AND dm.is_current = true
LEFT JOIN warehouse.dim_staff ds ON fo.staff_key = ds.staff_key AND ds.is_current = true
LEFT JOIN warehouse.dim_campaign dcam ON fo.campaign_key = dcam.campaign_key AND dcam.is_current = true;

COMMENT ON VIEW presentation.view_orders_with_dimensions IS 'Orders fact table with all dimension attributes pre-joined for Power BI dashboards';

