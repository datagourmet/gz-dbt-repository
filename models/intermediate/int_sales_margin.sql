WITH tab1 as (
SELECT 
    s.products_id, 
    s.date_date, 
    s.orders_id,
    s.revenue, 
    s.quantity, 
    CAST(p.purchase_price AS FLOAT64) as purchase_price_ok, 
    ROUND(s.quantity*CAST(p.purchase_price AS FLOAT64),2) AS purchase_cost,
    ROUND (s.revenue - (s.quantity*CAST(p.purchase_price AS FLOAT64)),2) AS margin,
FROM {{ref("stg_raw__sales")}} s
LEFT JOIN {{ref("stg_raw__product")}} p 
    USING (products_id)
)
SELECT 
    tab1.*,
    {{ margin_percent('revenue', 'purchase_cost') }} AS margin_percent
FROM tab1