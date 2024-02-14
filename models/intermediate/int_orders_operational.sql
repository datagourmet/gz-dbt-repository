SELECT
  o.orders_id
  ,o.date_date
  ,ROUND(CAST(o.margin AS FLOAT64) + CAST(s.shipping_fee AS FLOAT64) - (CAST(s.logcost AS FLOAT64) + CAST(s.ship_cost AS FLOAT64)), 2) AS operational_margin
  ,o.quantity
  ,o.revenue
  ,o.purchase_cost
  ,o.margin
  ,s.shipping_fee
  ,s.logcost
  ,s.ship_cost
FROM {{ref("int_orders_margin")}} o
LEFT JOIN {{ref("stg_raw__ship")}} s 
  USING(orders_id)
ORDER BY orders_id desc
