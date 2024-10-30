SELECT
    date_date,
    COUNT(orders_id) AS nb_transactions,
    ROUND(SUM(CAST(revenue AS FLOAT64)),0) AS revenue,
    ROUND(AVG(CAST(revenue AS FLOAT64)),1) AS average_basket,
    ROUND(SUM(CAST(revenue AS FLOAT64))/COUNT(orders_id),1) AS average_basket_bis,
    ROUND(SUM(CAST(margin AS FLOAT64)),0) AS margin,
    ROUND(SUM(CAST(operational_margin AS FLOAT64)),0) AS operational_margin,
    ROUND(SUM(CAST(purchase_cost AS FLOAT64)),0) AS purchase_cost,
    ROUND(SUM(CAST(shipping_fee AS FLOAT64)),0) AS shipping_fee,
    ROUND(SUM(CAST(logcost AS FLOAT64)),0) AS logcost,
    ROUND(SUM(CAST(ship_cost AS FLOAT64)),0) AS ship_cost,
    SUM(CAST(quantity AS INT64)) AS quantity
FROM {{ref("int_orders_operational")}}
GROUP BY date_date
ORDER BY date_date DESC