 SELECT
     date_date,
     operational_margin - IFNULL(ads_cost,0) AS ads_margin,
     ROUND(average_basket,2) AS average_basket,
     operational_margin,
     ads_cost,
     ads_impression,
     ads_clicks,
     quantity,
     revenue,
     purchase_cost,
     margin,
     shipping_fee,
     logcost,
     ship_cost
 FROM {{ ref('int_campaigns_day') }}
 FULL OUTER JOIN {{ ref('finance_days') }}
     USING (date_date)
 ORDER BY date_date DESC