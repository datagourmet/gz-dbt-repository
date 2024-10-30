 SELECT
     date_date,
     SUM(ads_cost) as ads_cost,
     SUM(impression) as ads_impression,
     CAST (SUM(click) as int64) as ads_clicks
 FROM {{ ref("int_campaigns") }}
 GROUP BY date_date
 ORDER BY date_date DESC