<<<<<<< HEAD
with monthly_data as (
    select 
        date_trunc('month', date_date) as datemonth,
        sum(ads_cost) as ads_cost,
        sum(ads_impression) as ads_impression,
        sum(ads_clicks) as ads_clicks,
        sum(quantity) as quantity,
        sum(revenue) as revenue,
        sum(purchase_cost) as purchase_cost,
        sum(margin) as margin,
        sum(shipping_fee) as shipping_fee,
        sum(log_cost) as log_cost,
        sum(ship_cost) as ship_cost
    from {{ ref('int_campaigns') }}
    group by datemonth
)

select
    datemonth,
    sum(margin) as ads_margin,
    case when sum(quantity) = 0 then 0 else sum(revenue) / sum(quantity) end as average_basket,
    sum(margin + shipping_fee - log_cost - ship_cost) as operational_margin,
    ads_cost,
    ads_impression,
    ads_clicks,
    quantity,
    revenue,
    purchase_cost,
    margin,
    shipping_fee,
    log_cost,
    ship_cost
from
    monthly_data
group by
    datemonth,
    ads_cost,
    ads_impression,
    ads_clicks,
    quantity,
    revenue,
    purchase_cost,
    margin,
    shipping_fee,
    log_cost,
    ship_cost
order by
    datemonth desc
FULL OUTER JOIN(ref('finance_days'))
=======
 SELECT
     date_trunc(date_date, MONTH) AS datemonth,
     SUM(operational_margin - ads_cost) AS ads_margin,
     ROUND(SUM(average_basket),2) AS average_basket,
     SUM(operational_margin) AS operational_margin,
     SUM(ads_cost) AS ads_cost,
     SUM(ads_impression) AS ads_impression,
     SUM(ads_clicks) AS ads_clicks,
     SUM(quantity) AS quantity,
     SUM(revenue) AS revenue,
     SUM(purchase_cost) AS purchase_cost,
     SUM(margin) AS margin,
     SUM(shipping_fee) AS shipping_fee,
     SUM(logcost) AS logcost,
     SUM(ship_cost) AS ship_cost,
 FROM {{ ref('finance_campaigns_day') }}
 GROUP BY datemonth
 ORDER BY datemonth desc
>>>>>>> 8ce6da639d67f65ead023e2f7dace14230bc6b18
