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
