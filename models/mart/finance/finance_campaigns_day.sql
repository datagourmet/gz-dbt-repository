with orders_data as (
    select
        date_date,
        orders_id,
        revenue,
        quantity,
        purchase_cost,
        shipping_fee,
        operational_margin
    from {{ ref('int_orders_operational') }}
),

daily_summary as (
    select
        date_trunc('month', date_date) as datemonth,
        count(distinct orders_id) as total_transactions,
        sum(revenue) as total_revenue,
        case 
            when count(distinct orders_id) > 0 then sum(revenue) / count(distinct orders_id)
            else 0
        end as average_basket,
        sum(operational_margin) as total_operational_margin,
        sum(purchase_cost) as total_purchase_cost,
        sum(shipping_fee) as total_shipping_fees,
        sum(quantity) as total_quantity_sold
    from orders_data
    group by date_trunc('month', date_date)
),

monthly_data as (
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
    coalesce(daily_summary.datemonth, monthly_data.datemonth) as datemonth,
    daily_summary.total_transactions,
    daily_summary.total_revenue,
    daily_summary.average_basket as daily_average_basket,
    daily_summary.total_operational_margin,
    daily_summary.total_purchase_cost,
    daily_summary.total_shipping_fees,
    daily_summary.total_quantity_sold,
    
    monthly_data.ads_cost,
    monthly_data.ads_impression,
    monthly_data.ads_clicks,
    monthly_data.quantity as monthly_quantity,
    monthly_data.revenue as monthly_revenue,
    monthly_data.purchase_cost as monthly_purchase_cost,
    monthly_data.margin as ads_margin,
    case 
        when monthly_data.quantity = 0 then 0 
        else monthly_data.revenue / monthly_data.quantity 
    end as monthly_average_basket,
    sum(monthly_data.margin + monthly_data.shipping_fee - monthly_data.log_cost - monthly_data.ship_cost) as monthly_operational_margin,
    monthly_data.shipping_fee,
    monthly_data.log_cost,
    monthly_data.ship_cost
from
    daily_summary
full outer join
    monthly_data
on
    daily_summary.datemonth = monthly_data.datemonth
order by
    datemonth desc
