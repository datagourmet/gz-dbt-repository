with tab1 as (
    SELECT date_date, ads_cost, ads_impression, ads_clicks
    FROM {{ ref("stg_raw__adwords") }}
    UNION ALL
    SELECT date_date, ads_cost, ads_impression, ads_clicks
    FROM {{ ref("stg_raw__bing") }}
    UNION ALL
    SELECT date_date, ads_cost, ads_impression, ads_clicks
    FROM {{ ref("stg_raw__criteo") }}
    UNION ALL
    SELECT date_date, ads_cost, ads_impression, ads_clicks
    FROM {{ ref("stg_raw__facebook") }}
)

select 
    date_date,
    sum(ads_cost) as ads_cost,
    sum(ads_impression) as ads_impression,
    sum(ads_clicks) as ads_clicks
from 
    tab1
group by 
    date_date
order by 
    date_date desc