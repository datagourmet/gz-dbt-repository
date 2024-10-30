with tab1 as (SELECT *
FROM {{ ref("stg_raw__adwords") }}
UNION ALL
SELECT *
FROM {{ ref("stg_raw__bing") }}
UNION ALL
SELECT *
FROM {{ ref("stg_raw__criteo") }}
UNION ALL
SELECT *
FROM {{ ref("stg_raw__facebook") }})
select *
from tab1
order by date_date