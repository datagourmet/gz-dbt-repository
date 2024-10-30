WITH source AS (
    SELECT * 
    FROM {{ source('raw', 'facebook') }}
),

renamed AS (
    SELECT
        date_date,
        paid_source,
        campaign_key,
        camPGN_name AS campaign_name,              
        CAST(ads_cost AS FLOAT64) AS ads_cost,      
        impression,
        click
    FROM source
)

SELECT * FROM renamed

