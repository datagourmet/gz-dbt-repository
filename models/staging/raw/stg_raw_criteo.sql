with 

source as (

    select * from {{ source('raw', 'criteo') }}

),

renamed as (

    select
        date_date,
        paid_source,
        campaign_key,
        campgn_name AS camapign_name,
        ads cost,
        impression,
        click

    from source

)

select * from renamed
