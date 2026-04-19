with sources as (
    select * from {{source('raw_data', 'yellow_tripdata')}}
),

renamed as (
    select * from sources LIMIT 5
)

select * from renamed