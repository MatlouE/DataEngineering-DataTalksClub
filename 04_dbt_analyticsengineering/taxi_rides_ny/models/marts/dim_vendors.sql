with vendors as (
    select distinct vendor_id
    from {{ ref('stg_green_tripdata')}}
)

select vendor_id,
    case 
        when vendor_id = 1 then 'Creative Mobile Technologies, LLC'
        when vendor_id = 2 then 'VeriFone Inc.'
        else 'Unknown'
    end as vendor_name
from vendors