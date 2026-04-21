with vendors as (
    select distinct vendor_id
    from {{ ref('stg_green_tripdata')}}
)

select vendor_id,
    
    {{ get_vendor_data('vendor_id')}} as vendor_name
from vendors
