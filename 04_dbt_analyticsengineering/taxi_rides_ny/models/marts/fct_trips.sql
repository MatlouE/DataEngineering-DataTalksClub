with trips as (
    select * from {{ ref('int_trips_unioned') }}
)

select * from trips



