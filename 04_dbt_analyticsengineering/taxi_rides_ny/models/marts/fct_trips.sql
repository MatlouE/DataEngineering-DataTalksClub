with trips as (
    select * from {{ ref('int_trips') }}
)

select * from trips



