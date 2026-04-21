with t as (
    select * from {{ref('int_trips')}}
)

select count(trip_id) from t