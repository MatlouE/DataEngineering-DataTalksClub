with with_surrogate_key as (
    select
        {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime', 'pickup_location_id' ])}} as trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type, fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type, service_type
        -- List all other columns explicitly instead of using *
    from {{ ref('int_trips_unioned') }}
),

with l as (
    select * from with_surrogate_key
    qualify row_number() over( partition by vendor_id, pickup_datetime, pickup_location_id order by pickup_datetime asc) = 1
),

payment_lookup as (
    select * from {{ ref('payment_type_lookup')}}
),

final as (
    select l.*,
    -- add payment_description from payment_lookup
    coalesce(payment_lookup.payment_description, 'Unknown')
    from l
    left join payment_lookup
    on l.payment_type = payment_lookup.payment_type
)



