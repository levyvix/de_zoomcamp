with
    source as (select * from {{ source("ny_taxi", "yellow_tripdata") }}),
    tripdata as (
        select *, row_number() over (partition by vendorid, tpep_pickup_datetime) as rn
        from source
        where vendorid is not null
    )

select
    {{ dbt_utils.generate_surrogate_key(["vendorid", "tpep_pickup_datetime"]) }}
    as tripid,
    {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} as vendorid,
    {{ dbt.safe_cast("ratecodeid", api.Column.translate_type("integer")) }}
    as ratecodeid,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }}
    as pulocationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }}
    as dolocationid,

    -- timestamps
    {{ dbt.safe_cast("tpep_pickup_datetime", api.Column.translate_type("timestamp")) }}
    as tpep_pickup_datetime,
    {{ dbt.safe_cast("tpep_dropoff_datetime", api.Column.translate_type("timestamp")) }}
    as tpep_dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }}
    as passenger_count,
    cast(trip_distance as numeric) as trip_distance,

    -- yellow cabs are street-hail
    1 as trip_type,

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    coalesce(
        {{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }}, 0
    ) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description

from
    tripdata cast(tip_amount as numeric) as tip_amount,
    cast(mta_tax as numeric) as mta_tax,
where rn = 1 cast(tolls_amount as numeric) as tolls_amount,
