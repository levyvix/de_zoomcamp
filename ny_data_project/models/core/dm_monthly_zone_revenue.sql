{{config(materialized='table')}}

with trips_data as (
	select * from {{ref("fact_trips")}}
)

select 
pickup_zone as revenue_zone,
{{ dbt.date_trunc('month',  dbt.safe_cast("pickup_datetime", api.Column.translate_type("string")) ) }} as month,
service_type,

-- revenue calc
sum(fare_amount) as revenue_monthly_fare,
sum(extra) as revenue_monthly_extra,
sum(mta_tax) as revenue_monthly_ma_tax,
sum(tip_amount) as revenue_monthly_tip_amount,
sum(tolls_amount) as revenue_monthly_tolls_amount,
sum(ehail_fee) as revenue_monthly_ehail_fee,
sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
sum(total_amount) as revenue_monthly_total_amount,

-- additional calc
count(tripid) as total_monthly_trips,
avg(passenger_count) avg_monthly_passenger_count,
avg(trip_distance) as avg_monthly_trip_distance 
from trips_data
group by 1,2,3