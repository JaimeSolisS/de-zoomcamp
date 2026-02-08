with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
    where dispatching_base_num is not null
),

renamed as (
    select
        cast(dispatching_base_num as string) as dispatching_base_number,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        cast(PUlocationID as integer) as pickup_location_id, --this column is NULL
        cast(DOlocationID as integer) as dropoff_location_id, --this column is NULL
        cast(SR_Flag as string) as sr_flag, --this column is NULL
        cast(Affiliated_base_number as string) as affiliated_base_number
    from source
)

select * from renamed