with source as (
  select * from {{ source('raw', 'yellow_tripdata') }}
)

select * from source