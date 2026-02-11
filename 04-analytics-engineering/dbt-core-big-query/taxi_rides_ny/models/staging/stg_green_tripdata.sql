with source as (
  select * from {{ source('raw', 'green_tripdata') }}
)

select * from source