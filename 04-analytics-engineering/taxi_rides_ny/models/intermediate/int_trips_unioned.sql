with yellow as (
    select * from {{ ref('stg_yellow_tripdata') }}
), 

green as (
    select * from {{ ref('stg_green_tripdata') }}
),   

unioned as (
    select * from yellow
    union all
    select * from green
)

select * from unioned