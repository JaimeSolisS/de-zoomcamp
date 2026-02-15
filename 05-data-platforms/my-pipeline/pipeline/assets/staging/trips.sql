/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table

custom_checks:
  - name: row_count_positive
    description: Ensures the table is not empty
    query: SELECT COUNT(*) > 0 FROM staging.trips
    value: 1

@bruin */
WITH cleaned_data AS (
    SELECT
        CONCAT(CAST(lpep_pickup_datetime AS VARCHAR), taxi_type ) AS trip_id,  -- Assuming a unique identifier exists
        lpep_pickup_datetime AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        payment_type,
        taxi_type,
        extracted_at
    FROM ingestion.trips
    WHERE pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND trip_distance >= 0
      AND fare_amount >= 0
      AND total_amount >= 0
),

deduplicated_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id  -- Deduplicate based on unique trip_id
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM cleaned_data
),

final_data AS (
    SELECT
        d.*
    FROM deduplicated_data d
    WHERE d.row_num = 1
)

SELECT
    f.*,
    p.payment_type_name
FROM final_data f
LEFT JOIN ingestion.payment_lookup p
    ON f.payment_type = p.payment_type_id
WHERE f.pickup_datetime >= '{{ start_datetime }}'
  AND f.pickup_datetime < '{{ end_datetime }}';