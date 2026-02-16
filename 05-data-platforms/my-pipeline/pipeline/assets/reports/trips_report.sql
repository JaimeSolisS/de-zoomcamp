/* @bruin
name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table

custom_checks:
  - name: row_count_positive
    description: Ensures the table is not empty
    query: SELECT COUNT(*) > 0 FROM reports.trips_report
    value: 1

@bruin */

WITH aggregated_data AS (
    SELECT
        DATE_TRUNC('day', pickup_datetime) AS trip_date,
        taxi_type,
        payment_type_name,
        COUNT(*) AS total_trips,
        SUM(passenger_count) AS total_passengers,
        SUM(trip_distance) AS total_distance,
        SUM(fare_amount) AS total_fare,
        SUM(tip_amount) AS total_tips,
        SUM(total_amount) AS total_revenue
    FROM staging.trips
    GROUP BY 1, 2, 3
)

SELECT
    trip_date,
    taxi_type,
    payment_type_name,
    total_trips,
    total_passengers,
    total_distance,
    total_fare,
    total_tips,
    total_revenue
FROM aggregated_data
ORDER BY trip_date, taxi_type, payment_type_name;
