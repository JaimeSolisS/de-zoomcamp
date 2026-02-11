CREATE OR REPLACE EXTERNAL TABLE `dbt_taxi_rides_ny.external_green_tripdata`
(
  VendorID INT64,
  lpep_pickup_datetime TIMESTAMP,
  lpep_dropoff_datetime TIMESTAMP,
  store_and_fwd_flag STRING,
  RatecodeID FLOAT64,
  PULocationID INT64,
  DOLocationID INT64,
  passenger_count FLOAT64,
  trip_distance FLOAT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  payment_type FLOAT64,
  trip_type FLOAT64,
  congestion_surcharge FLOAT64 
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp-484622-dbt-taxi-rides-ny/green/*.parquet']
);

CREATE OR REPLACE TABLE dbt_taxi_rides_ny.green_tripdata
PARTITION BY DATE(lpep_pickup_datetime) AS
SELECT *
 FROM dbt_taxi_rides_ny.external_green_tripdata;

-- Insert new records into the green_tripdata table
INSERT INTO dbt_taxi_rides_ny.green_tripdata
SELECT *
FROM dbt_taxi_rides_ny.external_green_tripdata
WHERE DATE(lpep_pickup_datetime) > (
  SELECT MAX(lpep_pickup_datetime) FROM dbt_taxi_rides_ny.green_tripdata
);