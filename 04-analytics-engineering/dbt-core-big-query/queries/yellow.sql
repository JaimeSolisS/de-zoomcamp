CREATE OR REPLACE EXTERNAL TABLE `dbt_taxi_rides_ny.external_yellow_tripdata`
(
  VendorID INT64,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count FLOAT64,
  trip_distance FLOAT64,
  RatecodeID FLOAT64,
  store_and_fwd_flag STRING,
  PULocationID INT64,
  DOLocationID INT64,
  payment_type INT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  congestion_surcharge FLOAT64 
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp-484622-dbt-taxi-rides-ny/yellow/*.parquet']
);

CREATE OR REPLACE TABLE dbt_taxi_rides_ny.yellow_tripdata
PARTITION BY DATE(tpep_pickup_datetime) AS
SELECT *
 FROM dbt_taxi_rides_ny.external_yellow_tripdata;

-- Insert new records into the green_tripdata table
INSERT INTO dbt_taxi_rides_ny.yellow_tripdata
SELECT *
FROM dbt_taxi_rides_ny.external_yellow_tripdata
WHERE DATE(tpep_pickup_datetime) > (
  SELECT MAX(tpep_pickup_datetime) FROM dbt_taxi_rides_ny.yellow_tripdata
);