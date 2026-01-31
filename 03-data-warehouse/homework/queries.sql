-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `taxi_rides_ny.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp-484622-ny-taxi/yellow_tripdata_2024-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE taxi_rides_ny.yellow_tripdata_non_partitioned AS
SELECT * FROM taxi_rides_ny.external_yellow_tripdata;

-- count the distinct number of PULocationIDs for the entire dataset on both tables
SELECT COUNT(DISTINCT PULocationID) 
FROM `taxi_rides_ny.external_yellow_tripdata` 

SELECT COUNT(DISTINCT PULocationID) 
FROM `taxi_rides_ny.yellow_tripdata_non_partitioned` 

-- retrieve the PULocationID from the materialized table in BigQuery. 
SELECT  PULocationID
FROM `taxi_rides_ny.yellow_tripdata_non_partitioned` 
-- retrieve the PULocationID and DOLocationID on the same table. 
SELECT  PULocationID, DOLocationID
FROM `taxi_rides_ny.yellow_tripdata_non_partitioned` 

--How many records have a fare_amount of 0?
SELECT count(*) 
FROM `taxi_rides_ny.yellow_tripdata_non_partitioned`
WHERE fare_amount = 0

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE taxi_rides_ny.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi_rides_ny.external_yellow_tripdata;

-- retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 
SELECT DISTINCT VendorID
FROM `taxi_rides_ny.yellow_tripdata_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM `taxi_rides_ny.yellow_tripdata_partitioned_clustered`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15' ;

-- Write a SELECT count(*) query FROM the materialized table you created
SELECT COUNT(*) 
FROM `dtaxi_rides_ny.yellow_tripdata_partitioned_clustered` 