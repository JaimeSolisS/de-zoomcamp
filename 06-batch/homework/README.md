# Module 6 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2025-11 data from the official website:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```

## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/06-batch/setup/pyspark.md)

**Answer: 4.1.1**

## Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB

**Answer: 25MB**

```py
df = spark.read.parquet("data/yellow_tripdata_2025-11.parquet")

df.repartition(4) \
  .write \
  .parquet("data/pq/", mode="overwrite")
```

```bash
%ls -l pq
otal 205312
-rw-r--r--  1        0 Mar  2 19:02 _SUCCESS
-rw-r--r--  1 25597759 Mar  2 19:02 part-00000-41ca608f-8933-43a3-86dc-63e069eb8e11-c000.snappy.parquet
-rw-r--r--  1 25583585 Mar  2 19:02 part-00001-41ca608f-8933-43a3-86dc-63e069eb8e11-c000.snappy.parquet
-rw-r--r--  1 25611777 Mar  2 19:02 part-00002-41ca608f-8933-43a3-86dc-63e069eb8e11-c000.snappy.parquet
-rw-r--r--  1 25616300 Mar  2 19:02 part-00003-41ca608f-8933-43a3-86dc-63e069eb8e11-c000.snappy.parquet
```

## Question 3: Count records

How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.

- 62,610
- 102,340
- 162,604
- 225,768

**Answer: 162,604**

```py
from pyspark.sql import functions as F
df3 = df.filter(F.col("tpep_pickup_datetime").between("2025-11-15 00:00:00", "2025-11-15 23:59:59"))
df3.count()
```

```
Out: 162604
```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 22.7
- 58.2
- 90.6
- 134.5

**Answer: 90.6**

```py
df.withColumn("trip_duration_hours", (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600.0)\
    .select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_duration_hours")\
    .orderBy(F.desc("trip_duration_hours")) \
    .show(truncate=False)
```

```
Out:
+--------------------+---------------------+-------------------+
|tpep_pickup_datetime|tpep_dropoff_datetime|trip_duration_hours|
+--------------------+---------------------+-------------------+
|2025-11-26 20:22:12 |2025-11-30 15:01:00  |90.64666666666666  |
|2025-11-27 04:22:41 |2025-11-30 09:19:35  |76.94833333333334  |
|2025-11-03 10:42:55 |2025-11-06 14:55:45  |76.21388888888889  |
|2025-11-07 11:23:22 |2025-11-10 08:40:41  |69.28861111111111  |
|2025-11-18 17:12:47 |2025-11-21 12:17:37  |67.08055555555555  |
+--------------------+---------------------+-------------------+
```

## Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

**Answer: 4040**

## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

If multiple answers are correct, select any

**Answer: Governor's Island/Ellis Island/Liberty Island**

```py
df_zones = spark.read.option("header", "true").csv("data/taxi_zone_lookup.csv")
df.createOrReplaceTempView("yellow")
df_zones.createOrReplaceTempView("zones")

spark.sql("""
        SELECT z.Zone, count(*) as Trips
        FROM yellow y
        JOIN zones z on y.PULocationID=z.LocationID
        GROUP BY 1
        ORDER BY count(*)

"""
).show(truncate=False)
```

```
Out:
+---------------------------------------------+-----+
|Zone                                         |Trips|
+---------------------------------------------+-----+
|Governor's Island/Ellis Island/Liberty Island|1    |
|Eltingville/Annadale/Prince's Bay            |1    |
|Arden Heights                                |1    |
|Port Richmond                                |3    |
|Rikers Island                                |4    |
+---------------------------------------------+-----+
```
