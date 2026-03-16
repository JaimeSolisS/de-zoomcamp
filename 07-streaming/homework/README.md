# Homework

In this homework, we'll practice streaming with Kafka (Redpanda) and PyFlink.

We use Redpanda, a drop-in replacement for Kafka. It implements the same
protocol, so any Kafka client library works with it unchanged.

For this homework we will be using Green Taxi Trip data from October 2025:

- [green_tripdata_2025-10.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet)

## Setup

We'll use the same infrastructure from the [workshop](../../../07-streaming/workshop/).

Follow the setup instructions: build the Docker image, start the services:

```bash
cd 07-streaming/workshop/
docker compose build
docker compose up -d
```

This gives us:

- Redpanda (Kafka-compatible broker) on `localhost:9092`
- Flink Job Manager at http://localhost:8081
- Flink Task Manager
- PostgreSQL on `localhost:5432` (user: `postgres`, password: `postgres`)

If you previously ran the workshop and have old containers/volumes,
do a clean start:

```bash
docker compose down -v
docker compose build
docker compose up -d
```

Note: the container names (like `workshop-redpanda-1`) assume the
directory is called `workshop`. If you renamed it, adjust accordingly.

## Question 1. Redpanda version

Run `rpk version` inside the Redpanda container:

```bash
docker exec -it workshop-redpanda-1 rpk version
```

What version of Redpanda are you running?

**Answer: v25.3.9**

```bash
 docker exec -it workshop-redpanda-1 rpk version
rpk version: v25.3.9
Git ref:     836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
Build date:  2026 Feb 26 07 48 21 Thu
OS/Arch:     linux/amd64
Go version:  go1.24.3

Redpanda Cluster
  node-1  v25.3.9 - 836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
```

## Question 2. Sending data to Redpanda

Create a topic called `green-trips`:

```bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

Now write a producer to send the green taxi data to this topic.

Read the parquet file and keep only these columns:

- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `tip_amount`
- `total_amount`

Convert each row to a dictionary and send it to the `green-trips` topic.
You'll need to handle the datetime columns - convert them to strings
before serializing to JSON.

Measure the time it takes to send the entire dataset and flush:

```python
from time import time

t0 = time()

# send all rows ...

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

How long did it take to send the data?

- 10 seconds
- 60 seconds
- 120 seconds
- 300 seconds

**Answer: 10 seconds**

Steps:

1. Create green Producer & Add green model:

```python
# Different data, different columns
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime','lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
df = pd.read_parquet(url, columns=columns)

df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].astype('str')
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype('str')
...
# Different topic
topic_name = 'green-trips'

# Green Model
for _, row in df.iterrows():
    ride = green_ride_from_row(row)
    producer.send(topic_name, value=ride)
...
```

2. Run the Producer:

```bash
uv run src/producers/green_producer.py
took 12.43 seconds
```

## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the `green-trips` topic
(set `auto_offset_reset='earliest'`).

Count how many trips have a `trip_distance` greater than 5.0 kilometers.

How many trips have `trip_distance` > 5?

- 6506
- 7506
- 8506
- 9506

**Answer: 8506**

Steps:

1. Connect to Postgress and create the table:

```bash
docker compose exec postgres psql -U postgres -d postgres

postgres=# CREATE TABLE green_processed_events (
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION
);
```

2. Create green Consumer with green desearilizer, green topic and green columns

```python
...
topic_name = 'green-trips'
...
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-to-postgres',
    value_deserializer=green_ride_deserializer
)
...
for message in consumer:
    ride = message.value
    cur.execute(
        """INSERT INTO green_processed_events
           (lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID, passenger_count, trip_distance, tip_amount, total_amount)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (ride.lpep_pickup_datetime, ride.lpep_dropoff_datetime, ride.PULocationID, ride.DOLocationID, ride.passenger_count, ride.trip_distance, ride.tip_amount, ride.total_amount)
    )
...
```

3. Run the Consumer:

```bash
 uv run src/consumers/green_consumer.py
```

3. Query the table, check that the number of records matches with source data, check the data and query to answer the question:

```bash
postgres=# select count(*) from green_processed_events;
 count
-------
 49416
(1 row)

postgres=# select * from green_processed_events limit 5;
 lpep_pickup_datetime | lpep_dropoff_datetime | pulocationid | dolocationid | passenger_count | trip_distance | tip_amount | total_amount
----------------------+-----------------------+--------------+--------------+-----------------+---------------+------------+--------------
 2025-10-01 00:21:47  | 2025-10-01 00:24:37   |          247 |           69 |               1 |           0.7 |        1.7 |           10
 2025-10-01 00:14:03  | 2025-10-01 00:24:14   |           66 |           25 |               1 |          1.61 |       2.78 |        16.68
 2025-10-01 00:16:44  | 2025-10-01 00:16:47   |          244 |          244 |               1 |             0 |        2.2 |         13.2
 2025-10-01 00:07:36  | 2025-10-01 00:32:14   |           95 |          170 |               1 |         10.37 |      11.31 |        67.85
 2025-09-30 21:10:29  | 2025-09-30 21:22:30   |           82 |          138 |               1 |          4.07 |       6.82 |        34.12
(5 rows)

postgres=# select count(*) from green_processed_events where trip_distance > 5;
 count
-------
  8506
(1 row)
```

## Part 2: PyFlink (Questions 4-6)

For the PyFlink questions, you'll adapt the workshop code to work with
the green taxi data. The key differences from the workshop:

- Topic name: `green-trips` (instead of `rides`)
- Datetime columns use `lpep_` prefix (instead of `tpep_`)
- You'll need to handle timestamps as strings (not epoch milliseconds)

You can convert string timestamps to Flink timestamps in your source DDL:

```sql
lpep_pickup_datetime VARCHAR,
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

Before running the Flink jobs, create the necessary PostgreSQL tables
for your results.

Important notes for the Flink jobs:

- Place your job files in `workshop/src/job/` - this directory is
  mounted into the Flink containers at `/opt/src/job/`
- Submit jobs with:
  `docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/your_job.py`
- The `green-trips` topic has 1 partition, so set parallelism to 1
  in your Flink jobs (`env.set_parallelism(1)`). With higher parallelism,
  idle consumer subtasks prevent the watermark from advancing.
- Flink streaming jobs run continuously. Let the job run for a minute
  or two until results appear in PostgreSQL, then query the results.
  You can cancel the job from the Flink UI at http://localhost:8081
- If you sent data to the topic multiple times, delete and recreate
  the topic to avoid duplicates:
  `docker exec -it workshop-redpanda-1 rpk topic delete green-trips`

## Question 4. Tumbling window - pickup location

Create a Flink job that reads from `green-trips` and uses a 5-minute
tumbling window to count trips per `PULocationID`.

Write the results to a PostgreSQL table with columns:
`window_start`, `PULocationID`, `num_trips`.

After the job processes all data, query the results:

```sql
SELECT PULocationID, num_trips
FROM <your_table>
ORDER BY num_trips DESC
LIMIT 3;
```

Which `PULocationID` had the most trips in a single 5-minute window?

- 42
- 74
- 75
- 166

**Answer: 74**

Steps:

1. Create the table

```bash
$ docker compose exec postgres psql -U postgres -d postgres

CREATE TABLE aggregated_trips_5_min (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);
```

2. Create the job. Using `aggregation_job` as base, we have to change:

- Topic and table

```python
table_name = "green_events_1"

'topic' = 'green-trips'
```

- Source schema

```pthon
CREATE TABLE green_events_1 (
    lpep_pickup_datetime VARCHAR,
    lpep_dropoff_datetime VARCHAR,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    tip_amount DOUBLE,
    total_amount DOUBLE
)
```

- Timestamp Parsing

```python
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss')
```

- Window Size

```python
TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
```

- Modify Sink table schema

```python
CREATE TABLE aggregated_trips_5_min (
    window_start TIMESTAMP(3),
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
)
```

- Update aggregation query

```python
SELECT
    window_start,
    PULocationID,
    COUNT(*) AS num_trips
```

The rest of the Flink pipeline structure stays the same.

run job

```
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/tumbling_window_pickup_location_green_job.py \
    --pyFiles /opt/src -d
```

Query

```bash
postgres=# SELECT PULocationID, num_trips FROM aggregated_trips_5_min ORDER BY num_trips DESC LIMIT 3;
 pulocationid | num_trips
--------------+-----------
           74 |        15
           74 |        14
           74 |        13
(3 rows)
```

## Question 5. Session window - longest streak

Create another Flink job that uses a session window with a 5-minute gap
on `PULocationID`, using `lpep_pickup_datetime` as the event time
with a 5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other.
When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the `PULocationID`
with the longest session (most trips in a single session).

How many trips were in the longest session?

- 12
- 31
- 51
- 81

**Answer: 81**

Steps:

1. create table

```
CREATE TABLE IF NOT EXISTS session_trips (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);
```

2. Create the job. For this we switch from a 5-minute tumbling window to a 5-minute session window.

- Change the table name of events.

```python
table_name = "green_events_2"
```

- Source schema stays the same

```python
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

- Sink table and function name changes

```python
def create_session_sink(t_env):
    table_name = 'session_trips'
```

- Add `window_end` to the sink schema. The primary key changes too, because now each result row is uniquely identified by start, end, and pickup location.

```python
CREATE TABLE session_trips (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID) NOT ENFORCED
)
```

- Change the query from `TUMBLE` to `SESSION`

```python
FROM TABLE(
    SESSION(TABLE {source_table} PARTITION BY PULocationID, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, PULocationID;
```

- Add `window_end` to the select and group by start, end and location

```python
SELECT
            window_start,
            window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(TABLE {source_table} PARTITION BY PULocationID, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID;
```

3. Run the job

```
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/session_window_green_job.py \
    --pyFiles /opt/src -d
```

4. Query table to get the results

```
SELECT PULocationID, window_start, window_end, num_trips,
       (window_end - window_start) AS session_duration
FROM session_trips
ORDER BY num_trips DESC
LIMIT 5;
 pulocationid |    window_start     |     window_end      | num_trips | session_duration
--------------+---------------------+---------------------+-----------+------------------
           74 | 2025-10-08 06:46:14 | 2025-10-08 08:27:40 |        81 | 01:41:26
           74 | 2025-10-01 06:52:23 | 2025-10-01 08:23:33 |        72 | 01:31:10
           74 | 2025-10-22 06:58:31 | 2025-10-22 08:25:04 |        71 | 01:26:33
           74 | 2025-10-27 06:56:30 | 2025-10-27 08:24:09 |        70 | 01:27:39
           74 | 2025-10-21 06:54:16 | 2025-10-21 08:26:24 |        69 | 01:32:08
(5 rows)
```

> [!NOTE]
> In Question 4 we put every event into fixed 5-minute buckets.  
> In Question 5 w group events into sessions, where a session continues as long as events for the same PULocationID keep arriving without a gap larger than 5 minutes.  
> Tumbling window = fixed intervals like 10:00–10:05, 10:05–10:10.  
> Session window = dynamic intervals based on activity, like 10:02–10:11.

## Question 6. Tumbling window - largest tip

Create a Flink job that uses a 1-hour tumbling window to compute the
total `tip_amount` per hour (across all locations).

Which hour had the highest total tip amount?

- 2025-10-01 18:00:00
- 2025-10-16 18:00:00
- 2025-10-22 08:00:00
- 2025-10-30 16:00:00

**Answer: 2025-10-16 18:00:00**

Steps:

1. Create the table

```
CREATE TABLE IF NOT EXISTS tip_per_hour (
    window_start TIMESTAMP,
    total_tip DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);
```

2. Create the job.

- Change sink function and sink table name

```python
def create_tip_sink(t_env):
    table_name = 'tip_per_hour'
```

- Change sink schema to

```python
 CREATE TABLE tip_per_hour (
    window_start TIMESTAMP(3),
    total_tip DOUBLE,
    PRIMARY KEY (window_start) NOT ENFORCED
)
```

- Change the aggregation query

```python
SELECT
      window_start,
      SUM(tip_amount) AS total_tip
  FROM TABLE(
      TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
  )
  GROUP BY window_start;
```

3. Submit job

```
docker compose exec jobmanager ./bin/flink run \
 -py /opt/src/job/tumbling_window_largest_tip_job.py \
 --pyFiles /opt/src -d
```

4. Query table to get the results

```
postgres=# select * from tip_per_hour order by total_tip DESC limit 3;
    window_start     |     total_tip
---------------------+-------------------
 2025-10-16 18:00:00 | 524.9599999999998
 2025-10-30 16:00:00 |             507.1
 2025-10-10 17:00:00 | 499.6000000000002
(3 rows)
```
