# Module 1 Homework: Docker & SQL

## Environment setup

Create a virtual environment and install required packages (I used `uv`):

```bash
uv init --python=3.13
uv add pandas psycopg2-binary pyarrow sqlalchemy tqdm
uv add --dev jupyter pgcli
```

## Download data

Download the green taxi parquet file and the taxi zones lookup:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

## Run Jupyter notebook

Start Jupyter:

```bash
uv run jupyter notebook
```

My notebook was converted to a standalone script (ingest_data.py) and dockerized; the script and Dockerfile used to build the image (green_taxi_ingest:v001) are included in this repository.

## Docker / docker-compose

I reused the docker-compose setup and built a Docker image for the ingestion script (ingest_data.py). The image is tagged as `green_taxi_ingest:v001`.

Start services:

```bash
docker-compose up -d
```

Build the ingestion image of ingest_data.py:

```bash
docker build -t green_taxi_ingest:v001 .
```

List networks:

```bash
docker network ls
```

Run the ingestion container:

```bash
docker run -it \
  --network=homework_default \
  green_taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=pgdatabase \
  --port=5432 \
  --db=ny_taxi \
  --table1=green_taxi_trips \
  --table2=taxi_zones
```

## Questions

Question 1. Understanding Docker images

Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container.

What's the version of `pip` in the image?

-   25.3
-   24.3.1
-   24.2.1
-   23.3.1

```bash
docker run -it --rm --entrypoint=bash python:3.13
# inside container:
pip --version
# result:
# pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```

Answer: 25.3.

---

## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that pgadmin should use to connect to the postgres database?

```yaml
services:
    db:
        container_name: postgres
        image: postgres:17-alpine
        environment:
            POSTGRES_USER: "postgres"
            POSTGRES_PASSWORD: "postgres"
            POSTGRES_DB: "ny_taxi"
        ports:
            - "5433:5432"
        volumes:
            - vol-pgdata:/var/lib/postgresql/data

    pgadmin:
        container_name: pgadmin
        image: dpage/pgadmin4:latest
        environment:
            PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
            PGADMIN_DEFAULT_PASSWORD: "pgadmin"
        ports:
            - "8080:80"
        volumes:
            - vol-pgadmin_data:/var/lib/pgadmin

volumes:
    vol-pgdata:
        name: vol-pgdata
    vol-pgadmin_data:
        name: vol-pgadmin_data
```

-   postgres:5433
-   localhost:5432
-   db:5433
-   postgres:5432
-   db:5432

If multiple answers are correct, select any

Answer: db:5432

Each service is reachable by its service name, in this case the host is db, and inside a network, containers talk to each other using the container’s internal port, not the host one, hence 5432.

Question 3. Counting short trips

For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?

-   7,853
-   8,007
-   8,254
-   8,421

SQL:

```sql
SELECT count(*) FROM public.green_taxi_trips
WHERE trip_distance <= 1.0
  AND lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01';
```

Answer: 8,007

---

Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles (to exclude data errors).

Use the pick up time for your calculations.

-   2025-11-14
-   2025-11-20
-   2025-11-23
-   2025-11-25

```sql
SELECT DATE(lpep_pickup_datetime) AS pickup_date, trip_distance
FROM public.green_taxi_trips
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 1;
```

Answer: 2025-11-14

---

Question 5. Biggest pickup zone

Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?

-   East Harlem North
-   East Harlem South
-   Morningside Heights
-   Forest Hills

```sql
SELECT tz."Zone", SUM(gtt.total_amount) AS total_amount
FROM public.green_taxi_trips gtt
JOIN public.taxi_zones tz ON gtt."PULocationID" = tz."LocationID"
WHERE DATE(gtt.lpep_pickup_datetime) = '2025-11-18'
GROUP BY tz."Zone"
ORDER BY total_amount DESC
LIMIT 1;
```

Answer: East Harlem North

---

Question 6. Largest tip

For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

Note: it's `tip` , not `trip`. We need the name of the zone, not the ID.

-   JFK Airport
-   Yorkville West
-   East Harlem North
-   LaGuardia Airport

SQL:

```sql
WITH pickup_at_east_harlem_north AS (
  SELECT "DOLocationID", tip_amount
  FROM public.green_taxi_trips gtt
  JOIN public.taxi_zones tz ON gtt."PULocationID" = tz."LocationID"
  WHERE tz."Zone" = 'East Harlem North'
    AND lpep_pickup_datetime >= '2025-11-01'
    AND lpep_pickup_datetime < '2025-12-01'
)
SELECT tz."Zone", p.tip_amount
FROM pickup_at_east_harlem_north p
JOIN public.taxi_zones tz ON p."DOLocationID" = tz."LocationID"
ORDER BY p.tip_amount DESC
LIMIT 1;
```

Answer: Yorkville West

---

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

For setting my GCP credentials I used
```bash
gcloud auth application-default login

gcloud auth application-default set-quota-project "<MY Project ID>"
```

In your VM on GCP/Laptop/GitHub Codespace install Terraform.
Copy the files from the course repo
[here](../../../01-docker-terraform/terraform/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset. My files are in the terraform directory.

Question 7. Terraform Workflow

Which of the following sequences, respectively, describes the workflow for:

1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:

-   terraform import, terraform apply -y, terraform destroy
-   teraform init, terraform plan -auto-apply, terraform rm
-   terraform init, terraform run -auto-approve, terraform destroy
-   terraform init, terraform apply -auto-approve, terraform destroy
-   terraform import, terraform apply -y, terraform rm

Answer: terraform init, terraform apply -auto-approve, terraform destroy