#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "store_and_fwd_flag": "string",
    "RatecodeID": "Int64",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "ehail_fee":    "float64",
    "payment_type": "Int64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "payment_type": "Int64",
    "trip_type": "Int64",
    "congestion_surcharge": "float64",
    "cbd_congestion_fee"  : "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=11, type=int, help='Month of the data')
@click.option('--table1', default='green_taxi_trips', help='Target table name')
@click.option('--table2', default='taxi_zones', help='Target table name')
def run(user, password, host, port, db, year, month, table1, table2):
    """Ingest NYC taxi data into PostgreSQL database."""
    prefix_trips = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    year = 2025
    month = 11
    prefix_zones = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    trips = pd.read_parquet(f'{prefix_trips}/green_tripdata_{year}-{month}.parquet')

    zones = pd.read_csv(f"{prefix_zones}/taxi_zone_lookup.csv")
    trips = trips.astype(dtype)

    trips.head(n=0).to_sql(name=table1, con=engine, if_exists='replace')
    zones.head(n=0).to_sql(name=table2, con=engine, if_exists='replace')

    # because we have few rows in both tables, I decided to append in just one step, else we can use chunksize parameter
    trips.to_sql(name=table1, con=engine, if_exists='append')
    zones.to_sql(name=table2, con=engine, if_exists='append')

if __name__ == '__main__':
    run()