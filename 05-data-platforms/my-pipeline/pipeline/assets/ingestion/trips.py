"""@bruin

name: ingestion.trips
type: python
image: 3.11
connection: taxi_rides_ny_duckdb 

materialization:
  type: table
  strategy: append

#Define output columns (names + types) for metadata, lineage, and quality checks.
#columns:
#  - name: TODO_col1
#    type: TODO_type
#    description: TODO

@bruin"""

import os
import pandas as pd
import requests
from datetime import datetime
import io  

def materialize():
    """
    Ingest NYC Taxi data from the TLC public endpoint.
    """
    # Fetch pipeline variables
    taxi_types = os.getenv("BRUIN_VARS", "{}")
    print(f"Raw BRUIN_VARS: {taxi_types}")
    taxi_types = eval(taxi_types).get("taxi_types", ["yellow"])

    start_date = os.getenv("BRUIN_START_DATE")
    end_date = os.getenv("BRUIN_END_DATE")
    print(f"Start date: {start_date}, End date: {end_date}, Taxi types: {taxi_types}")

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    data_frames = []

    for taxi_type in taxi_types:
        current_date = pd.to_datetime(start_date)
        end_date_dt = pd.to_datetime(end_date)

        while current_date <= end_date_dt:
            year = current_date.year
            month = f"{current_date.month:02d}"

            file_name = f"{taxi_type}_tripdata_{year}-{month}.parquet"
            url = f"{base_url}{file_name}"

            try:
                response = requests.get(url)
                response.raise_for_status()

                # Load data into a DataFrame
                df = pd.read_parquet(io.BytesIO(response.content))  # Use io.BytesIO here
                df["taxi_type"] = taxi_type
                df["extracted_at"] = datetime.utcnow()

                data_frames.append(df)

            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch data for {file_name}: {e}")

            # Increment the month
            current_date += pd.DateOffset(months=1)

    # Concatenate all DataFrames
    if data_frames:
        final_dataframe = pd.concat(data_frames, ignore_index=True)
        return final_dataframe
    else:
        print("No data was fetched.")
        return pd.DataFrame()


