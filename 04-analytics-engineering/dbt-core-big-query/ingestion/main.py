import os
import requests
from google.cloud import storage

GCS_BUCKET = "de-zoomcamp-484622-dbt-taxi-rides-ny"
#GCS_PREFIX = ""
BASE_DIR = os.getcwd()
PREFIX =  "https://d37ci6vzurychx.cloudfront.net"

storage_client = storage.Client.from_service_account_json(
    "../../../credentials/gcp_creds.json"
)
bucket = storage_client.bucket(GCS_BUCKET)


def download_to_local(url):
    filename = os.path.basename(url)
    local_path = os.path.join(BASE_DIR, filename)

    print(f"Downloading {url}")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

    return local_path


def upload_to_gcs(local_path, gcs_path):
    print(f"Uploading gs://{GCS_BUCKET}/{gcs_path}")
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)



def main():
    print("Starting data ingestion process...")
    for taxi_type in ("green", "yellow"):
        for year in (2019, 2020):
            for i in range(1, 13):
                month_str = f"{i:02d}"
                url = f"{PREFIX}/trip-data/{taxi_type}_tripdata_{year}-{month_str}.parquet"
            
                filename = os.path.basename(url)

                local_file = download_to_local(url)

                gcs_object = f"{taxi_type}/{year}/{filename}"
                #gcs_object = filename
                upload_to_gcs(local_file, gcs_object)

                print(f"Uploaded gs://{GCS_BUCKET}/{gcs_object}")

                os.remove(local_file)

    print("Data ingestion completed.")


if __name__ == "__main__":
    main()