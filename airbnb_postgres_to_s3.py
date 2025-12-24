import csv
import os
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    "owner": "Priyadarshi",
    "depends_on_past": False,
}

listing_dates = ["2025-06-09"]

OUTPUT_DIR = "/opt/airflow/data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def download_csv():
    listing_url_template = (
        "https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/{date}/visualisations/listings.csv"
    )
    for date in listing_dates:
        url = listing_url_template.format(date=date)
        response = requests.get(url)
        if response.status_code == 200:
            file_path = os.path.join(OUTPUT_DIR, f"user-{date}.csv")
            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"✅ File saved at {file_path}")
        else:
            print(f"❌ Error downloading {url}")

def preprocess_csv():
    for data in listing_dates:
        input_path = os.path.join(OUTPUT_DIR, f"user-{data}.csv")
        output_path = os.path.join(OUTPUT_DIR, f"user-{data}.csv")
        df = pd.read_csv(input_path)
        df.fillna("", inplace=True)
        df.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)

with DAG(
    dag_id="airbnb_postgres_to_s3_again",   # keep same ID if you want to overwrite old one
    default_args=default_args,
    start_date=datetime(2025, 9, 13),
    schedule=None,
    catchup=False,
) as dag:

    download_csv_task = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
    )

    preprocess_csv_task = PythonOperator(
        task_id="preprocess_csv",
        python_callable=preprocess_csv,
    )

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="airbnb_postgres",
        sql="""
        DROP TABLE IF EXISTS listings;
        CREATE TABLE IF NOT EXISTS listings (
            id BIGINT,
            name TEXT,
            host_id INTEGER,
            host_name VARCHAR(100),
            neighbourhood_group VARCHAR(100),
            neighbourhood VARCHAR(100),
            latitude NUMERIC(18,16),
            longitude NUMERIC(18,16),
            room_type VARCHAR(100),
            price VARCHAR(100),
            minimum_nights INTEGER,
            number_of_reviews INTEGER,
            last_review VARCHAR(100),
            reviews_per_month VARCHAR(100),
            calculated_host_listing_count INTEGER,
            availability_365 INTEGER,
            number_of_reviews_ltm INTEGER,
            license VARCHAR(100)
        );
        """,
    )

    download_csv_task >> preprocess_csv_task >> create_table
