Airbnb Data Pipeline: Postgres to S3 using Apache Airflow
Overview

This project implements an end-to-end data pipeline using Apache Airflow that ingests Airbnb listing data, processes it, stores it in PostgreSQL, and finally exports it to Amazon S3 in CSV format.

The pipeline demonstrates core data engineering concepts such as data ingestion, transformation, database loading, custom Airflow operators, and cloud storage integration.

Architecture Flow

Download Airbnb listings CSV from Inside Airbnb

Clean and preprocess the dataset using Pandas

Load the processed data into a PostgreSQL table

Export data from PostgreSQL to Amazon S3 using a custom Airflow operator

Technologies Used

Apache Airflow

Python

PostgreSQL

Amazon S3

Pandas

Requests

DAG Details

DAG ID: airbnb_postgres_to_s3

Schedule: Daily

Catchup: Disabled

Start Date: 13 September 2025

Pipeline Tasks
1. Create Table

Creates a PostgreSQL table for storing Airbnb listing data.
If the table already exists, it is dropped and recreated to ensure schema consistency.

2. Download CSV

Downloads the Airbnb listings CSV file for Amsterdam from the Inside Airbnb public dataset and stores it temporarily on the local filesystem.

3. Preprocess CSV

Selects required columns

Renames inconsistent column names

Cleans price values

Handles missing values

Outputs a cleaned CSV ready for database ingestion

4. Load to PostgreSQL

Uses PostgreSQL COPY command for efficient bulk loading of cleaned data into the database.

5. Upload to S3 (Custom Operator)

Exports data from PostgreSQL into a CSV file and uploads it to an Amazon S3 bucket using a custom Airflow operator.

Custom Operator
PostgresToS3Operator

This operator:

Executes a SQL query on PostgreSQL

Converts the result into a CSV format

Uploads the CSV directly to Amazon S3

This demonstrates how to extend Airflow with custom operators for reusable workflows.

Configuration
Airflow Connections Required
Connection Type	Connection ID
PostgreSQL	airbnb_postgres
AWS S3	aws_s3Bucket

Ensure AWS credentials and PostgreSQL details are properly configured in Airflow.

S3 Output

Bucket: airbnbtos3

Key: exports/listings.csv

How to Run

Set up Apache Airflow

Configure PostgreSQL and AWS S3 connections in Airflow

Place the DAG file inside the Airflow dags/ directory

Start Airflow scheduler and webserver

Enable the DAG from the Airflow UI

Use Case

This project is suitable for:

Learning real-world Airflow pipelines

Understanding ETL workflows

Demonstrating cloud + database integration

Data Engineering internship and portfolio projects

Author

Priyadarshi
B.Tech Student | Aspiring Data Engineer
