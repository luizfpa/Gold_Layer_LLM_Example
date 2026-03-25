import os
import sys
import csv
import duckdb
from datetime import datetime

# Add utils to path
notebooks_dir = os.path.dirname(os.path.abspath(__file__))
utils_path = os.path.join(os.path.dirname(notebooks_dir), 'utils')
sys.path.append(utils_path)
from pipeline_utils import send_email_notification, config

SOURCE_SYSTEM_NAME = "random_user_api"
TABLE_NAME = "bronze_user_data"
DATA_FILE = "users.csv"

def ingest_user_data():
    print(f"Ingesting user data into Bronze layer table: {TABLE_NAME}")

    # Get paths
    data_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', DATA_FILE)
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'tmp', 'duckdb', 'bronze')
    os.makedirs(output_dir, exist_ok=True)
    db_path = os.path.join(output_dir, "bronze.duckdb")

    conn = duckdb.connect(db_path)

    # Drop table if exists to ensure clean state
    conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

    # Create table 
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        gender VARCHAR,
        first_name VARCHAR,
        last_name VARCHAR,
        email VARCHAR,
        phone VARCHAR,
        dob_date VARCHAR,
        dob_age INTEGER,
        location_city VARCHAR,
        location_country VARCHAR,
        registered_date VARCHAR,
        cell VARCHAR,
        ingestion_timestamp TIMESTAMP,
        source_system VARCHAR,
        PRIMARY KEY (email, source_system)
    )
    """)

    # Read CSV
    try:
        with open(data_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        print(f"Error reading raw data: {e}")
        rows = []

    ingestion_timestamp = datetime.now()

    # Insert data (replace if exists based on email and source_system)
    for row in rows:
        # Clean and convert age to integer
        try:
            dob_age = int(row.get('dob_age', 0))
        except:
            dob_age = 0

        conn.execute(f"""
        INSERT INTO {TABLE_NAME} VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        ) ON CONFLICT (email, source_system) DO UPDATE SET
            gender = excluded.gender,
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            phone = excluded.phone,
            dob_date = excluded.dob_date,
            dob_age = excluded.dob_age,
            location_city = excluded.location_city,
            location_country = excluded.location_country,
            registered_date = excluded.registered_date,
            cell = excluded.cell,
            ingestion_timestamp = excluded.ingestion_timestamp,
            source_system = excluded.source_system
        """, (
            row.get('gender'), row.get('first_name'), row.get('last_name'),
            row.get('email'), row.get('phone'), row.get('dob_date'),
            dob_age, row.get('location_city'), row.get('location_country'),
            row.get('registered_date'), row.get('cell'),
            ingestion_timestamp, SOURCE_SYSTEM_NAME
        ))

    count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"Successfully ingested {count} records into Bronze layer.")
    conn.close()

if __name__ == "__main__":
    try:
        ingest_user_data()
        send_email_notification(
            subject=f"Pipeline Success: Bronze User Ingestion for {SOURCE_SYSTEM_NAME}",
            body=f"The Bronze layer user ingestion for {SOURCE_SYSTEM_NAME} completed successfully."
        )
    except Exception as e:
        print(f"Pipeline failed: {e}")
        send_email_notification(
            subject=f"Pipeline Failure: Bronze User Ingestion for {SOURCE_SYSTEM_NAME}",
            body=f"The Bronze layer user ingestion for {SOURCE_SYSTEM_NAME} failed with error: {e}"
        )
        raise
