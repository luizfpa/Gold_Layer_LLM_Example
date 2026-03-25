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

SOURCE_SYSTEM_NAME = "world_bank_api"
TABLE_NAME = "bronze_population_data"
DATA_FILE = "population.csv"

def ingest_population_data():
    print(f"Ingesting population data into Bronze layer table: {TABLE_NAME}")

    # Get paths
    data_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', DATA_FILE)
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'tmp', 'duckdb', 'bronze')
    os.makedirs(output_dir, exist_ok=True)
    db_path = os.path.join(output_dir, "bronze.duckdb")

    conn = duckdb.connect(db_path)

    # Drop table if exists
    conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

    # Create table 
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        country VARCHAR,
        year VARCHAR,
        value BIGINT,
        ingestion_timestamp TIMESTAMP,
        source_system VARCHAR,
        PRIMARY KEY (country, year, source_system)
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

    # Insert data
    for row in rows:
        try:
            value = int(row.get('value', 0))
        except:
            value = 0

        conn.execute(f"""
        INSERT INTO {TABLE_NAME} VALUES (
            ?, ?, ?, ?, ?
        ) ON CONFLICT (country, year, source_system) DO UPDATE SET
            value = excluded.value,
            ingestion_timestamp = excluded.ingestion_timestamp,
            source_system = excluded.source_system
        """, (
            row.get('country'), row.get('year'), value,
            ingestion_timestamp, SOURCE_SYSTEM_NAME
        ))

    count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"Successfully ingested {count} records into Bronze layer.")
    conn.close()

if __name__ == "__main__":
    try:
        ingest_population_data()
        send_email_notification(
            subject=f"Pipeline Success: Bronze Population Ingestion for {SOURCE_SYSTEM_NAME}",
            body=f"The Bronze layer population ingestion for {SOURCE_SYSTEM_NAME} completed successfully."
        )
    except Exception as e:
        print(f"Pipeline failed: {e}")
        send_email_notification(
            subject=f"Pipeline Failure: Bronze Population Ingestion for {SOURCE_SYSTEM_NAME}",
            body=f"The Bronze layer population ingestion for {SOURCE_SYSTEM_NAME} failed with error: {e}"
        )
        raise
