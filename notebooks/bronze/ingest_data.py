import os
import duckdb
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BRONZE_LAYER_PATH = "./tmp/duckdb/bronze"
BRONZE_TABLE_NAME = "bronze_raw_data"

def get_db_path(table_name):
    os.makedirs(BRONZE_LAYER_PATH, exist_ok=True)
    return os.path.join(BRONZE_LAYER_PATH, f"{table_name}.duckdb")

def create_table_if_not_exists():
    db_path = get_db_path(BRONZE_TABLE_NAME)
    conn = duckdb.connect(db_path)
    
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE_NAME} (
        id INTEGER,
        name VARCHAR,
        value VARCHAR,
        timestamp VARCHAR,
        ingestion_timestamp TIMESTAMP,
        source_system VARCHAR
    )
    """)
    conn.close()
    logging.info(f"Table '{BRONZE_TABLE_NAME}' ensured at '{db_path}'")

def simulate_data_ingestion():
    logging.info("Simulating data ingestion...")
    data = [
        (1, "ProductA", "100", "2023-01-01 10:00:00"),
        (2, "ProductB", "250", "2023-01-01 10:05:00"),
        (3, "ProductC", "120", "2023-01-01 10:10:00"),
        (4, "ProductA", "110", "2023-01-01 10:15:00"),
        (5, "ProductD", "300", "2023-01-01 10:20:00"),
        (1, "ProductA", "105", "2023-01-01 10:25:00")
    ]
    return data

def ingest_to_bronze(data):
    db_path = get_db_path(BRONZE_TABLE_NAME)
    conn = duckdb.connect(db_path)
    
    ingestion_ts = datetime.now()
    
    logging.info(f"Ingesting {len(data)} records into Bronze layer table: {BRONZE_TABLE_NAME}")
    
    for row in data:
        conn.execute(f"""
        INSERT INTO {BRONZE_TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?)
        """, (row[0], row[1], row[2], row[3], ingestion_ts, "simulated_source"))
    
    result = conn.execute(f"SELECT COUNT(*) FROM {BRONZE_TABLE_NAME}").fetchone()
    logging.info(f"Successfully ingested {result[0]} records into Bronze layer.")
    
    conn.close()

def send_email_notification(subject: str, body: str):
    logging.info(f"""
    --- EMAIL NOTIFICATION ---
    To: admin@fabric-pipeline.com
    Subject: {subject}

    {body}
    --------------------------
    """)

if __name__ == "__main__":
    try:
        create_table_if_not_exists()
        data = simulate_data_ingestion()
        ingest_to_bronze(data)
        send_email_notification(
            subject=f"Pipeline Success: Bronze Ingestion for simulated_source",
            body=f"The Bronze layer ingestion for simulated_source completed successfully."
        )
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        send_email_notification(
            subject=f"Pipeline Failure: Bronze Ingestion for simulated_source",
            body=f"The Bronze layer ingestion for simulated_source failed with error: {e}"
        )
        raise