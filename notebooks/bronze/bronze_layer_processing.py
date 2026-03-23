import os
import duckdb
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BRONZE_LAYER_PATH = "./tmp/duckdb/bronze"
BRONZE_RAW_TABLE_NAME = "bronze_raw_data"
BRONZE_PROCESSED_TABLE_NAME = "bronze_processed_data"

def get_db_path(table_name, layer_path=BRONZE_LAYER_PATH):
    os.makedirs(layer_path, exist_ok=True)
    return os.path.join(layer_path, f"{table_name}.duckdb")

def create_processed_table():
    db_path = get_db_path(BRONZE_PROCESSED_TABLE_NAME)
    conn = duckdb.connect(db_path)
    
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_PROCESSED_TABLE_NAME} (
        product_id INTEGER,
        product_name VARCHAR,
        raw_value VARCHAR,
        event_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP,
        source_system VARCHAR
    )
    """)
    conn.close()
    logging.info(f"Table '{BRONZE_PROCESSED_TABLE_NAME}' ensured at '{db_path}'")

def process_bronze_data():
    # Read from raw table
    raw_db_path = get_db_path(BRONZE_RAW_TABLE_NAME)
    conn_raw = duckdb.connect(raw_db_path)
    
    logging.info(f"Reading raw data from Bronze layer table: {BRONZE_RAW_TABLE_NAME}")
    raw_data = conn_raw.execute(f"SELECT * FROM {BRONZE_RAW_TABLE_NAME}").fetchall()
    conn_raw.close()
    
    logging.info(f"Processing {len(raw_data)} records...")
    
    # Transform data
    processed_data = []
    for row in raw_data:
        # row: id, name, value, timestamp, ingestion_timestamp, source_system
        product_id = row[0]
        product_name = row[1]
        raw_value = row[2]
        event_timestamp = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S")
        ingestion_timestamp = row[4]
        source_system = row[5]
        
        processed_data.append((
            product_id,
            product_name,
            raw_value,
            event_timestamp,
            ingestion_timestamp,
            source_system
        ))
    
    # Write to processed table
    processed_db_path = get_db_path(BRONZE_PROCESSED_TABLE_NAME)
    conn = duckdb.connect(processed_db_path)
    
    # Clear and insert
    conn.execute(f"DELETE FROM {BRONZE_PROCESSED_TABLE_NAME}")
    
    for row in processed_data:
        conn.execute(f"""
        INSERT INTO {BRONZE_PROCESSED_TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?)
        """, row)
    
    logging.info(f"Successfully processed and stored {len(processed_data)} records in Bronze processed layer.")
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
        create_processed_table()
        process_bronze_data()
        send_email_notification(
            subject="Pipeline Success: Bronze Layer Processing",
            body="The Bronze layer processing completed successfully."
        )
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        send_email_notification(
            subject="Pipeline Failure: Bronze Layer Processing",
            body=f"The Bronze layer processing failed with error: {e}"
        )
        raise