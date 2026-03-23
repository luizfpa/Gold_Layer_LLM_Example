import os
import duckdb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BRONZE_LAYER_PATH = "./tmp/duckdb/bronze"
SILVER_LAYER_PATH = "./tmp/duckdb/silver"
BRONZE_PROCESSED_TABLE_NAME = "bronze_processed_data"
SILVER_PRODUCT_DATA_TABLE_NAME = "silver_product_data"

def get_db_path(table_name, layer_path):
    os.makedirs(layer_path, exist_ok=True)
    return os.path.join(layer_path, f"{table_name}.duckdb")

def create_silver_table():
    db_path = get_db_path(SILVER_PRODUCT_DATA_TABLE_NAME, SILVER_LAYER_PATH)
    conn = duckdb.connect(db_path)
    
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_PRODUCT_DATA_TABLE_NAME} (
        product_id INTEGER,
        product_name VARCHAR,
        product_value DOUBLE,
        event_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP,
        source_system VARCHAR
    )
    """)
    conn.close()
    logging.info(f"Table '{SILVER_PRODUCT_DATA_TABLE_NAME}' ensured at '{db_path}'")

def process_silver_data():
    # Read from bronze processed table
    bronze_db_path = get_db_path(BRONZE_PROCESSED_TABLE_NAME, BRONZE_LAYER_PATH)
    conn_bronze = duckdb.connect(bronze_db_path)
    
    logging.info(f"Reading processed data from Bronze layer table: {BRONZE_PROCESSED_TABLE_NAME}")
    bronze_data = conn_bronze.execute(f"SELECT * FROM {BRONZE_PROCESSED_TABLE_NAME}").fetchall()
    conn_bronze.close()
    
    logging.info(f"Processing {len(bronze_data)} records for Silver layer...")
    
    # Transform: clean data, standardize names, convert types
    silver_data = []
    for row in bronze_data:
        product_id = row[0]
        product_name = row[1].strip().lower() if row[1] else None
        raw_value = row[2]
        try:
            product_value = float(raw_value) if raw_value else 0.0
        except:
            product_value = 0.0
        event_timestamp = row[3]
        ingestion_timestamp = row[4]
        source_system = row[5]
        
        silver_data.append((
            product_id,
            product_name,
            product_value,
            event_timestamp,
            ingestion_timestamp,
            source_system
        ))
    
    # Write to silver table
    silver_db_path = get_db_path(SILVER_PRODUCT_DATA_TABLE_NAME, SILVER_LAYER_PATH)
    conn = duckdb.connect(silver_db_path)
    
    # Clear and insert
    conn.execute(f"DELETE FROM {SILVER_PRODUCT_DATA_TABLE_NAME}")
    
    for row in silver_data:
        conn.execute(f"""
        INSERT INTO {SILVER_PRODUCT_DATA_TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?)
        """, row)
    
    logging.info(f"Successfully processed and stored {len(silver_data)} records in Silver layer.")
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
        create_silver_table()
        process_silver_data()
        send_email_notification(
            subject="Pipeline Success: Silver Layer Processing",
            body="The Silver layer processing completed successfully."
        )
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        send_email_notification(
            subject="Pipeline Failure: Silver Layer Processing",
            body=f"The Silver layer processing failed with error: {e}"
        )
        raise