import os
import sys
import duckdb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BRONZE_LAYER_PATH = "./tmp/duckdb/bronze"
SILVER_LAYER_PATH = "./tmp/duckdb/silver"
SILVER_USER_TABLE_NAME = "silver_user_data"
SILVER_POPULATION_TABLE_NAME = "silver_population_data"

def get_db_path(table_name, layer_path):
    os.makedirs(layer_path, exist_ok=True)
    return os.path.join(layer_path, f"{table_name}.duckdb")

def create_silver_tables():
    # Create silver user table
    db_path = get_db_path(SILVER_USER_TABLE_NAME, SILVER_LAYER_PATH)
    conn = duckdb.connect(db_path)
    conn.execute(f"DROP TABLE IF EXISTS {SILVER_USER_TABLE_NAME}")
    conn.execute(f"""
    CREATE TABLE {SILVER_USER_TABLE_NAME} (
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
        processed_timestamp TIMESTAMP
    )
    """)
    conn.close()
    
    # Create silver population table
    db_path = get_db_path(SILVER_POPULATION_TABLE_NAME, SILVER_LAYER_PATH)
    conn = duckdb.connect(db_path)
    conn.execute(f"DROP TABLE IF EXISTS {SILVER_POPULATION_TABLE_NAME}")
    conn.execute(f"""
    CREATE TABLE {SILVER_POPULATION_TABLE_NAME} (
        country VARCHAR,
        year INTEGER,
        value BIGINT,
        ingestion_timestamp TIMESTAMP,
        source_system VARCHAR,
        processed_timestamp TIMESTAMP
    )
    """)
    conn.close()
    logging.info("Silver tables created successfully")

def process_silver_data():
    from datetime import datetime
    
    # Process user data
    bronze_db_path = os.path.join(BRONZE_LAYER_PATH, "bronze.duckdb")
    conn_bronze = duckdb.connect(bronze_db_path)
    
    logging.info(f"Reading user data from Bronze layer...")
    bronze_data = conn_bronze.execute(f"SELECT * FROM bronze_user_data").fetchall()
    conn_bronze.close()
    
    logging.info(f"Processing {len(bronze_data)} user records for Silver layer...")
    
    silver_db_path = get_db_path(SILVER_USER_TABLE_NAME, SILVER_LAYER_PATH)
    conn = duckdb.connect(silver_db_path)
    
    processed_timestamp = datetime.now()
    
    for row in bronze_data:
        # Convert age to integer
        dob_age = row[6]
        if dob_age is None:
            dob_age = 0
            
        conn.execute(f"""
        INSERT INTO {SILVER_USER_TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (*row, processed_timestamp))
    
    logging.info(f"Successfully processed {len(bronze_data)} user records in Silver layer.")
    conn.close()
    
    # Process population data
    bronze_db_path = os.path.join(BRONZE_LAYER_PATH, "bronze.duckdb")
    conn_bronze = duckdb.connect(bronze_db_path)
    
    logging.info(f"Reading population data from Bronze layer...")
    bronze_data = conn_bronze.execute(f"SELECT * FROM bronze_population_data").fetchall()
    conn_bronze.close()
    
    logging.info(f"Processing {len(bronze_data)} population records for Silver layer...")
    
    silver_db_path = get_db_path(SILVER_POPULATION_TABLE_NAME, SILVER_LAYER_PATH)
    conn = duckdb.connect(silver_db_path)
    
    for row in bronze_data:
        # Convert year to integer
        year = row[1]
        try:
            year = int(year)
        except:
            year = 0
            
        conn.execute(f"""
        INSERT INTO {SILVER_POPULATION_TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?)
        """, (row[0], year, row[2], row[3], row[4], processed_timestamp))
    
    logging.info(f"Successfully processed {len(bronze_data)} population records in Silver layer.")
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
        create_silver_tables()
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
