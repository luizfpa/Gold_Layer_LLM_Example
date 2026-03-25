import os
import duckdb
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

GOLD_LAYER_PATH = "./tmp/duckdb/gold"
GOLD_REPORTING_VIEW_TABLE_NAME = "gold_reporting_demographic_summary"

def get_db_path(table_name, layer_path=GOLD_LAYER_PATH):
    os.makedirs(layer_path, exist_ok=True)
    return os.path.join(layer_path, f"{table_name}.duckdb")

def create_reporting_view_table():
    db_path = get_db_path(GOLD_REPORTING_VIEW_TABLE_NAME)
    conn = duckdb.connect(db_path)
    conn.execute(f"DROP TABLE IF EXISTS {GOLD_REPORTING_VIEW_TABLE_NAME}")
    conn.execute(f"""
    CREATE TABLE {GOLD_REPORTING_VIEW_TABLE_NAME} (
        country VARCHAR,
        gender VARCHAR,
        user_count BIGINT,
        average_age DOUBLE,
        age_group VARCHAR
    )
    """)
    conn.close()
    logging.info(f"Table '{GOLD_REPORTING_VIEW_TABLE_NAME}' created at '{db_path}'")

def create_reporting_view():
    # Read from gold user demographics table
    gold_db_path = get_db_path("gold_user_demographics")
    conn_gold = duckdb.connect(gold_db_path)
    
    logging.info(f"Reading user demographics from Gold layer...")
    gold_data = conn_gold.execute(f"SELECT * FROM gold_user_demographics").fetchall()
    conn_gold.close()
    
    logging.info(f"Creating BI/Reporting view with {len(gold_data)} records...")
    
    # Add age group dimension
    reporting_data = []
    for row in gold_data:
        country = row[0]
        gender = row[1]
        user_count = row[2]
        average_age = row[3]
        
        # Determine age group
        if average_age < 25:
            age_group = "Young (0-24)"
        elif average_age < 40:
            age_group = "Adult (25-39)"
        elif average_age < 60:
            age_group = "Middle-aged (40-59)"
        else:
            age_group = "Senior (60+)"
        
        reporting_data.append((
            country,
            gender,
            user_count,
            average_age,
            age_group
        ))
    
    # Write to reporting view table
    reporting_db_path = get_db_path(GOLD_REPORTING_VIEW_TABLE_NAME)
    conn = duckdb.connect(reporting_db_path)
    
    for row in reporting_data:
        conn.execute(f"""
        INSERT INTO {GOLD_REPORTING_VIEW_TABLE_NAME} VALUES (?, ?, ?, ?, ?)
        """, row)
    
    logging.info(f"Successfully created BI/Reporting view with {len(reporting_data)} records.")
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
        create_reporting_view_table()
        create_reporting_view()
        send_email_notification(
            subject="Pipeline Success: Gold Layer Reporting View Creation",
            body="The Gold layer BI/Reporting view creation completed successfully."
        )
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        send_email_notification(
            subject="Pipeline Failure: Gold Layer Reporting View Creation",
            body=f"The Gold layer BI/Reporting view creation failed with error: {e}"
        )
        raise
