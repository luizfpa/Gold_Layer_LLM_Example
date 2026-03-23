import os
import duckdb
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

GOLD_LAYER_PATH = "./tmp/duckdb/gold"
GOLD_AGGREGATED_PRODUCT_DATA_TABLE_NAME = "gold_aggregated_product_data"
GOLD_REPORTING_VIEW_TABLE_NAME = "gold_reporting_product_summary"

def get_db_path(table_name, layer_path=GOLD_LAYER_PATH):
    os.makedirs(layer_path, exist_ok=True)
    return os.path.join(layer_path, f"{table_name}.duckdb")

def create_reporting_view_table():
    db_path = get_db_path(GOLD_REPORTING_VIEW_TABLE_NAME)
    conn = duckdb.connect(db_path)
    
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_REPORTING_VIEW_TABLE_NAME} (
        activity_date DATE,
        activity_year INTEGER,
        activity_month INTEGER,
        activity_day INTEGER,
        product_name VARCHAR,
        daily_unique_products BIGINT,
        daily_total_value DOUBLE,
        daily_average_value DOUBLE
    )
    """)
    conn.close()
    logging.info(f"Table '{GOLD_REPORTING_VIEW_TABLE_NAME}' ensured at '{db_path}'")

def create_reporting_view():
    # Read from gold aggregated table
    gold_db_path = get_db_path(GOLD_AGGREGATED_PRODUCT_DATA_TABLE_NAME)
    conn_gold = duckdb.connect(gold_db_path)
    
    logging.info(f"Reading aggregated data from Gold layer table: {GOLD_AGGREGATED_PRODUCT_DATA_TABLE_NAME}")
    gold_data = conn_gold.execute(f"SELECT * FROM {GOLD_AGGREGATED_PRODUCT_DATA_TABLE_NAME}").fetchall()
    conn_gold.close()
    
    logging.info(f"Creating BI/Reporting view with {len(gold_data)} records...")
    
    # Add time dimension columns
    reporting_data = []
    for row in gold_data:
        activity_date = row[0]
        product_name = row[1]
        daily_unique_products = row[2]
        daily_total_value = row[3]
        daily_average_value = row[4]
        
        # Extract year, month, day
        if hasattr(activity_date, 'year'):
            activity_year = activity_date.year
            activity_month = activity_date.month
            activity_day = activity_date.day
        else:
            # Handle string date
            dt = datetime.strptime(str(activity_date), "%Y-%m-%d")
            activity_year = dt.year
            activity_month = dt.month
            activity_day = dt.day
        
        reporting_data.append((
            activity_date,
            activity_year,
            activity_month,
            activity_day,
            product_name,
            daily_unique_products,
            daily_total_value,
            daily_average_value
        ))
    
    # Write to reporting view table
    reporting_db_path = get_db_path(GOLD_REPORTING_VIEW_TABLE_NAME)
    conn = duckdb.connect(reporting_db_path)
    
    conn.execute(f"DELETE FROM {GOLD_REPORTING_VIEW_TABLE_NAME}")
    
    for row in reporting_data:
        conn.execute(f"""
        INSERT INTO {GOLD_REPORTING_VIEW_TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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