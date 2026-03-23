import os
import duckdb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SILVER_LAYER_PATH = "./tmp/duckdb/silver"
GOLD_LAYER_PATH = "./tmp/duckdb/gold"
SILVER_PRODUCT_DATA_TABLE_NAME = "silver_product_data"
GOLD_AGGREGATED_PRODUCT_DATA_TABLE_NAME = "gold_aggregated_product_data"

def get_db_path(table_name, layer_path):
    os.makedirs(layer_path, exist_ok=True)
    return os.path.join(layer_path, f"{table_name}.duckdb")

def create_gold_table():
    db_path = get_db_path(GOLD_AGGREGATED_PRODUCT_DATA_TABLE_NAME, GOLD_LAYER_PATH)
    conn = duckdb.connect(db_path)
    
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_AGGREGATED_PRODUCT_DATA_TABLE_NAME} (
        activity_date DATE,
        product_name VARCHAR,
        daily_unique_products BIGINT,
        daily_total_value DOUBLE,
        daily_average_value DOUBLE
    )
    """)
    conn.close()
    logging.info(f"Table '{GOLD_AGGREGATED_PRODUCT_DATA_TABLE_NAME}' ensured at '{db_path}'")

def process_gold_data():
    # Read from silver table
    silver_db_path = get_db_path(SILVER_PRODUCT_DATA_TABLE_NAME, SILVER_LAYER_PATH)
    conn_silver = duckdb.connect(silver_db_path)
    
    logging.info(f"Reading refined data from Silver layer table: {SILVER_PRODUCT_DATA_TABLE_NAME}")
    silver_data = conn_silver.execute(f"SELECT * FROM {SILVER_PRODUCT_DATA_TABLE_NAME}").fetchall()
    conn_silver.close()
    
    logging.info(f"Processing {len(silver_data)} records for Gold layer aggregation...")
    
    # Aggregate by product_name and day
    from collections import defaultdict
    aggregation = defaultdict(lambda: {"count": 0, "total": 0.0})
    
    for row in silver_data:
        product_id = row[0]
        product_name = row[1]
        product_value = row[2]
        event_timestamp = row[3]
        
        # Get date part
        if hasattr(event_timestamp, 'date'):
            activity_date = event_timestamp.date()
        else:
            activity_date = str(event_timestamp)[:10]
        
        key = (activity_date, product_name)
        aggregation[key]["count"] += 1
        aggregation[key]["total"] += product_value if product_value else 0.0
    
    # Calculate averages and prepare results
    gold_data = []
    for (activity_date, product_name), stats in aggregation.items():
        daily_unique_products = stats["count"]
        daily_total_value = stats["total"]
        daily_average_value = stats["total"] / stats["count"] if stats["count"] > 0 else 0.0
        
        gold_data.append((
            activity_date,
            product_name,
            daily_unique_products,
            daily_total_value,
            daily_average_value
        ))
    
    # Sort by activity_date and product_name
    gold_data.sort(key=lambda x: (x[0], x[1]))
    
    # Write to gold table
    gold_db_path = get_db_path(GOLD_AGGREGATED_PRODUCT_DATA_TABLE_NAME, GOLD_LAYER_PATH)
    conn = duckdb.connect(gold_db_path)
    
    conn.execute(f"DELETE FROM {GOLD_AGGREGATED_PRODUCT_DATA_TABLE_NAME}")
    
    for row in gold_data:
        conn.execute(f"""
        INSERT INTO {GOLD_AGGREGATED_PRODUCT_DATA_TABLE_NAME} VALUES (?, ?, ?, ?, ?)
        """, row)
    
    logging.info(f"Successfully processed and stored {len(gold_data)} aggregated records in Gold layer.")
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
        create_gold_table()
        process_gold_data()
        send_email_notification(
            subject="Pipeline Success: Gold Layer Processing",
            body="The Gold layer processing (aggregation) completed successfully."
        )
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        send_email_notification(
            subject="Pipeline Failure: Gold Layer Processing",
            body=f"The Gold layer processing (aggregation) failed with error: {e}"
        )
        raise