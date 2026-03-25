import os
import duckdb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SILVER_LAYER_PATH = "./tmp/duckdb/silver"
GOLD_LAYER_PATH = "./tmp/duckdb/gold"
GOLD_USER_DEMOGRAPHICS_TABLE = "gold_user_demographics"
GOLD_POPULATION_STATS_TABLE = "gold_population_stats"

def get_db_path(table_name, layer_path):
    os.makedirs(layer_path, exist_ok=True)
    return os.path.join(layer_path, f"{table_name}.duckdb")

def create_gold_tables():
    # Create gold user demographics table
    db_path = get_db_path(GOLD_USER_DEMOGRAPHICS_TABLE, GOLD_LAYER_PATH)
    conn = duckdb.connect(db_path)
    conn.execute(f"DROP TABLE IF EXISTS {GOLD_USER_DEMOGRAPHICS_TABLE}")
    conn.execute(f"""
    CREATE TABLE {GOLD_USER_DEMOGRAPHICS_TABLE} (
        country VARCHAR,
        gender VARCHAR,
        user_count BIGINT,
        average_age DOUBLE
    )
    """)
    conn.close()
    
    # Create gold population stats table
    db_path = get_db_path(GOLD_POPULATION_STATS_TABLE, GOLD_LAYER_PATH)
    conn = duckdb.connect(db_path)
    conn.execute(f"DROP TABLE IF EXISTS {GOLD_POPULATION_STATS_TABLE}")
    conn.execute(f"""
    CREATE TABLE {GOLD_POPULATION_STATS_TABLE} (
        country VARCHAR,
        year INTEGER,
        total_population BIGINT,
        latest_population BIGINT
    )
    """)
    conn.close()
    logging.info("Gold tables created successfully")

def process_gold_data():
    # Process user demographics
    silver_db_path = get_db_path("silver_user_data", SILVER_LAYER_PATH)
    conn_silver = duckdb.connect(silver_db_path)
    
    logging.info(f"Reading user data from Silver layer...")
    silver_data = conn_silver.execute(f"SELECT location_country, gender, dob_age FROM silver_user_data").fetchall()
    conn_silver.close()
    
    logging.info(f"Processing {len(silver_data)} user records for Gold layer aggregation...")
    
    from collections import defaultdict
    aggregation = defaultdict(lambda: {"count": 0, "total_age": 0.0})
    
    for row in silver_data:
        country = row[0]
        gender = row[1]
        age = row[2] if row[2] else 0
        
        key = (country, gender)
        aggregation[key]["count"] += 1
        aggregation[key]["total_age"] += age
    
    gold_data = []
    for (country, gender), stats in aggregation.items():
        count = stats["count"]
        avg_age = stats["total_age"] / count if count > 0 else 0.0
        gold_data.append((country, gender, count, avg_age))
    
    gold_db_path = get_db_path(GOLD_USER_DEMOGRAPHICS_TABLE, GOLD_LAYER_PATH)
    conn = duckdb.connect(gold_db_path)
    
    for row in gold_data:
        conn.execute(f"""
        INSERT INTO {GOLD_USER_DEMOGRAPHICS_TABLE} VALUES (?, ?, ?, ?)
        """, row)
    
    logging.info(f"Successfully processed {len(gold_data)} aggregated user records in Gold layer.")
    conn.close()
    
    # Process population stats
    silver_db_path = get_db_path("silver_population_data", SILVER_LAYER_PATH)
    conn_silver = duckdb.connect(silver_db_path)
    
    logging.info(f"Reading population data from Silver layer...")
    silver_data = conn_silver.execute(f"SELECT country, year, value FROM silver_population_data").fetchall()
    conn_silver.close()
    
    logging.info(f"Processing {len(silver_data)} population records for Gold layer...")
    
    # Group by country
    country_data = defaultdict(list)
    for row in silver_data:
        country_data[row[0]].append((row[1], row[2]))
    
    gold_data = []
    for country, values in country_data.items():
        total_pop = sum([v[1] for v in values])
        # Get latest year population
        sorted_values = sorted(values, key=lambda x: x[0], reverse=True)
        latest_pop = sorted_values[0][1] if sorted_values else 0
        
        # Get most common year
        most_common_year = sorted_values[0][0] if sorted_values else 0
        
        gold_data.append((country, most_common_year, total_pop, latest_pop))
    
    gold_db_path = get_db_path(GOLD_POPULATION_STATS_TABLE, GOLD_LAYER_PATH)
    conn = duckdb.connect(gold_db_path)
    
    for row in gold_data:
        conn.execute(f"""
        INSERT INTO {GOLD_POPULATION_STATS_TABLE} VALUES (?, ?, ?, ?)
        """, row)
    
    logging.info(f"Successfully processed {len(gold_data)} aggregated population records in Gold layer.")
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
        create_gold_tables()
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
