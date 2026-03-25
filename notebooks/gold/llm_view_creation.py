import os
import duckdb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

GOLD_LAYER_PATH = "./tmp/duckdb/gold"
GOLD_LLM_VIEW_TABLE_NAME = "gold_llm_demographic_summary"

def get_db_path(table_name, layer_path=GOLD_LAYER_PATH):
    os.makedirs(layer_path, exist_ok=True)
    return os.path.join(layer_path, f"{table_name}.duckdb")

def create_llm_view_table():
    db_path = get_db_path(GOLD_LLM_VIEW_TABLE_NAME)
    conn = duckdb.connect(db_path)
    conn.execute(f"DROP TABLE IF EXISTS {GOLD_LLM_VIEW_TABLE_NAME}")
    conn.execute(f"""
    CREATE TABLE {GOLD_LLM_VIEW_TABLE_NAME} (
        country VARCHAR,
        gender VARCHAR,
        user_count BIGINT,
        average_age DOUBLE,
        summary_text VARCHAR
    )
    """)
    conn.close()
    logging.info(f"Table '{GOLD_LLM_VIEW_TABLE_NAME}' created at '{db_path}'")

def create_llm_view():
    # Read from gold user demographics table
    gold_db_path = get_db_path("gold_user_demographics")
    conn_gold = duckdb.connect(gold_db_path)
    
    logging.info(f"Reading user demographics from Gold layer...")
    gold_data = conn_gold.execute(f"SELECT * FROM gold_user_demographics").fetchall()
    conn_gold.close()
    
    logging.info(f"Creating LLM view with {len(gold_data)} records...")
    
    # Create summary text for each record
    llm_data = []
    for row in gold_data:
        country = row[0]
        gender = row[1]
        user_count = row[2]
        average_age = row[3]
        
        # Create LLM-friendly summary
        summary_text = f"In {country}, there are {user_count} users of gender {gender} with an average age of {average_age:.1f} years."
        
        llm_data.append((
            country,
            gender,
            user_count,
            average_age,
            summary_text
        ))
    
    # Write to LLM view table
    llm_db_path = get_db_path(GOLD_LLM_VIEW_TABLE_NAME)
    conn = duckdb.connect(llm_db_path)
    
    for row in llm_data:
        conn.execute(f"""
        INSERT INTO {GOLD_LLM_VIEW_TABLE_NAME} VALUES (?, ?, ?, ?, ?)
        """, row)
    
    logging.info(f"Successfully created LLM consumption view with {len(llm_data)} records.")
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
        create_llm_view_table()
        create_llm_view()
        send_email_notification(
            subject="Pipeline Success: Gold Layer LLM View Creation",
            body="The Gold layer LLM consumption view creation completed successfully."
        )
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        send_email_notification(
            subject="Pipeline Failure: Gold Layer LLM View Creation",
            body=f"The Gold layer LLM consumption view creation failed with error: {e}"
        )
        raise
