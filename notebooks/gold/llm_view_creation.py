import os
import duckdb
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SILVER_LAYER_PATH = "./tmp/duckdb/silver"
GOLD_LAYER_PATH = "./tmp/duckdb/gold"
OUTPUT_PATH = "./output"

def create_llm_view():
    # Read from gold user demographics table
    gold_db_path = os.path.join(GOLD_LAYER_PATH, "gold_user_demographics.duckdb")
    conn_gold = duckdb.connect(gold_db_path)
    
    logging.info(f"Reading user demographics from Gold layer...")
    gold_data = conn_gold.execute(f"SELECT * FROM gold_user_demographics").fetchall()
    conn_gold.close()
    
    logging.info(f"Creating LLM JSON view with {len(gold_data)} records...")
    
    # Create JSON data directly
    llm_data = []
    for row in gold_data:
        country = row[0]
        gender = row[1]
        user_count = row[2]
        average_age = row[3]
        
        # Create LLM-friendly JSON object
        llm_data.append({
            "country": country,
            "gender": gender,
            "user_count": user_count,
            "average_age": round(average_age, 1),
            "insight": f"In {country}, there are {user_count} users of gender {gender} with an average age of {average_age:.1f} years."
        })
    
    # Save directly as JSON file
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    json_path = os.path.join(OUTPUT_PATH, "gold_llm_demographic_summary.json")
    with open(json_path, 'w') as f:
        json.dump(llm_data, f, indent=2)
    
    logging.info(f"Successfully created LLM JSON view with {len(llm_data)} records at {json_path}")

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
        create_llm_view()
        send_email_notification(
            subject="Pipeline Success: Gold Layer LLM View Creation",
            body="The Gold layer LLM JSON view creation completed successfully."
        )
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        send_email_notification(
            subject="Pipeline Failure: Gold Layer LLM View Creation",
            body=f"The Gold layer LLM JSON view creation failed with error: {e}"
        )
        raise
