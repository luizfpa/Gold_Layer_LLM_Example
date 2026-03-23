import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, cast, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType

# Import utility functions
sys.path.append("../utils")
from pipeline_utils import retry, send_email_notification, create_idempotent_table, upsert_to_delta, config, get_storage_path, create_spark_session

# Initialize Spark Session
spark = create_spark_session("GoldLLMViewCreation")

# Load Configuration
gold_config = config.get('gold_layer', {})
GOLD_LAYER_PATH = gold_config.get('base_path', 'abfss://gold@medallionpipeline.dfs.core.windows.net/')
GOLD_AGGREGATED_USER_TABLE = gold_config.get('aggregated_user_data_table_name', 'gold_user_demographics')
GOLD_LLM_VIEW_TABLE = gold_config.get('llm_view_table_name', 'gold_llm_demographic_summary')

# Resolve paths based on local mode
GOLD_STORAGE_PATH = get_storage_path(GOLD_LAYER_PATH)
GOLD_AGGREGATED_PATH = GOLD_STORAGE_PATH

@retry(exceptions=(Exception,), tries=3, delay=5)
def create_llm_view():
    print(f"Reading aggregated user data from Gold layer table: {GOLD_AGGREGATED_USER_TABLE}")
    df_gold = spark.read.format("delta").load(GOLD_AGGREGATED_PATH)

    # --- LLM View Transformations ---
    # Create a natural language summary for LLM consumption
    df_llm = df_gold.select(
        col("country"),
        col("gender"),
        col("total_users"),
        col("average_age"),
        concat_ws(" ",
            lit("In"), col("country"),
            lit("there are"), col("total_users").cast(StringType()),
            lit("users with an average age of"), col("average_age").cast(StringType()),
            lit("years old.")
        ).alias("user_demographic_summary")
    )

    # Define schema
    llm_schema = StructType([
        StructField("country", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("total_users", LongType(), True),
        StructField("average_age", IntegerType(), True),
        StructField("user_demographic_summary", StringType(), True)
    ])

    # Ensure the table exists
    create_idempotent_table(spark, GOLD_LLM_VIEW_TABLE, llm_schema, ["country", "gender"], GOLD_STORAGE_PATH)

    # Use upsert for idempotency
    upsert_to_delta(spark, df_llm, GOLD_LLM_VIEW_TABLE, ["country", "gender"], GOLD_STORAGE_PATH)

    print(f"Successfully created LLM view with {df_llm.count()} records.")

if __name__ == "__main__":
    try:
        create_llm_view()
        send_email_notification(
            subject="Pipeline Success: Gold LLM View Creation",
            body="The Gold layer LLM view creation completed successfully."
        )
    except Exception as e:
        print(f"Pipeline failed: {e}")
        send_email_notification(
            subject="Pipeline Failure: Gold LLM View Creation",
            body=f"The Gold layer LLM view creation failed with error: {e}"
        )
        raise
    finally:
        spark.stop()
