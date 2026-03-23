import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, year, month, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType

# Import utility functions
sys.path.append("../utils")
from pipeline_utils import retry, send_email_notification, create_idempotent_table, upsert_to_delta, config, get_storage_path, create_spark_session

# Initialize Spark Session
spark = create_spark_session("GoldUserAggregation")

# Load Configuration
silver_config = config.get('silver_layer', {})
gold_config = config.get('gold_layer', {})
SILVER_LAYER_PATH = silver_config.get('base_path', 'abfss://silver@medallionpipeline.dfs.core.windows.net/')
GOLD_LAYER_PATH = gold_config.get('base_path', 'abfss://gold@medallionpipeline.dfs.core.windows.net/')
SILVER_TABLE_NAME = silver_config.get('user_data_table_name', 'silver_user_data')
GOLD_TABLE_NAME = gold_config.get('aggregated_user_data_table_name', 'gold_user_demographics')

# Resolve paths based on local mode
SILVER_STORAGE_PATH = get_storage_path(SILVER_LAYER_PATH)
GOLD_STORAGE_PATH = get_storage_path(GOLD_LAYER_PATH)

@retry(exceptions=(Exception,), tries=3, delay=5)
def process_gold_user_data():
    print(f"Reading user data from Silver layer table: {SILVER_TABLE_NAME}")
    df_silver = spark.read.format("delta").load(SILVER_STORAGE_PATH)

    # --- Gold Layer Transformations ---
    # Aggregate user data by country
    df_gold = df_silver.groupBy(
        col("location_country").alias("country"),
        col("gender")
    ).agg(
        count(col("email")).alias("total_users"),
        avg(col("dob_age")).alias("average_age")
    ).orderBy(col("total_users").desc())

    # Define Gold layer schema
    gold_schema = StructType([
        StructField("country", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("total_users", LongType(), True),
        StructField("average_age", IntegerType(), True)
    ])

    # Ensure the Gold table exists
    create_idempotent_table(spark, GOLD_TABLE_NAME, gold_schema, ["country", "gender"], GOLD_STORAGE_PATH)

    # Use upsert for idempotency
    upsert_to_delta(spark, df_gold, GOLD_TABLE_NAME, ["country", "gender"], GOLD_STORAGE_PATH)

    print(f"Successfully processed {df_gold.count()} aggregated user records in Gold layer.")

if __name__ == "__main__":
    try:
        process_gold_user_data()
        send_email_notification(
            subject="Pipeline Success: Gold User Aggregation",
            body="The Gold layer user aggregation completed successfully."
        )
    except Exception as e:
        print(f"Pipeline failed: {e}")
        send_email_notification(
            subject="Pipeline Failure: Gold User Aggregation",
            body=f"The Gold layer user aggregation failed with error: {e}"
        )
        raise
    finally:
        spark.stop()
