import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_date, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Import utility functions
sys.path.append("../utils")
from pipeline_utils import retry, send_email_notification, create_idempotent_table, upsert_to_delta, config, get_storage_path, create_spark_session

# Initialize Spark Session
spark = create_spark_session("SilverUserProcessing")

# Load Configuration
bronze_config = config.get('bronze_layer', {})
silver_config = config.get('silver_layer', {})
BRONZE_LAYER_PATH = bronze_config.get('base_path', 'abfss://bronze@medallionpipeline.dfs.core.windows.net/')
SILVER_LAYER_PATH = silver_config.get('base_path', 'abfss://silver@medallionpipeline.dfs.core.windows.net/')
BRONZE_TABLE_NAME = bronze_config.get('raw_user_table_name', 'bronze_user_data')
SILVER_TABLE_NAME = silver_config.get('user_data_table_name', 'silver_user_data')

# Resolve paths based on local mode
BRONZE_STORAGE_PATH = get_storage_path(BRONZE_LAYER_PATH)
SILVER_STORAGE_PATH = get_storage_path(SILVER_LAYER_PATH)

@retry(exceptions=(Exception,), tries=3, delay=5)
def process_silver_user_data():
    print(f"Reading raw user data from Bronze layer table: {BRONZE_TABLE_NAME}")
    df_bronze = spark.read.format("delta").load(BRONZE_STORAGE_PATH)

    # --- Silver Layer Transformations ---
    # 1. Data Cleaning and Standardization
    df_silver = df_bronze.select(
        col("gender"),
        trim(lower(col("first_name"))).alias("first_name"),
        trim(lower(col("last_name"))).alias("last_name"),
        col("email"),
        col("phone"),
        to_date(col("dob_date")).alias("dob_date"),
        col("dob_age").cast(IntegerType()),
        trim(lower(col("location_city"))).alias("location_city"),
        trim(lower(col("location_country"))).alias("location_country"),
        to_date(col("registered_date")).alias("registered_date"),
        col("cell"),
        col("ingestion_timestamp"),
        col("source_system")
    ).withColumn("processed_timestamp", current_timestamp())

    # Define Silver layer schema
    silver_schema = StructType([
        StructField("gender", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("dob_date", StringType(), True), # Keeping as string for date simplicity
        StructField("dob_age", IntegerType(), True),
        StructField("location_city", StringType(), True),
        StructField("location_country", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("cell", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("source_system", StringType(), True),
        StructField("processed_timestamp", TimestampType(), True)
    ])

    # Ensure the Silver table exists
    create_idempotent_table(spark, SILVER_TABLE_NAME, silver_schema, ["email", "source_system"], SILVER_STORAGE_PATH)

    # Use upsert for idempotency
    upsert_to_delta(spark, df_silver, SILVER_TABLE_NAME, ["email", "source_system"], SILVER_STORAGE_PATH)

    print(f"Successfully processed {df_silver.count()} user records in Silver layer.")

if __name__ == "__main__":
    try:
        process_silver_user_data()
        send_email_notification(
            subject="Pipeline Success: Silver User Processing",
            body="The Silver layer user processing completed successfully."
        )
    except Exception as e:
        print(f"Pipeline failed: {e}")
        send_email_notification(
            subject="Pipeline Failure: Silver User Processing",
            body=f"The Silver layer user processing failed with error: {e}"
        )
        raise
    finally:
        spark.stop()
