import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Import utility functions
# Get the absolute path to the notebooks directory
notebooks_dir = os.path.dirname(os.path.abspath(__file__))
utils_path = os.path.join(os.path.dirname(notebooks_dir), 'utils')
sys.path.append(utils_path)
from pipeline_utils import retry, send_email_notification, create_idempotent_table, upsert_to_delta, config, get_storage_path, get_raw_data_path, create_spark_session

# Initialize Spark Session
spark = create_spark_session("BronzeUserIngestion")

# Load Configuration
bronze_config = config.get('bronze_layer', {})
BRONZE_LAYER_PATH = bronze_config.get('base_path', 'abfss://bronze@medallionpipeline.dfs.core.windows.net/')
RAW_DATA_PATH_CONFIG = bronze_config.get('raw_data_path', 'abfss://rawdata@medallionpipeline.dfs.core.windows.net/')
SOURCE_SYSTEM_NAME = "random_user_api"
BRONZE_TABLE_NAME = bronze_config.get('raw_user_table_name', 'bronze_user_data')

# Resolve paths based on local mode
BRONZE_STORAGE_PATH = get_storage_path(BRONZE_LAYER_PATH)
# Correctly construct raw data path - get_raw_data_path already includes filename
RAW_DATA_PATH = get_raw_data_path("users.csv")

# Define schema for the raw user data
user_schema = StructType([
    StructField("gender", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("dob_date", StringType(), True),
    StructField("dob_age", IntegerType(), True),
    StructField("location_city", StringType(), True),
    StructField("location_country", StringType(), True),
    StructField("registered_date", StringType(), True),
    StructField("cell", StringType(), True)
])

def ingest_user_data():
    print(f"Ingesting user data into Bronze layer table: {BRONZE_TABLE_NAME}")
    
    # Read the raw CSV file
    try:
        df_raw = spark.read.format("csv").option("header", "true").schema(user_schema).load(RAW_DATA_PATH + "users.csv")
    except Exception as e:
        print(f"Error reading raw data: {e}")
        # If file not found, create empty dataframe
        df_raw = spark.createDataFrame([], user_schema)

    # Add metadata columns
    df_bronze = df_raw.withColumn("ingestion_timestamp", current_timestamp())
    df_bronze = df_bronze.withColumn("source_system", lit(SOURCE_SYSTEM_NAME))

    # Ensure the Bronze table exists with the correct schema
    create_idempotent_table(spark, BRONZE_TABLE_NAME, user_schema.add(StructField("ingestion_timestamp", TimestampType(), True)).add(StructField("source_system", StringType(), True)), ["email", "source_system"], BRONZE_STORAGE_PATH)

    # Use upsert for idempotency
    upsert_to_delta(spark, df_bronze, BRONZE_TABLE_NAME, ["email", "source_system"], BRONZE_STORAGE_PATH)

    print(f"Successfully ingested {df_bronze.count()} records into Bronze layer.")

if __name__ == "__main__":
    try:
        ingest_user_data()
        send_email_notification(
            subject=f"Pipeline Success: Bronze User Ingestion for {SOURCE_SYSTEM_NAME}",
            body=f"The Bronze layer user ingestion for {SOURCE_SYSTEM_NAME} completed successfully."
        )
    except Exception as e:
        print(f"Pipeline failed: {e}")
        send_email_notification(
            subject=f"Pipeline Failure: Bronze User Ingestion for {SOURCE_SYSTEM_NAME}",
            body=f"The Bronze layer user ingestion for {SOURCE_SYSTEM_NAME} failed with error: {e}"
        )
        raise
    finally:
        spark.stop()
