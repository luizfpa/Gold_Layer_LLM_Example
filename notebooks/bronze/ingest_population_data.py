import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType

# Import utility functions
sys.path.append("../utils")
from pipeline_utils import retry, send_email_notification, create_idempotent_table, upsert_to_delta, config, get_storage_path, get_raw_data_path, create_spark_session

# Initialize Spark Session
spark = create_spark_session("BronzePopulationIngestion")

# Load Configuration
bronze_config = config.get('bronze_layer', {})
BRONZE_LAYER_PATH = bronze_config.get('base_path', 'abfss://bronze@medallionpipeline.dfs.core.windows.net/')
RAW_DATA_PATH_CONFIG = bronze_config.get('raw_data_path', 'abfss://rawdata@medallionpipeline.dfs.core.windows.net/')
SOURCE_SYSTEM_NAME = "world_bank_api"
BRONZE_TABLE_NAME = bronze_config.get('raw_population_table_name', 'bronze_population_data')

# Resolve paths based on local mode
BRONZE_STORAGE_PATH = get_storage_path(BRONZE_LAYER_PATH)
RAW_DATA_PATH = get_raw_data_path("population.csv")

# Define schema for the raw population data
population_schema = StructType([
    StructField("country", StringType(), True),
    StructField("year", StringType(), True),
    StructField("value", LongType(), True)
])

def ingest_population_data():
    print(f"Ingesting population data into Bronze layer table: {BRONZE_TABLE_NAME}")
    
    # Read the raw CSV file
    try:
        df_raw = spark.read.format("csv").option("header", "true").schema(population_schema).load(RAW_DATA_PATH + "population.csv")
    except Exception as e:
        print(f"Error reading raw data: {e}")
        df_raw = spark.createDataFrame([], population_schema)

    # Add metadata columns
    df_bronze = df_raw.withColumn("ingestion_timestamp", current_timestamp())
    df_bronze = df_bronze.withColumn("source_system", lit(SOURCE_SYSTEM_NAME))

    # Ensure the Bronze table exists with the correct schema
    create_idempotent_table(spark, BRONZE_TABLE_NAME, population_schema.add(StructField("ingestion_timestamp", TimestampType(), True)).add(StructField("source_system", StringType(), True)), ["country", "year", "source_system"], BRONZE_STORAGE_PATH)

    # Use upsert for idempotency
    upsert_to_delta(spark, df_bronze, BRONZE_TABLE_NAME, ["country", "year", "source_system"], BRONZE_STORAGE_PATH)

    print(f"Successfully ingested {df_bronze.count()} records into Bronze layer.")

if __name__ == "__main__":
    try:
        ingest_population_data()
        send_email_notification(
            subject=f"Pipeline Success: Bronze Population Ingestion for {SOURCE_SYSTEM_NAME}",
            body=f"The Bronze layer population ingestion for {SOURCE_SYSTEM_NAME} completed successfully."
        )
    except Exception as e:
        print(f"Pipeline failed: {e}")
        send_email_notification(
            subject=f"Pipeline Failure: Bronze Population Ingestion for {SOURCE_SYSTEM_NAME}",
            body=f"The Bronze layer population ingestion for {SOURCE_SYSTEM_NAME} failed with error: {e}"
        )
        raise
    finally:
        spark.stop()
