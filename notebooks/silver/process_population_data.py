import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType

# Import utility functions
sys.path.append("../utils")
from pipeline_utils import retry, send_email_notification, create_idempotent_table, upsert_to_delta, config, get_storage_path, create_spark_session

# Initialize Spark Session
spark = create_spark_session("SilverPopulationProcessing")

# Load Configuration
bronze_config = config.get('bronze_layer', {})
silver_config = config.get('silver_layer', {})
BRONZE_LAYER_PATH = bronze_config.get('base_path', 'abfss://bronze@medallionpipeline.dfs.core.windows.net/')
SILVER_LAYER_PATH = silver_config.get('base_path', 'abfss://silver@medallionpipeline.dfs.core.windows.net/')
BRONZE_TABLE_NAME = bronze_config.get('raw_population_table_name', 'bronze_population_data')
SILVER_TABLE_NAME = silver_config.get('population_data_table_name', 'silver_population_data')

# Resolve paths based on local mode
BRONZE_STORAGE_PATH = get_storage_path(BRONZE_LAYER_PATH)
SILVER_STORAGE_PATH = get_storage_path(SILVER_LAYER_PATH)

@retry(exceptions=(Exception,), tries=3, delay=5)
def process_silver_population_data():
    print(f"Reading raw population data from Bronze layer table: {BRONZE_TABLE_NAME}")
    df_bronze = spark.read.format("delta").load(BRONZE_STORAGE_PATH)

    # --- Silver Layer Transformations ---
    # 1. Data Type Conversion
    df_silver = df_bronze.select(
        col("country"),
        col("year").cast(IntegerType()).alias("year"),
        col("value").cast(LongType()).alias("population_value"),
        col("ingestion_timestamp"),
        col("source_system")
    ).withColumn("processed_timestamp", current_timestamp())

    # Define Silver layer schema
    silver_schema = StructType([
        StructField("country", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("population_value", LongType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("source_system", StringType(), True),
        StructField("processed_timestamp", TimestampType(), True)
    ])

    # Ensure the Silver table exists
    create_idempotent_table(spark, SILVER_TABLE_NAME, silver_schema, ["country", "year", "source_system"], SILVER_STORAGE_PATH)

    # Use upsert for idempotency
    upsert_to_delta(spark, df_silver, SILVER_TABLE_NAME, ["country", "year", "source_system"], SILVER_STORAGE_PATH)

    print(f"Successfully processed {df_silver.count()} population records in Silver layer.")

if __name__ == "__main__":
    try:
        process_silver_population_data()
        send_email_notification(
            subject="Pipeline Success: Silver Population Processing",
            body="The Silver layer population processing completed successfully."
        )
    except Exception as e:
        print(f"Pipeline failed: {e}")
        send_email_notification(
            subject="Pipeline Failure: Silver Population Processing",
            body=f"The Silver layer population processing failed with error: {e}"
        )
        raise
    finally:
        spark.stop()
