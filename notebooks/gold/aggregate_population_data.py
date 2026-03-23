import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, min as spark_min, avg as spark_avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType

# Import utility functions
sys.path.append("../utils")
from pipeline_utils import retry, send_email_notification, create_idempotent_table, upsert_to_delta, config, get_storage_path, create_spark_session

# Initialize Spark Session
spark = create_spark_session("GoldPopulationAggregation")

# Load Configuration
silver_config = config.get('silver_layer', {})
gold_config = config.get('gold_layer', {})
SILVER_LAYER_PATH = silver_config.get('base_path', 'abfss://silver@medallionpipeline.dfs.core.windows.net/')
GOLD_LAYER_PATH = gold_config.get('base_path', 'abfss://gold@medallionpipeline.dfs.core.windows.net/')
SILVER_TABLE_NAME = silver_config.get('population_data_table_name', 'silver_population_data')
GOLD_TABLE_NAME = gold_config.get('aggregated_population_data_table_name', 'gold_population_stats')

# Resolve paths based on local mode
SILVER_STORAGE_PATH = get_storage_path(SILVER_LAYER_PATH)
GOLD_STORAGE_PATH = get_storage_path(GOLD_LAYER_PATH)

@retry(exceptions=(Exception,), tries=3, delay=5)
def process_gold_population_data():
    print(f"Reading population data from Silver layer table: {SILVER_TABLE_NAME}")
    df_silver = spark.read.format("delta").load(SILVER_STORAGE_PATH)

    # --- Gold Layer Transformations ---
    # Aggregate population stats
    df_gold = df_silver.groupBy(
        col("country")
    ).agg(
        spark_max(col("population_value")).alias("max_population"),
        spark_min(col("population_value")).alias("min_population"),
        spark_avg(col("population_value")).alias("average_population")
    ).orderBy(col("max_population").desc())

    # Define Gold layer schema
    gold_schema = StructType([
        StructField("country", StringType(), True),
        StructField("max_population", LongType(), True),
        StructField("min_population", LongType(), True),
        StructField("average_population", IntegerType(), True)
    ])

    # Ensure the Gold table exists
    create_idempotent_table(spark, GOLD_TABLE_NAME, gold_schema, ["country"], GOLD_STORAGE_PATH)

    # Use upsert for idempotency
    upsert_to_delta(spark, df_gold, GOLD_TABLE_NAME, ["country"], GOLD_STORAGE_PATH)

    print(f"Successfully processed {df_gold.count()} aggregated population records in Gold layer.")

if __name__ == "__main__":
    try:
        process_gold_population_data()
        send_email_notification(
            subject="Pipeline Success: Gold Population Aggregation",
            body="The Gold layer population aggregation completed successfully."
        )
    except Exception as e:
        print(f"Pipeline failed: {e}")
        send_email_notification(
            subject="Pipeline Failure: Gold Population Aggregation",
            body=f"The Gold layer population aggregation failed with error: {e}"
        )
        raise
    finally:
        spark.stop()
