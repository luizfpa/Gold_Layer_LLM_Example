import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType

# Import utility functions
sys.path.append("../utils")
from pipeline_utils import retry, send_email_notification, create_idempotent_table, upsert_to_delta, config, get_storage_path, create_spark_session

# Initialize Spark Session
spark = create_spark_session("GoldReportingViewCreation")

# Load Configuration
gold_config = config.get('gold_layer', {})
GOLD_LAYER_PATH = gold_config.get('base_path', 'abfss://gold@medallionpipeline.dfs.core.windows.net/')
GOLD_AGGREGATED_USER_TABLE = gold_config.get('aggregated_user_data_table_name', 'gold_user_demographics')
GOLD_REPORTING_VIEW_TABLE = gold_config.get('reporting_view_table_name', 'gold_reporting_demographic_summary')

# Resolve paths based on local mode
GOLD_STORAGE_PATH = get_storage_path(GOLD_LAYER_PATH)
GOLD_AGGREGATED_PATH = GOLD_STORAGE_PATH

@retry(exceptions=(Exception,), tries=3, delay=5)
def create_reporting_view():
    print(f"Reading aggregated user data from Gold layer table: {GOLD_AGGREGATED_USER_TABLE}")
    df_gold = spark.read.format("delta").load(GOLD_AGGREGATED_PATH)

    # --- Reporting View Transformations ---
    # Add simple metrics and formatting
    df_reporting = df_gold.select(
        col("country"),
        col("gender"),
        col("total_users"),
        col("average_age"),
        (col("total_users") * 100).alias("user_percentage") # Mock metric
    ).orderBy(col("total_users").desc())

    # Define schema
    reporting_schema = StructType([
        StructField("country", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("total_users", LongType(), True),
        StructField("average_age", IntegerType(), True),
        StructField("user_percentage", IntegerType(), True)
    ])

    # Ensure the table exists
    create_idempotent_table(spark, GOLD_REPORTING_VIEW_TABLE, reporting_schema, ["country", "gender"], GOLD_STORAGE_PATH)

    # Use upsert for idempotency
    upsert_to_delta(spark, df_reporting, GOLD_REPORTING_VIEW_TABLE, ["country", "gender"], GOLD_STORAGE_PATH)

    print(f"Successfully created reporting view with {df_reporting.count()} records.")

if __name__ == "__main__":
    try:
        create_reporting_view()
        send_email_notification(
            subject="Pipeline Success: Gold Reporting View Creation",
            body="The Gold layer reporting view creation completed successfully."
        )
    except Exception as e:
        print(f"Pipeline failed: {e}")
        send_email_notification(
            subject="Pipeline Failure: Gold Reporting View Creation",
            body=f"The Gold layer reporting view creation failed with error: {e}"
        )
        raise
    finally:
        spark.stop()
