# Usage Guide for Data Pipeline

This guide provides instructions on how to execute and monitor the data pipeline. Ensure you have completed the [Setup Guide](setup_guide.md) before proceeding.

## Running the Data Pipeline

The data pipeline consists of several Python scripts that should be executed in a specific order to ensure data flows correctly through the Bronze, Silver, and Gold layers of the Medallion architecture.

### Execution Order

Each step below corresponds to a script in the `notebooks/` directory.

1.  **Bronze Layer Ingestion:**
    *   **Script:** `notebooks/bronze/ingest_user_data.py`
    *   **Purpose:** Simulates raw user data ingestion and writes it to the Bronze layer.
    *   **Action:** Run `python notebooks/bronze/ingest_user_data.py`

2.  **Bronze Layer Population Ingestion:**
    *   **Script:** `notebooks/bronze/ingest_population_data.py`
    *   **Purpose:** Simulates raw population data ingestion and writes it to the Bronze layer.
    *   **Action:** Run `python notebooks/bronze/ingest_population_data.py`

3.  **Silver Layer Processing:**
    *   **Script:** `notebooks/silver/silver_layer_processing.py`
    *   **Purpose:** Cleans, transforms, and enriches data from the Bronze layer, applying business rules and standardizing formats.
    *   **Action:** Run `python notebooks/silver/silver_layer_processing.py`

4.  **Gold Layer Processing (Aggregation):**
    *   **Script:** `notebooks/gold/gold_layer_processing.py`
    *   **Purpose:** Aggregates and curates data from the Silver layer, creating a summarized dataset optimized for consumption.
    *   **Action:** Run `python notebooks/gold/gold_layer_processing.py`

5.  **Gold Layer LLM View Creation:**
    *   **Script:** `notebooks/gold/llm_view_creation.py`
    *   **Purpose:** Creates a specialized JSON view from the Gold layer data, specifically structured and formatted for consumption by Large Language Models (LLMs).
    *   **Action:** Run `python notebooks/gold/llm_view_creation.py`

6.  **Gold Layer BI/Reporting View Creation:**
    *   **Script:** `notebooks/gold/reporting_view_creation.py`
    *   **Purpose:** Creates another specialized view from the Gold layer data, optimized for business intelligence tools and reporting dashboards.
    *   **Action:** Run `python notebooks/gold/reporting_view_creation.py`

## Output Files

After running the pipeline, check the `output/` folder for the results:

- `gold_llm_demographic_summary.json` - JSON output optimized for LLM ingestion
- `gold_user_demographics.csv` - Aggregated user demographics
- `gold_population_stats.csv` - Aggregated population statistics
- `gold_reporting_demographic_summary.csv` - BI/Reporting view

## Data Storage

Data is stored locally in DuckDB databases in the `tmp/duckdb/` directory:
- Bronze layer: `tmp/duckdb/bronze/bronze.duckdb`
- Silver layer: `tmp/duckdb/silver/`
- Gold layer: `tmp/duckdb/gold/`

## Monitoring

### Console Logs

The pipeline logs progress and notifications to the console. Each script sends a notification upon success or failure.

*   **Success Notification Example:**
    ```
    --- EMAIL NOTIFICATION ---
    To: admin@fabric-pipeline.com
    Subject: Pipeline Success: Bronze User Ingestion for random_user_api

    The Bronze layer user ingestion for random_user_api completed successfully.
    ```

### Checking Data

You can inspect the data using DuckDB:

```python
import duckdb
conn = duckdb.connect('tmp/duckdb/gold/gold_user_demographics.duckdb')
print(conn.execute('SELECT * FROM gold_user_demographics').fetchall())
conn.close()
```
