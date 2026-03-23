# Usage Guide for Microsoft Fabric Data Pipeline

This guide provides instructions on how to execute and monitor the Microsoft Fabric data pipeline. Ensure you have completed the [Setup Guide](setup_guide.md) before proceeding.

## Running the Data Pipeline

The data pipeline consists of several PySpark notebooks that should be executed in a specific order to ensure data flows correctly through the Bronze, Silver, and Gold layers of the Medallion architecture.

### Execution Order

Each step below corresponds to a notebook in the `notebooks/` directory. You will need to open each notebook in your Microsoft Fabric workspace and run all its cells.

1.  **Bronze Layer Ingestion:**
    *   **Notebook:** `notebooks/bronze/ingest_data.py`
    *   **Purpose:** Simulates raw data ingestion and writes it to the Bronze layer. This notebook also adds metadata like ingestion timestamp and source system.
    *   **Action:** Open `ingest_data.py` and click `Run all`.

2.  **Bronze Layer Processing:**
    *   **Notebook:** `notebooks/bronze/bronze_layer_processing.py`
    *   **Purpose:** Performs initial cleaning and type casting on the raw data from the Bronze layer, preparing it for the Silver layer.
    *   **Action:** Open `bronze_layer_processing.py` and click `Run all`.

3.  **Silver Layer Processing:**
    *   **Notebook:** `notebooks/silver/silver_layer_processing.py`
    *   **Purpose:** Cleans, transforms, and enriches data from the Bronze layer, applying business rules and standardizing formats. The output is a refined dataset in the Silver layer.
    *   **Action:** Open `silver_layer_processing.py` and click `Run all`.

4.  **Gold Layer Processing (Aggregation):**
    *   **Notebook:** `notebooks/gold/gold_layer_processing.py`
    *   **Purpose:** Aggregates and curates data from the Silver layer, creating a summarized dataset optimized for consumption.
    *   **Action:** Open `gold_layer_processing.py` and click `Run all`.

5.  **Gold Layer LLM View Creation:**
    *   **Notebook:** `notebooks/gold/llm_view_creation.py`
    *   **Purpose:** Creates a specialized view from the Gold layer data, specifically structured and formatted for consumption by Large Language Models (LLMs).
    *   **Action:** Open `llm_view_creation.py` and click `Run all`.

6.  **Gold Layer BI/Reporting View Creation:**
    *   **Notebook:** `notebooks/gold/reporting_view_creation.py`
    *   **Purpose:** Creates another specialized view from the Gold layer data, optimized for business intelligence tools and reporting dashboards.
    *   **Action:** Open `reporting_view_creation.py` and click `Run all`.

### Automating Pipeline Execution

In a production environment, you would typically automate the execution of these notebooks using Microsoft Fabric Pipelines (Data Pipelines).

1.  **Create a Data Pipeline:** In your Fabric workspace, navigate to the `Data Factory` persona and create a new `Data pipeline`.
2.  **Add Notebook Activities:** For each step above, add a `Notebook` activity to your data pipeline.
3.  **Configure Dependencies:** Set up dependencies so that notebooks run in the correct sequence (e.g., `bronze_layer_processing.py` runs after `ingest_data.py` completes successfully).
4.  **Schedule the Pipeline:** Configure a schedule for your data pipeline to run automatically at desired intervals.

## Monitoring and Notifications

### Email Notifications

The pipeline is configured to send email notifications to `example@email.com` upon successful completion or failure of each notebook. Monitor your inbox for these alerts to stay informed about the pipeline's status.

*   **Success Notification Example:**
    ```
    Subject: Pipeline Success: Bronze Ingestion for simulated_source
    Body: The Bronze layer ingestion for simulated_source completed successfully.
    ```
*   **Failure Notification Example:**
    ```
    Subject: Pipeline Failure: Silver Layer Processing
    Body: The Silver layer processing failed with error: [Error Details]
    ```

### Fabric Monitoring Hub

Microsoft Fabric provides a comprehensive monitoring hub where you can track the execution status of your notebooks and data pipelines.

1.  **Navigate to Monitoring Hub:** In the left navigation pane of the Fabric portal, click on `Monitoring hub`.
2.  **Review Runs:** Here you can see the status of all your notebook runs and data pipeline executions, including logs, duration, and any errors.

### Checking Data in Lakehouse

After each layer's processing, you can inspect the data directly in your Lakehouse.

1.  **Navigate to Lakehouse:** In your Fabric workspace, open the `MedallionLakehouse` you created.
2.  **Browse Tables:** Under the `Tables` section, you will find the Delta tables created by each notebook (e.g., `bronze_raw_data`, `silver_product_data`, `gold_aggregated_product_data`, `gold_llm_product_summary`, `gold_reporting_product_summary`).
3.  **Query Data:** You can use the built-in SQL endpoint or create new notebooks to query these tables and verify the data quality and transformations.
