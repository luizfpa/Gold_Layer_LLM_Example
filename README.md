# Microsoft Fabric Medallion Architecture Data Pipeline

This repository provides a comprehensive solution for building a data pipeline in Microsoft Fabric, adhering to the Medallion architecture (Bronze, Silver, Gold layers). The pipeline is designed for robustness, incorporating idempotency and retry mechanisms, and includes specific outputs for LLM consumption and business intelligence reporting.

## Repository Structure

``` 
. 
├── notebooks/
│   ├── bronze/
│   │   ├── ingest_data.py              # Ingest raw data
│   │   ├── ingest_user_data.py         # Ingest user data from RandomUser API
│   │   └── ingest_population_data.py   # Ingest population data from World Bank API
│   ├── silver/
│   │   ├── silver_layer_processing.py  # Silver layer main script
│   │   ├── process_user_data.py        # Clean and standardize user data
│   │   └── process_population_data.py  # Clean and standardize population data
│   ├── gold/
│   │   ├── gold_layer_processing.py   # Gold layer main script
│   │   ├── llm_view_creation.py        # Create LLM-friendly view
│   │   ├── reporting_view_creation.py  # Create BI view
│   │   ├── aggregate_user_data.py     # Aggregate user demographics
│   │   ├── aggregate_population_data.py # Aggregate population stats
│   │   ├── create_llm_demographic_view.py   # Create LLM-friendly view
│   │   └── create_reporting_demographic_view.py # Create BI view
│   └── utils/
│       └── pipeline_utils.py          # Utility functions (Retry, Idempotency)
├── docs/
│   ├── architecture_overview.md
│   ├── setup_guide.md
│   ├── usage_guide.md
│   └── data_dictionary.md
├── config/
│   └── pipeline_config.json           # Central configuration
├── data/
│   ├── users.csv                      # Sample user data
│   └── population.csv                 # Sample population data
├── .gitignore
├── Gold_Layer_LLM_Ingestion.code-workspace
└── README.md
```

## Data Pipeline Architecture (Medallion Model)

The pipeline follows the Medallion architecture, which logically organizes data into three layers: Bronze, Silver, and Gold. This approach promotes data quality, governance, and accessibility.

### 1. Bronze Layer (Raw Data)

**Purpose:** Ingest raw data from various sources with minimal transformations. This layer serves as a historical archive of the original data.

**Characteristics:**
- Data is stored in its original format (CSV).
- Schema is loosely enforced.
- Data is immutable and retained for auditing and reprocessing.

**Implementation:**
- **Ingestion**: `ingest_user_data.py` and `ingest_population_data.py` read raw data and add metadata.
- **Storage**: Delta Lake tables (`bronze_user_data`, `bronze_population_data`).

### 2. Silver Layer (Refined Data)

**Purpose:** Clean, filter, and enrich the raw data from the Bronze layer. This layer provides a "single source of truth" for business concepts.

**Characteristics:**
- Data is cleansed, deduplicated, and standardized.
- Business rules and transformations are applied.
- Schema is enforced.

**Implementation:**
- **Processing**: `process_user_data.py` cleans user data (trim, lowercase). `process_population_data.py` casts types.
- **Storage**: Delta Lake tables (`silver_user_data`, `silver_population_data`).

### 3. Gold Layer (Curated Data)

**Purpose:** Deliver highly curated, aggregated, and denormalized data optimized for specific business applications, such as reporting, analytics, and machine learning.

**Characteristics:**
- Data is highly optimized for consumption.
- Business-specific aggregations and joins are performed.

**Implementation:**
- **Aggregation**: `aggregate_user_data.py` and `aggregate_population_data.py` create aggregated metrics.
- **Views**:
    - **LLM View**: `create_llm_demographic_view.py` creates natural language summaries.
    - **Reporting View**: `create_reporting_demographic_view.py` creates BI-optimized views.

## Key Pipeline Features

- **Local Execution**: The pipeline can be run locally using Python and DuckDB without requiring Azure or Microsoft Fabric.
- **Idempotency**: Implemented using Delta Lake `MERGE INTO` operations.
- **Retry Logic**: Built-in retry mechanisms using `pipeline_utils.py`.
- **Notifications**: Email notifications on success or failure.
- **Configuration-driven**: All parameters managed via `pipeline_config.json`.

## Running Locally

The pipeline is designed to run locally using **DuckDB** as a lightweight embedded database, making it easy to develop and test without cloud infrastructure.

### Prerequisites

- Python 3.9+
- DuckDB (`pip install duckdb`)

### Execution Order

Run the scripts in the following order:

1. **Bronze Layer:**
   ```bash
   python notebooks/bronze/ingest_data.py
   python notebooks/bronze/bronze_layer_processing.py
   ```

2. **Silver Layer:**
   ```bash
   python notebooks/silver/silver_layer_processing.py
   ```

3. **Gold Layer:**
   ```bash
   python notebooks/gold/gold_layer_processing.py
   python notebooks/gold/llm_view_creation.py
   python notebooks/gold/reporting_view_creation.py
   ```

Data is stored in `tmp/duckdb/` (already ignored by `.gitignore`).

## Next Steps

1. Ensure dependencies are installed (PySpark, Delta Lake).
2. Update `config/pipeline_config.json` with your Azure Storage Account details.
3. Run the notebooks in the specified order:
    - Bronze: `ingest_user_data.py`, `ingest_population_data.py`
    - Silver: `process_user_data.py`, `process_population_data.py`
    - Gold: `aggregate_user_data.py`, `aggregate_population_data.py`, `create_llm_demographic_view.py`, `create_reporting_demographic_view.py`
