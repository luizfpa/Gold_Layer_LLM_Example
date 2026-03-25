# Microsoft Fabric Medallion Architecture Data Pipeline

This repository provides a comprehensive solution for building a data pipeline in Microsoft Fabric, adhering to the Medallion architecture (Bronze, Silver, Gold layers). The pipeline is designed for robustness, incorporating idempotency and retry mechanisms, and includes specific outputs for LLM consumption and business intelligence reporting.

## Repository Structure

``` 
. 
├── notebooks/
│   ├── bronze/
│   │   ├── ingest_user_data.py         # Ingest user data from RandomUser API
│   │   └── ingest_population_data.py   # Ingest population data from World Bank API
│   ├── silver/
│   │   └── silver_layer_processing.py  # Silver layer main script
│   ├── gold/
│   │   ├── gold_layer_processing.py   # Gold layer aggregation
│   │   ├── llm_view_creation.py       # Create LLM-friendly JSON view
│   │   └── reporting_view_creation.py  # Create BI view
│   └── utils/
│       └── pipeline_utils.py          # Utility functions
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
├── output/                            # Pipeline output files
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
- **Storage**: DuckDB tables (`bronze_user_data`, `bronze_population_data`).

### 2. Silver Layer (Refined Data)

**Purpose:** Clean, filter, and enrich the raw data from the Bronze layer. This layer provides a "single source of truth" for business concepts.

**Characteristics:**
- Data is cleansed, deduplicated, and standardized.
- Business rules and transformations are applied.
- Schema is enforced.

**Implementation:**
- **Processing**: `silver_layer_processing.py` processes user and population data.
- **Storage**: DuckDB tables (`silver_user_data`, `silver_population_data`).

### 3. Gold Layer (Curated Data)

**Purpose:** Deliver highly curated, aggregated, and denormalized data optimized for specific business applications, such as reporting, analytics, and machine learning.

**Characteristics:**
- Data is highly optimized for consumption.
- Business-specific aggregations and joins are performed.

**Implementation:**
- **Aggregation**: `gold_layer_processing.py` creates aggregated metrics.
- **Views**:
    - **LLM View**: `llm_view_creation.py` creates JSON output optimized for LLM ingestion.
    - **Reporting View**: `reporting_view_creation.py` creates CSV output for BI tools.

## Key Pipeline Features

- **Local Execution**: The pipeline can be run locally using Python and DuckDB without requiring Azure or Microsoft Fabric.
- **Idempotency**: Implemented using DuckDB with PRIMARY KEY constraints.
- **Retry Logic**: Built-in retry mechanisms using `pipeline_utils.py`.
- **Notifications**: Log notifications on success or failure.
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
   python notebooks/bronze/ingest_user_data.py
   python notebooks/bronze/ingest_population_data.py
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

Output files are generated in `output/`:
- `gold_llm_demographic_summary.json` - LLM-optimized JSON output
- `gold_user_demographics.csv` - User demographics aggregation
- `gold_population_stats.csv` - Population statistics
- `gold_reporting_demographic_summary.csv` - BI/Reporting view

## Next Steps

1. Ensure dependencies are installed (`pip install duckdb`).
2. Run the notebooks in the specified order.
3. Check the `output/` folder for results.
