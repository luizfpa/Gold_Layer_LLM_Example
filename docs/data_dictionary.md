# Data Dictionary

This document provides a data dictionary for the tables created in each layer of the Medallion architecture within this data pipeline.

## Bronze Layer

### `bronze_raw_data`

This table stores the raw, untransformed data ingested from source systems.

| Column Name         | Data Type     | Description                                     | Example Value             |
| :------------------ | :------------ | :---------------------------------------------- | :------------------------ |
| `id`                | `Integer`     | Unique identifier from the source system.       | `1`                       |
| `name`              | `String`      | Name of the product or entity.                  | `ProductA`                |
| `value`             | `String`      | Raw value associated with the product/entity.   | `"100"`                   |
| `timestamp`         | `String`      | Raw timestamp string from the source.           | `"2023-01-01 10:00:00"`   |
| `ingestion_timestamp` | `Timestamp`   | Timestamp when the record was ingested into Bronze. | `2023-01-01 10:30:00.123` |
| `source_system`     | `String`      | Identifier of the source system.                | `simulated_source`        |

### `bronze_processed_data`

This table stores the raw data after initial type casting and column renaming, still within the Bronze layer.

| Column Name         | Data Type     | Description                                     | Example Value             |
| :------------------ | :------------ | :---------------------------------------------- | :------------------------ |
| `product_id`        | `Integer`     | Unique product identifier.                      | `1`                       |
| `product_name`      | `String`      | Name of the product.                            | `ProductA`                |
| `raw_value`         | `String`      | Original raw value.                             | `"100"`                   |
| `event_timestamp`   | `Timestamp`   | Timestamp of the event, cast to TimestampType.  | `2023-01-01 10:00:00`     |
| `ingestion_timestamp` | `Timestamp`   | Timestamp when the record was ingested into Bronze. | `2023-01-01 10:30:00.123` |
| `source_system`     | `String`      | Identifier of the source system.                | `simulated_source`        |

## Silver Layer

### `silver_product_data`

This table contains cleaned, transformed, and conformed product data.

| Column Name         | Data Type     | Description                                     | Example Value             |
| :------------------ | :------------ | :---------------------------------------------- | :------------------------ |
| `product_id`        | `Integer`     | Unique product identifier.                      | `1`                       |
| `product_name`      | `String`      | Cleaned and standardized product name (lowercase, trimmed). | `producta`                |
| `product_value`     | `Double`      | Numeric value associated with the product.      | `100.0`                   |
| `event_timestamp`   | `Timestamp`   | Timestamp of the event.                         | `2023-01-01 10:00:00`     |
| `ingestion_timestamp` | `Timestamp`   | Timestamp when the record was ingested into Bronze. | `2023-01-01 10:30:00.123` |
| `source_system`     | `String`      | Identifier of the source system.                | `simulated_source`        |

## Gold Layer

### `gold_aggregated_product_data`

This table stores aggregated product data, summarized by activity date and product name.

| Column Name             | Data Type     | Description                                     | Example Value             |
| :---------------------- | :------------ | :---------------------------------------------- | :------------------------ |
| `activity_date`         | `Timestamp`   | Date of the aggregated activity (truncated to day). | `2023-01-01 00:00:00`     |
| `product_name`          | `String`      | Product name.                                   | `producta`                |
| `daily_unique_products` | `Long`        | Count of unique products for the day.           | `2`                       |
| `daily_total_value`     | `Double`      | Sum of `product_value` for the day.             | `210.0`                   |
| `daily_average_value`   | `Double`      | Average of `product_value` for the day.         | `105.0`                   |

### `gold_llm_product_summary`

This table provides a view optimized for Large Language Model (LLM) consumption, including a descriptive summary text.

| Column Name             | Data Type     | Description                                     | Example Value             |
| :---------------------- | :------------ | :---------------------------------------------- | :------------------------ |
| `activity_date`         | `Timestamp`   | Date of the aggregated activity.                | `2023-01-01 00:00:00`     |
| `product_name`          | `String`      | Product name.                                   | `producta`                |
| `daily_unique_products` | `Long`        | Count of unique products for the day.           | `2`                       |
| `daily_total_value`     | `Double`      | Sum of `product_value` for the day.             | `210.0`                   |
| `daily_average_value`   | `Double`      | Average of `product_value` for the day.         | `105.0`                   |
| `product_summary_text`  | `String`      | A natural language summary of the product's daily activity. | `On 2023-01-01 00:00:00 the product producta had 2 unique activities, with a total value of 210.0 and an average value of 105.0.` |

### `gold_reporting_product_summary`

This table provides a view optimized for BI and reporting tools, with additional time dimensions.

| Column Name             | Data Type     | Description                                     | Example Value             |
| :---------------------- | :------------ | :---------------------------------------------- | :------------------------ |
| `activity_date`         | `Timestamp`   | Date of the aggregated activity.                | `2023-01-01 00:00:00`     |
| `activity_year`         | `Integer`     | Year of the activity.                           | `2023`                    |
| `activity_month`        | `Integer`     | Month of the activity.                          | `1`                       |
| `activity_day`          | `Integer`     | Day of the month of the activity.               | `1`                       |
| `product_name`          | `String`      | Product name.                                   | `producta`                |
| `daily_unique_products` | `Long`        | Count of unique products for the day.           | `2`                       |
| `daily_total_value`     | `Double`      | Sum of `product_value` for the day.             | `210.0`                   |
| `daily_average_value`   | `Double`      | Average of `product_value` for the day.         | `105.0`                   |
