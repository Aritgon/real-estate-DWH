🏗️ Connecticut Real Estate Data Pipeline (ELT)

An End-to-End Data Engineering Project: From API Ingestion to Layered Data Warehouse
📖 Project Overview

This project implements a modern ELT (Extract, Load, Transform) pipeline. It fetches 1.14 million real estate transaction records from the Connecticut State Open Data API, ingests them into a Google BigQuery Raw layer, and performs data cleaning and standardization to create a structured "Silver" layer for analytics.
🛠️ The Tech Stack

    Language: Python 3.x

    Data Warehouse: Google BigQuery

    Libraries: pandas, google-cloud-bigquery, requests

    Infrastructure: Linux (Ubuntu) environment

🚀 1. Data Ingestion (The "Extract & Load")

The ingestion was built using a custom Python script (ingest.py and upload.py) to handle the Socrata Open Data API.
The Process:

    Paginated Fetching: Since the API limits responses to 50k rows per request, I implemented a while loop with $offset and $limit parameters to paginate through 1.1 million rows.

    Buffer & Yield: Used Python generators (yield) to stream data in chunks, preventing memory overflow on the local machine.

    Automated Upload: Used the google-cloud-bigquery library to stream DataFrames directly into a "Bronze/Raw" table.

⚠️ Challenges & Solutions:

    The "Missing Column" Trap: Historical data (2001–2019) was missing columns like property_type that only appeared in newer records.

        Solution: Implemented job_config.schema_update_options = [ALLOW_FIELD_ADDITION] to allow the BigQuery table to evolve as new columns appeared in later batches.

    Rate Limiting: Frequent API calls led to potential IP throttling.

        Solution: Added a time.sleep(0.25) backoff to respect the API's rate limits.

    Schema Mismatches: Pandas often guessed types incorrectly (e.g., mixing strings and dicts in the location column).

        Solution: Forced all raw data to string format during the initial load to ensure 100% ingestion success, deferring type-casting to the SQL layer.

🧹 2. Data Transformation (The "Silver Layer")

Once the data landed in BigQuery, I shifted to SQL-based transformations to create a high-quality "Silver" layer.
Key Operations:

    Normalization: Converted town, address, and property_type to lowercase and trimmed whitespace for consistency.

    Type Casting: Converted raw strings into proper BigQuery types (INT64 for years, FLOAT64 for amounts, and DATE for timestamps).

    Data Validation:

        Filtered out "junk" records (sales < $100).

        Used SAFE_CAST and SAFE.PARSE_DATE to prevent the pipeline from crashing on malformed strings.

        Cleaned up Socrata-specific metadata (columns starting with computed_).

📈 3. Data Warehouse Architecture

The project follows the Medallion Architecture:

    Bronze (Raw): Immutable, raw strings from the API. Includes metadata and messy columns.

    Silver (Staged): Cleaned, deduplicated, and strictly typed data. Ready for joining with other datasets.

    Gold (Analytics): (Planned) Aggregated views for dashboards, such as "Year-over-Year Growth by Town."

🧠 Lessons Learned

    ELT > ETL: It is much more efficient to load "messy" data into BigQuery and clean it with SQL than to try and write perfect Python cleaning logic that crashes on every API anomaly.

    Schema Evolution: Real-world APIs are not static; your warehouse must be designed to handle new fields appearing mid-stream.

    Persistence: Moving 1.1 million rows requires handling silent failures and understanding API offsets.

🎯 Next Steps (The "Hard" Stuff)

    Orchestration: Moving from manual execution to Apache Airflow to schedule the pipeline.

    dbt: Implementing dbt to manage the SQL transformations with version control and automated testing.

    CI/CD: Setting up GitHub Actions to deploy SQL changes automatically to BigQuery.