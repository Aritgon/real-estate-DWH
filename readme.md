# Connecticut Real Estate data warehouse project (2026)

[![Python](https://img.shields.io/badge/Python-100%25-blue)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-orange)](https://airflow.apache.org/)
[![BigQuery](https://img.shields.io/badge/Google%20BigQuery-Enterprise-blue)](https://cloud.google.com/bigquery)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

## Description:
An **automated ELT pipeline** designed to ingest, clean, and model `real estate data` from the `SODA 2.0 API` into a **production-ready** `Star Schema` within Google `BigQuery`. This project focuses on `high data integrity` and cost-effective **transformation** using a **Medallion Architecture**.

### Technical Stack
* **Orchestration**: Apache Airflow
* **Data Warehouse**: Google BigQuery
* **Languages/Libraries**: Python, requests, Pandas, SQL (BigQuery)
* **Modeling**: Medallion Architecture (Bronze, Silver, Gold), Star Schema, SCD Type 1

## setup guide

Follow the structured guideline from this [setup guide](setup.md)
---

## Pipeline flow diagram & project features:

> Image below shows the **data flow path** of the pipeline:
<img src="pngs/real estate project flow diagram.png" height="2400" width="1000" alt="Data pipeline structure">

## Project Features & Problems Solved :

### Core Features
1. **API Ingestion**: Data is fetched from the `SODA 2.0 API` and loaded into **BigQuery** using `JobConfig` and `load_table_from_dataframe` to handle Pandas dataframes directly.

2. **Medallion Structure**:
    * **Raw Layer**: Functions as a `data lake`, storing **API data** in its original state without any filters applied.
    * **Silver Layer**: Cleans the data by **removing nulls**, fixing **object anomalies**, and **standardizing formats** for schema implementation.

    * **Gold Layer**: Implements a `star schema` to optimize **query speed** and **dashboard filtering**.

3. **Automation**: The pipeline is fully **automated** using `Apache Airflow`, which triggers data fetching operations on the `1st` and `15th` of `every month`.

4. **Visualization**: Includes **dashboards** designed to track **assessed values and sale amounts across different locations and time periods**.

### Problems Solved
1. **Data Quality**: The architecture successfully filtered out approximately `~8.5%` of `junk` data from the `raw` layer, ensuring the `gold layer` is clean for reporting without requiring aggressive **manual cleaning**.

> The following filter removes `~1%` of data from the `raw` layer:

```
    where assessedvalue is not null and saleamount is not null
    and safe_cast(assessedvalue as FLOAT64) > 0 and safe_cast(saleamount as FLOAT64) > 0
    -- filtering date.
    and parse_date('%Y-%m-%d', LEFT(daterecorded, 10)) between '2001-01-01' and current_date()
    and safe_cast(listyear as INT64) <= extract(year from (parse_date('%Y-%m-%d', LEFT(daterecorded, 10))));
```

> This filter removes `~8.5%` of data by removing numerical outliers from the `silver` layer:

```
    a.assessed_value between 2000 and 2250000
    and a.sale_amount between 2000 and 3050000
    and safe_divide(a.assessed_value, a.sale_amount) between 0 and 1.4
```

2. **Deduplication**: Implemented `SCD` (Slowly Changing Dimension) `Type 1` to **remove duplicate API entries** and **maintain data integrity** in BigQuery.

3. **Efficient Updates**: Used structured `MERGE` and `USING` conditions on the **fact table**. While most columns use a standard `WHEN MATCHED` logic, I applied specific filters to **descriptive** columns (town, assessed_value, sale_amount) to ensure the warehouse only **updates** when actual changes are detected.

4. **Performance**: The `star schema` setup provides **high filtering capabilities** for **BI software** without sacrificing performance or query speed.


> **Apache airflow** tasks workflow:
<img src="pngs/data_pipeline_to_bgq-graph(1).png" height="2400" width="1000" alt ="apache airflow tasks flow">

> Description about the airflow processes:

1. **fetching_started**
    * **Heaviest task of the workflow**. Fetches the `real estate data` from the `API` and stores it into the `raw` stage of **bigquery**

2. **raw_to_silver_cleanup**
    * Extracts `clean` data from `raw` layer by removing around **~1%** of junk data

3. **gold_layer_dim_table_creation**
    * An idempotent `SQL script` to create a `dim` table. In this layer, we have also implemented `slowly changing dimension type 1` for better data entry and duplicate safe approach

4. **gold_layer_fact_table_creation**
    * An idempotent `SQL script` with `slowly changing dimension type 1` implementation to create the `fact` table. This query strongly focuses on `descriptive` data ingestion
---

