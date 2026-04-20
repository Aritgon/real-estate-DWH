# Connecticut Real Estate data warehouse project (2026)

[![Python](https://img.shields.io/badge/Python-100%25-blue)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-orange)](https://airflow.apache.org/)
[![BigQuery](https://img.shields.io/badge/Google%20BigQuery-Enterprise-blue)](https://cloud.google.com/bigquery)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

## Description:
> An **automated data pipeline**, which fetches messy real estate data from connecticut `SODA 2.0` API and uploads the data to a `bigquery` warehouse. A well-curated `medallion architecture` filters the raw data and establishes `star schema` in the `gold` layer of the warehouse to have cleaned, non-duplicate data for dashboard designing.

## Project Features

This designed pipeline is responsible to collect raw data from the API to filter out **junk** data and create an analytics-ready `star schema vault`. The following are the main features of the project -

1. Fetches data from `SODA 2.0` API and loads it straight into Google Bigquery by Bigquery specific functions such as `JobConfig` to configure the data ingestion and `load_table_from_dataframe` to upload the as **pandas dataframe**.

2. Implements a Medallion architecture with three clear layers:
    1. **Raw layer**: Acts as a simple **data lake**. It stores the data exactly as it comes from the `SODA 2.0 API`, without any changes and filters.

    2. **Silver layer**: Cleans the data by removing *null values*, fixing *random object anomalies*, and *standardizing formats*. This layers makes the data usable and ready for `star schema implementation`.

    3. **Gold layer**: Builds a clean and simple **star schema**. This makes querying faster and allows smooth filtering when building dashboards.

3. Creates easy-to-use dashboards that let you track **assessed values and sale amounts** over the **years** and across different **places**.

## Data flow diagram

> Image below shows the **data flow path** of the pipeline:
<img src="pngs/real estate project flow diagram.png" height="2400" width="1000" alt="Data pipeline structure">

### Description about the workflow:

1. API fetch operation from `SODA 2.0 API` using python and libraries such as `pandas`, `requests` to upload the data to a **bigquery** dataset in `RAW` layer

2. Later the following filter was introduced to get `~1%` of junk data filtered out in the `SILVER` layer
    * ```
        where assessedvalue is not null and saleamount is not null
        and safe_cast(assessedvalue as FLOAT64) > 0 and safe_cast(saleamount as FLOAT64) > 0
        and parse_date('%Y-%m-%d', LEFT(daterecorded, 10)) between '2001-01-01' and current_date()
        and safe_cast(listyear as INT64) <= extract(year from (parse_date('%Y-%m-%d', LEFT(daterecorded, 10))));
    ```
3. In the final stage or `GOLD` layer, A simply structured **star schema** was implemented to have better **data profiling** and **filtering capabilities** for `BI` softwares. I have also put filter out *outliers* and data anomalies that could've skewed the analysis
    * ```
    a.assessed_value between 2000 and 2250000
    and a.sale_amount between 2000 and 3050000
    and safe_divide(a.assessed_value, a.sale_amount) between 0 and 1.4
    ```

    * This filter removes around **~8.5%** of junk data from the `SILVER` layer

> **Apache airflow** tasks workflow:
<img src="pngs/data_pipeline_to_bgq-graph(1).png" height="2400" width="1000" alt ="apache airflow tasks flow">

> Description about the airflow processes:

1. **fetching_started**
    * *Heaviest task among all tasks*. Fetches the `real estate data` from the `API` and stores it into the `raw` stage of **bigquery**

2. **raw_to_silver_cleanup**
    * Extracts `clean` data from `raw` layer by removing around **~1%** of junk data

3. **gold_layer_dim_table_creation**
    * An idempotent `SQL script` to create a `dim` table. In this layer, we have also implemented `slowly changing dimension type 1` for better data entry and duplicate safe approach

4. **gold_layer_fact_table_creation**
    * An idempotent `SQL script` with `slowly changing dimension type 1` implementation to create the `fact` table. This query strongly focuses on `descriptive` data ingestion
----

## Problems solved:

- The medallion architechture fetches the raw data and filters it to more `cleaner` version of the data inside the same architechture, without any need of external database and tools implementation. The whole architechture is working as a `datalake` and also a `data filtering machine`.

- Without any aggressive data cleaning, the datalake dropped around `~8.5%` of data from the `raw` layer for the `gold` layer ready for dashboard features.

- With the help of `airflow`, this whole data pipeline is automated and it will fetch newer data from the API on **1st** and **15th** of every month.

- Implementation of `SCD` (slowly changing dimensity) `type 1` helps in getting rid of duplicate entries from the API, maintaining the clean data in bigquery.

- Implemented curately structured `MERGE` and `USING` conditions on `fact` table. Used `WHEN MATCHED` on most repeated columns but put filter on descriptive columns such as `town`, `assessed_value`, `sale_amount` etc to get only newer data if it is changed, otherwise the warehouse will update newer data.

- Designed a simple star schema for better dashboard filtering on powerBI and performance.

----

