# 🕴 Connecticut Real Estate data warehouse project (2026)

## Description:
> An **automated data pipeline** which fetches raw real estate data from Connecticut `SODA 2.0 API` and uploads the data to a `Bigquery` warehouse. From there, an end-to-end medallion architechture `(bronze/raw -> silver -> gold)` was constructed to get cleaned data for further visualizations and dashboarding tools such as `streamlit` or `powerBI`.

>> This whole data pipeline is properly maintained by `Apache Airflow`, a tool that helps in automating the pipeline from data fetch operation to gold layer.

## Problems solved:

- The medallion architechture fetches the raw data and filters it to more `cleaner` version of the data inside the same architechture, without any need of external database and tools implementation.
The whole architechture is working as a `datalake` and also a `data filtering machine`.

- Without any aggressive data cleaning, the datalake dropped around `~8.5%` of data from the `raw` layer for the `gold` layer ready for dashboard features.

- With the help of `airflow`, this whole data pipeline is automated and it will fetch newer data from the API on `1st` and `15th` of every month.

- Implementation of `SCD` (slowly changing dimensity) `type 1` helps in getting rid of duplicate entries from the API, maintaining the clean data in bigquery.

- Implemented curately structured `MERGE` and `USING` conditions on `fact` table. Used `WHEN MATCHED` on most repeated columns but put filter on descriptive columns such as `town`, `assessed_value`, `sale_amount` etc to get only newer data if it is changed, otherwise the warehouse will update newer data.

- Designed a simple star schema for better dashboard filtering on powerBI and performance.
--------------------------------------------------------------------------

## Data flow diagram

> Image below shows the data flow path of the pipeline

![Data Flow Diagram]("pngs/real estate project flow diagram.png")

