# 🕴 Connecticut Real Estate data warehouse project (2026)

## Description:
> An **automated data pipeline** which fetches raw real estate data from Connecticut `SODA 2.0 API` and uploads the data to a `Bigquery` warehouse. From there, an end-to-end medallion architechture `(bronze/raw -> silver -> gold)` was constructed to get cleaned data for further visualizations and dashboarding tools such as `streamlit` or `powerBI`.

>> This whole data pipeline is properly maintained by `Apache Airflow`, a tool that helps in automating the pipeline from data fetch operation to gold layer.

## Problems we solved:

- The medallion architechture fetches the raw data and filters it to more `cleaner` version of the data inside the same architechture, without any need of external database and tools implementation.
The whole architechture is working as a `datalake` and also a `data filtering machine`.

- Without any aggressive data cleaning, we dropped around `~8.5%` of data from the `raw` layer.