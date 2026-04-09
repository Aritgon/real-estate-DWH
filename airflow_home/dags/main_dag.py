# module importation.
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryColumnCheckOperator, BigQueryValueCheckOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import os

# load_dotenv()
load_dotenv()

# Phase 1: define the python scripts to fetch data from API and put it into a bigquery project.dataset.table_name
def fetch_and_upload():
    # import functions as callable modules.
    try:
        from project_utils.upload import transform_and_upload
        from project_utils.ingest import fetch_data
    except Exception as e:
        logging.error(f"Import problem occured (orchestration phase): {e}")
        raise # raise the issue in airflow UI.

    # data quality variable.
    data_content = 0
    logging.info("----- Initiating data fetch & uploading operation -----")

    try:
        for chunk in fetch_data(): # chunk why? we used yield generator to pause continuous data fetching after a batch.
            transform_and_upload(chunk)
            data_content += len(chunk)
            logging.info(f"uploaded {data_content} rows...")
    except Exception as e:
        logging.critical(f"Fatal pipeline issue at {data_content} : {e}")
        raise
    finally:
        logging.info(f"we've successfully moved {data_content} rows of data towards bigquery dataset.")


# setting up default args.
default_args = {
    'owner' : "arit gon",
    'depends_on_past' : False,
    # 'retries' : 2,
    # 'retry_delay' : timedelta(minutes=5)
}

# Phase 2: DAG manager
with DAG(
    dag_id="data_pipeline_to_bgq",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule='0 6 1,15 * *',
    template_searchpath=['/home/linuxpaglu/real_estate_DWH/airflow_home/dags'],
    catchup=False
) as dag:

    # Task 1 : fetch operation & bigquery upload in raw layer of our medallion layer.
    fetch_upload = PythonOperator(
        task_id = 'fetching_started',
        python_callable=fetch_and_upload
    )

    # Task 2 : raw -> silver data upload.
    raw_to_silver = BigQueryInsertJobOperator(
        task_id="raw_to_silver_cleanup",
        configuration={
            "query" : {
                "query": "{% include 'project_utils/transform_to_silver.sql' %}",
                "useLegacySql": False,
            },
        },
        params={
             "project_id": os.getenv("GCP_PROJECT_ID"),
             "raw_dataset_id": os.getenv("GCP_DATASET_ID_RAW"),
             "silver_dataset_id": os.getenv("GCP_DATASET_ID_SILVER")
        }
    )

    # # data checks before commiting cleaned data to the gold layer.
    # quality_check_columns = BigQueryColumnCheckOperator(
    #     task_id="columnar_data_check",
    #     table=f"{os.getenv('GCP_PROJECT_ID')}.{os.getenv('GCP_DATASET_ID_SILVER')}.real_estate_silver",
    #     column_mapping={
    #         "serial_number" : {"null_check" : {"equal_to": 0}}, # no more null serial number.

    #         "assessed_value" : {
    #             "max" : {"less_than" : 2250001},
    #             "min" : {"greater_than" : 2000}
    #         },
    #         "sale_amount" : {
    #             "max" : {"less_than" : 3050001},
    #             "min" : {"greater_than" : 2000}
    #         },
    #         "sales_ratio": {
    #             "max": {"less_than": 1.41},
    #             "min": {"greater_than": 0} 
    #         }
    #     },
    #     use_legacy_sql=False
    # )


    # Task 3 : Silver -> gold (Implementing star schema for a complete data warehouse)
    silver_to_gold = BigQueryInsertJobOperator(
        task_id="silver_to_gold",
        configuration={
            "query" : {
                "query" : "{% include 'project_utils/silver_to_gold.sql' %}",
                "useLegacySql": False,
            },
        },
        params={
            "project_id": os.getenv("GCP_PROJECT_ID"),
            "silver_dataset_id": os.getenv("GCP_DATASET_ID_SILVER"),
            "gold_dataset_id": os.getenv("GCP_DATASET_ID")
        }
    )

# workflow direction and path.
fetch_upload >> raw_to_silver >> silver_to_gold