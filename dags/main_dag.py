# module importation.
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
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
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

# Phase 2: DAG manager
with DAG(
    dag_id="data_pipeline_to_bgq",
    default_args=default_args,
    start_date=datetime(2026, 4, 9),
    schedule='0 6 1,15 * *',
    template_searchpath=['/home/arit/real-estate-DWH/dags'],
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
        gcp_conn_id="google_cloud_default",
        location="asia-south1",
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

    #Task 3 : Silver -> gold layer (dim_property)
    silver_to_gold_dim_property = BigQueryInsertJobOperator(
        task_id="gold_layer_dim_table_creation",
        gcp_conn_id="google_cloud_default",
        location="asia-south1",
        configuration={
            "query" : {
                "query" : "{% include 'project_utils/dim_property.sql' %}",
                "useLegacySql" : False
            },    
        },
        params={
            "project_id": os.getenv("GCP_PROJECT_ID"),
            "silver_dataset_id": os.getenv("GCP_DATASET_ID_SILVER"),
            "gold_dataset_id": os.getenv("GCP_DATASET_ID_GOLD")
        }
    )

    # Task 4 : Silver to gold layer (fact_real_estate)
    silver_to_gold_fact_real_estate = BigQueryInsertJobOperator(
        task_id="gold_layer_fact_table_creation",
        gcp_conn_id="google_cloud_default",
        location="asia-south1",
        configuration={
            "query" : {
                "query" : "{% include 'project_utils/fact_real_estate.sql' %}",
                "useLegacySql" : False
            },    
        },
        params={
            "project_id": os.getenv("GCP_PROJECT_ID"),
            "silver_dataset_id": os.getenv("GCP_DATASET_ID_SILVER"),
            "gold_dataset_id": os.getenv("GCP_DATASET_ID_GOLD")
        }
    )

# workflow direction and path.
fetch_upload >> raw_to_silver >> silver_to_gold_dim_property >> silver_to_gold_fact_real_estate