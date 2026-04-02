# import the wanted libraries.
import pandas as pd
from google.cloud import bigquery
import logging
from datetime import datetime
from dotenv import load_dotenv
import os

# load_dotenv
load_dotenv()

# logging setup.
logging.basicConfig(
    level=logging.INFO,
    format= '%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/home/linuxpaglu/real_estate_DWH/pipeline_main.log"),
        logging.StreamHandler()
    ]
)

# google client setup.
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("GCP_DATASET_ID_RAW")
TABLE_NAME = os.getenv("GCP_TABLE_NAME_RAW")
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

client = bigquery.Client(project=PROJECT_ID)

def transform_and_upload(data):
    
    if not data:
        logging.warning("received empty batch. Skipping...")
        return

    df = pd.DataFrame(data) # turning the JSON data into a DataFrame.

    # cleaning process.
    try:
        logging.info(f"Starting data cleaning & transformation with row count : {len(df)}")

        # type casting for safer upload to bigquery.
        df = df.astype(str)
        
        # column name fix.
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace(r"[^a-z0-9_]", "", regex=True)
        logging.info('column names fixed.')

    except Exception as e:
        logging.info(f"error occured {e}")
        return

    finally:
        logging.info(f"\n Cleaned rows getting into bigquery : {len(df)}")

    # --- bigquery upload ---
    # uploading the data to bigquery dataset and table.
    try:
        logging.info(f"Starting bigquery ingestion.. with {len(df)} of data.")

        # upload configuration.
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND", 
            autodetect=True, # auto detects and creates the schema.
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION # manages every schema related issues such schema being missed.
            ]
        )

        # this will upload the data from dataframe to bgq dataset.
        job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)

        # this will wait for a confirmation from gcloud.
        job.result()

        logging.info(f"successfully uploaded {len(df)} rows to bigquery {TABLE_ID}")

    except Exception as e:
        logging.info(f"error occured while uploading data to bgquery -> {e}")

# run.
if __name__ == "__main__":
    # call for the fetch_data function.

    try:
        from ingest import fetch_data
    except Exception as e:
        logging.warning(f"error occured while importing fetch_data function: {e}")
    
    data_quantity = 0 # counter
    logging.info("--- Data transformation & ingestion started ---")

    try:
        for chunk in fetch_data():
            transform_and_upload(chunk)
            data_quantity += len(chunk)
            logging.info(f"chunk operation : uploaded {data_quantity} rows to bigquery.")

    except Exception as e:
        logging.critical(f"critical failure -> pipeline crashed : {e}")

    finally:
        logging.info(f"pipeline finished. \n approximate rows added : {data_quantity}")