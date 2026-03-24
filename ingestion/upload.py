# import the wanted libraries.
import pandas as pd
from google.cloud import bigquery
import os

# client setup.
PROJECT_ID = "YOUR PROJECT ID"
DATASET_ID = "DATASET ID"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.table_name"

client = bigquery.client(PROJECT_ID)

def transform_and_upload(data):
    
    if not data:
        return

    df = pd.DataFrame(data) # pandas dataframe.

    # small cleaning process.
    # column name fix.
    df.columns = df.columns.replace(r'[a-zA-Z0-9_]', "", regex=True)

    # dropping duplicates as a whole.
    df = df.drop_duplicates()

    # dropping nulls based on a certain criteria.
    df = df.dropna(subset=['assessedvalue', 'salesamount'])

    # converting all data to string for safer data ingestion approach.
    df = df.astype(str)

    # uploading the data to bigquery dataset and table.
    try:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=TRUE)
        job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
        job.result()
        print(f"successfully uploaded{len(df)} rows to bigquery {table_id}")
    except Exception as e:
        print(f"we have faced issue {e}")


# run.
if __name__ == "__main__":
    # call for the fetch_data function.
    from ingest.py import fetch_data

    data_quantity = 0 # counter
    print("Starting timeline...")

    try:
        for chunk in fetch_data():
            transform_and_upload(chunk)
            data_quantity += len(chunk)

    finally:
        print(f"pipeline finished. \n approximate rows added : {data_quantity}")