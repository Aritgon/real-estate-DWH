# import the wanted libraries.
import pandas as pd
from google.cloud import bigquery
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

# load_dotenv
# load_dotenv()

# # logging setup.
# logging.basicConfig(
#     level=logging.INFO,
#     format= '%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler("/home/arit/real-estate-DWH/pipeline_main.log"),
#         logging.StreamHandler()
#     ]
# )

# # google client setup.
# PROJECT_ID = os.getenv('GCP_PROJECT_ID')
# DATASET_ID = os.getenv('GCP_DATASET_ID_RAW')
# TABLE_NAME = os.getenv('GCP_TABLE_NAME_RAW')
# TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

# client = bigquery.Client()

# # Helper functions.
# # 1. Function to clean the column names.
# def clean_column_name(key):
#     key = str(key).strip().lower() # STR transformation, striping and lowercasing.
#     bad_char = [":", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-", "+", "=", "{", "}", "[", "]", "|", ";", "'", "<", ">", ",", ".", "?"]
#     for ch in bad_char:
#         key = key.replace(ch, "_") # replacing bad characters with underscore.

#     # if key contains a number at the beginning.
#     if key and key[0].isdigit():
#         key = "col_" + key # adding prefix "col"
        
#     return key

# # 2. Function to safely cast integer values.
# # def safe_cast_int(value):
# #     try:
# #         return int(value)
# #     except:
# #         return None

# # # 3. Function to safely cast float values.
# # def safe_cast_float(value):
# #     try:
# #         return float(value)
# #     except:
# #         return None

# # Main upload function.
# def upload_chunk_to_bigquery(data):
    
#     if not data:
#         logging.warning("received empty batch. Skipping...")
#         return # this will exit the function if the data is empty.

#     try:
#         logging.info(f"starting to upload {len(data)} rows to bigquery...")

#         # ingestion time.
#         loading_time = datetime.now(timezone.utc).isoformat() # returns a timestamp.

#         # cleaned empty list.
#         cleaned_data = []

#         for chunk in data:
#             cleaned_chunk = {}

#             for key, value in chunk.items():
#                 if not key:
#                     logging.warning(f"empty key found in row {chunk}. Skipping this key. Original key: {key}")
#                     continue # if the key is empty, skip this iteration.

#                 cleaned_key = clean_column_name(key) # cleaning the column name.

#                 # minimum type casting.
#                 # if cleaned_key in ["serialnumber", "listyear", "date_recorded"]:
#                 #     cleaned_key = safe_cast_int(value) # changes the value to integer if possible
                
#                 # elif cleaned_key in ["assessedvalue", "saleamount", "salesratio"]:
#                 #     cleaned_key = safe_cast_float(value) if value is not None else None #  changes the value to float
                
#                 # elif cleaned_key in ['town', 'address', 'propertytype', 'residentialtype']:
#                 #     cleaned_key = str(value).strip() if value is not None else None # changes the value to string and strip
                
#                 # fixing value data types and maintaining the original value if the casting fails.
#                 cleaned_key = str(value) if value is not None else None

#                 # adding the cleaned key and value to the cleaned chunk. 
#                 cleaned_chunk[cleaned_key] = value
        
#             # adding the loading time to the cleaned chunk.
#             cleaned_chunk["ingestion_timestamp"] = loading_time

#             # adding the cleaned chunk to the cleaned data list.
#             cleaned_data.append(cleaned_chunk)


#         # upload configuration.
#         job_config = bigquery.LoadJobConfig(
#             # schema=[
#             #    bigquery.SchemaField("serialnumber", "INTEGER"),
#             #    bigquery.SchemaField("listyear", "INTEGER"),
#             #    bigquery.SchemaField("daterecorded", "TIMESTAMP"),
#             #    bigquery.SchemaField("town", "STRING"),
#             #    bigquery.SchemaField("address", "STRING"),
#             #    bigquery.SchemaField("assessedvalue", "FLOAT"),
#             #    bigquery.SchemaField("saleamount", "FLOAT"),
#             #    bigquery.SchemaField("salesratio", "FLOAT"),
#             #    bigquery.SchemaField("propertytype", "STRING"),
#             #    bigquery.SchemaField("residentialtype", "STRING"),
#             # ],
#             autodetect=True, # this will allow bigquery to automatically detect the schema based on the data.
#             write_disposition="WRITE_APPEND",
#             schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
#             # this will allow to add new columns to the schema if there is a new field in the data. (schema evolution)
#         )

#         # this will upload the data from dataframe to bgq dataset.
#         job = client.load_table_from_json(
#             json_rows=cleaned_data,
#             destination=TABLE_ID,
#             job_config=job_config
#         )

#         # this will wait for a confirmation from gcloud.
#         job.result()

#         logging.info(f"successfully uploaded {len(data)} rows to bigquery {TABLE_ID}")

#     except Exception as e:
#         logging.info(f"error occured while uploading data to bgquery -> {e}")


# ## ---------------------------------------------------------------------------------------------------
load_dotenv()

# logging setup.
logging.basicConfig(
    level=logging.INFO,
    format= '%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/home/arit/real-estate-DWH/pipeline_main.log"),
        logging.StreamHandler()
    ]
)

PROJECT_ID=os.getenv("GCP_PROJECT_ID")
DATASET_ID=os.getenv("GCP_DATASET_ID_RAW")
TABLE_NAME=os.getenv("GCP_TABLE_NAME_RAW")

TABLE_ID=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

# bigquery client setup.
client = bigquery.Client()

# this is a test function using pandas to upload the API data to the bigquery dataset.
def upload_to_bigquery(chunk):

    if not chunk:
        logging.warning("received empty batch of data. skipping...")
        raise

    try:
        logging.info(f"Starting bigquery ingestion with {len(chunk)} rows...")

        # cleaning the column names and creating a dataframe.
        df = pd.DataFrame(chunk)

        # this will remove leading and trailing spaces, convert to lowercase and replace bad characters with underscore with regex expression.
        df.columns = df.columns.str.strip().str.lower().str.replace(r'[^a-z0-9]+', '_', regex=True)

        # converting every field to STRING (safer data ingestion).
        df = df.astype(str)

        # adding ingestion timestamp to the dataframe for debugging and monitoring purposes.
        process_time = datetime.now(timezone.utc).isoformat()
        df['ingestion_timestamp'] = process_time

    except Exception as e:
        logging.error(f"error occured while processing the data -> {e}")
        raise

    # upload configuration.
    job_config = bigquery.LoadJobConfig(
        autodetect=True, # autodetect the schema based on the data.
        write_disposition='WRITE_APPEND', # this will append the new data to the existing table. if the table does not exist, it will be created.
        schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION # this will allow to add new columns to the schema if there is a new field in the data. (schema evolution)
    )

    # data uploading to bigquery.
    try:
        job = client.load_table_from_dataframe(
            data=df,
            destination=TABLE_ID,
            job_config=job_config
        )
        job.result() # this will wait for the job to complete.
        logging.info(f"successfully uploaded {len(chunk)} rows to bigquery {TABLE_ID}")
    except Exception as e:
        logging.critical(f"error occured at data ingestion to bigquery raw layer stage : {e}")
        raise

