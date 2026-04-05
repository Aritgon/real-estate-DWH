# import required libraries.
import requests
import time
import logging
from datetime import datetime

# API endpoint.
# URL = "https://data.ct.gov/resource/5mzw-sjtu.json"

# logging setup.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/home/linuxpaglu/real_estate_DWH/pipeline_main.log"),
        logging.StreamHandler()
    ]
)

def fetch_data():
    # parameters.
    API_URL = "https://data.ct.gov/resource/5mzw-sjtu.json"
    LIMIT = 50000 # SOCRATA has support of 50000 rows.
    OFFSET = 0 # page.

    while True:
        try:
            # setting parameters (SOCRATA uses $ for parameters)
            params = {
                "$limit" : LIMIT,
                "$offset" : OFFSET
            }
        
            # fetch the data.
            logging.info(f"Initiating fetch operation at offset : {OFFSET}")
            response = requests.get(API_URL, params=params)
            response.raise_for_status() # raise errors.

            data = response.json() # JSON format.

            # if there is no data left, the loop will break with the last fetched data rows.
            if not data:
                logging.info(f"Data fetch operation is finished.No more data in the source.")
                break

            # using yield to pause the data fetch operation.
            yield data

            # data fetching through pagination technique.
            # the data we are fetching is distributed in rows and pages. 
            OFFSET += LIMIT
            logging.info(f"rows added by far: {OFFSET}")

            # using timer to not get restricted while fetching the data.
            time.sleep(0.25)
            logging.info("putting sleeper of 0.25 seconds...")

        except Exception as e:
            logging.info(f"exception occured at {OFFSET} : {e}")
            break