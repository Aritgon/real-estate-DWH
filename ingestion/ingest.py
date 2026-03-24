# import required libraries.
import requests

# API endpoint.
# URL = "https://data.ct.gov/resource/5mzw-sjtu.json"

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
                "$offset" : OFFSET,
                "$order" : ":id" 
            }
        
            # fetch the data.
            print(f"starting data fetch operation, at {OFFSET}")
            response = requests.get(API_URL, params=params)
            response.raise_for_status() # raise errors.

            data = response.json() # JSON format.and

            # if there is no data left, the loop will break with the last fetched data rows.
            if not data:
                print(f"data fetch operation is finished.")
                break

            # using yield to pause the data fetch operation.
            yield data

            # keeping the loop for pagination.
            OFFSET += LIMIT
            print(f"rows added : {OFFSET}")

        except Exception as e:
            print(f"exception occured at {OFFSET} : {e}")
            break
            
        finally:
            print(f"we have uploaded {OFFSET} data to the table")
            
