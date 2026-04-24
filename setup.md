## A guide to run this project on your machine 🚀

---
## ---- FIRST and FOREMOST task to be completed  ----

1. Install a `PYTHON ENVIRONMENT` in your machine. Run this block of code to install the required packages and the environment - 

```
python -m venv venv
source venv/bin/activate  # Windows: venv\\Scripts\\activate
pip install -r requirements.txt
```
2. This above script will install `google cloud` and it's supporting libraries along with `bigquery`.

After that run this code to initiate `google authentication login` in your machine.

``` 
    gcloud auth application-default login
```

**please ensure you have `GOOGLE CLOUD SDK` installed in your machine**, if not install it from this link -

[link to download to Gcloud CLI](https://docs.cloud.google.com/sdk/docs/install-sdk)

*************************************************************************************
#### Install Gcloud CLI tool (Terminal way) 

Run this blocks of codes one by one -

* update packages and dependency installation
```
sudo apt-get update
sudo apt-get install apt-transport-https ca-certificates gnupg curl -y
```

* Add google cloud public key
```
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
```

* Add the Gcloud CLI repository
```
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
```

* Install the SDK
```
sudo apt-get update && sudo apt-get install google-cloud-sdk -y
```

* Initialize

```
gcloud init
```

> Log in with your gmail so that every project can be opened under that mail and used for future project building. 


---

## ---- Second Task : AIRFLOW home & user setup ----

1. Setup the `HOME` directory for `airflow` files
2. Initiate the airflow database

> run this block of code :

```
export AIRFLOW_HOME=$(pwd)/airflow
airflow migrate db
```

3. Create Airflow User

```
airflow users create \\
    --username admin \\
    --firstname YourName \\
    --lastname YourLastName \\
    --role Admin \\
    --email admin@example.com
```

### If you face a user creation issue, follow this next step.

1. Run the following code to fix the user creation hassle for the project.

```
airflow standalone
```

---
## ---- Second task : AIRFLOW ACCOUNT AND PROJECT PATH SETUP ----

### Configure GCP Connection

1. Launch the Airflow UI: 
```
airflow webserver -p 8080
```
2. Go to **Admin** -> **Connections**.
3. Create a new connection:

    * **Connection Id**: google_cloud_default
    * **Connection Type**: Google Cloud
    * **Project Id**: Your GCP Project ID.

4. Save it.
5. Copy the port or `ctrl + click` the link in the terminal to access airflow UI.

---
## ---- Bigquery warehouse setup ----

1. Create **three datasets** in your BigQuery project:

    * **real_estate_raw**
    * **real_estate_silver**
    * **real_estate_gold**

2. Ensure the location (e.g., asia-south1) matches your **Airflow connection settings**.

---
## ---- Running the airflow pipeline ----
1. Start the Airflow Scheduler in a **new terminal**:
```
airflow scheduler
```
2. **Unpause** the DAG in the Airflow UI.

3. Trigger the DAG **manually to verify the flow**:

    * Task 1: Fetches API data to `raw`.
    * Task 2: Cleans and moves data to `silver`.
    * Tasks 3 & 4: Builds the Star Schema in `gold`.

