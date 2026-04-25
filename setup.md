## A guide to run this project on your machine 🚀

---
## ---- 1st. Install python environment  ----

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

Run this blocks of code one by one -

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

* Initialization

```
    gcloud init
```

> Log in with your gmail so that every project can be opened under that mail and used for future project building. 

1. Run this code to complete the gcloud login
```
    gcloud auth login
```

---

## ---- 2nd : AIRFLOW home & user setup ----

1. Setup the `HOME` directory for `airflow` files
2. Initiate the airflow database

> run this block of code :

```
    export AIRFLOW_HOME=$(pwd)/dags
    airflow migrate db
```

#### OR

1. setup the airflow_home address in `.bashrc` file.
```
    nano ~/.bashrc

    export AIRFLOW_HOME=$(pwd)/dags #(You can also write the full path of the dag folder)
```

2. then save the file and write -

```
    source ~/.bashrc
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

> This issue generally occurs because of airflow versions above 3.0. 

1. Run the following code in your terminal to fix the user creation hassle for the project.

```
    pip install apache-airflow-providers-fab
```

2. Then set the following context in the config file of airflow such as `airflow.cfg`

```
    auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
```

**Now you can create the user as it was written in the  `3rd` pointer of ```second task``` section**

---

## ---- 3rd : AIRFLOW UI connecion and project path setup ----

### Configure GCP Connection
1. Start the Airflow Scheduler in a **new terminal**:
```
    airflow scheduler
```

2. Launch the Airflow UI in an another **terminal**: 
```
    airflow webserver -p 8080
```
> Open the airflow UI by the given port from the terminal.

3. Go to **Admin** -> **Connections**

4. Create a new connection:

    * **Connection Id**: google_cloud_default
    * **Connection Type**: Google Cloud
    * **Project Id**: Your GCP Project ID.

5. Save it.

---
## ---- 4th. Bigquery dataset setup ----

1. Create **three datasets** in your BigQuery project:

    * **real_estate_raw**
    * **real_estate_silver**
    * **real_estate_gold**

2. Ensure the location (e.g., asia-south1) matches your **Airflow connection settings**.

---