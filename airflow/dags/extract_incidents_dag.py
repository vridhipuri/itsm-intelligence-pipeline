from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import os
import boto3
from dotenv import load_dotenv

# load credentials from our .env file
load_dotenv()

# ── default settings applied to every task in this DAG ──────────────────────
# retries=1 means if a task fails, Airflow tries once more before marking it failed
# retry_delay=5 minutes means it waits 5 minutes before retrying
default_args = {
    "owner": "itsm-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── define the DAG ───────────────────────────────────────────────────────────
# dag_id        : unique name — shows up in the Airflow UI
# schedule      : "0 6 * * *" means run every day at 6am (cron syntax)
# start_date    : Airflow won't schedule runs before this date
# catchup=False : don't run all the missed days since start_date
with DAG(
    dag_id="extract_servicenow_incidents",
    default_args=default_args,
    description="Extract incidents from ServiceNow and upload to S3",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["servicenow", "extract", "s3"],
) as dag:

    # ── task 1: extract incidents from ServiceNow ────────────────────────────
    def extract_incidents():
        """
        Calls the ServiceNow REST API, pulls all incidents,
        saves locally and uploads raw JSON to S3.
        """
        # read credentials from environment variables
        # Airflow passes these in from the .env file we created
        snow_instance = os.getenv("SNOW_INSTANCE")
        snow_username = os.getenv("SNOW_USERNAME")
        snow_password = os.getenv("SNOW_PASSWORD")
        bucket_name   = os.getenv("AWS_BUCKET_NAME")
        aws_region    = os.getenv("AWS_REGION")

        # the fields we want from each incident
        fields = [
            "number",
            "short_description",
            "priority",
            "state",
            "category",
            "assignment_group",
            "assigned_to",
            "opened_at",
            "resolved_at",
            "closed_at",
            "sys_updated_on",
        ]

        params = {
            "sysparm_fields": ",".join(fields),
            "sysparm_limit": 1000,
            "sysparm_display_value": "true",
        }

        print(f"Connecting to ServiceNow instance: {snow_instance}")
        response = requests.get(
            f"https://{snow_instance}.service-now.com/api/now/table/incident",
            auth=(snow_username, snow_password),
            params=params,
        )

        if response.status_code != 200:
            # raising an exception tells Airflow the task failed
            raise Exception(f"ServiceNow API error: {response.status_code} - {response.text}")

        incidents = response.json()["result"]
        print(f"Pulled {len(incidents)} incidents from ServiceNow")

        # convert to JSON string for uploading
        today = datetime.now().strftime("%Y-%m-%d")
        json_data = json.dumps(incidents, indent=2)

        # upload to S3 under raw/incidents/YYYY-MM-DD/
        s3_key = f"raw/incidents/{today}/incidents_{today}.json"

        s3_client = boto3.client(
            "s3",
            region_name=aws_region,
        )

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json",
        )

        print(f"Uploaded to s3://{bucket_name}/{s3_key}")
        # returning a value lets the next task read it if needed
        return s3_key

    # ── task 2: verify the upload succeeded ─────────────────────────────────
    def verify_upload(**context):
        """
        Checks that the file we just uploaded actually exists in S3.
        This is a simple data quality check — did the upload work?
        context is passed automatically by Airflow and lets us read
        the return value of the previous task.
        """
        bucket_name = os.getenv("AWS_BUCKET_NAME")
        aws_region  = os.getenv("AWS_REGION")

        # xcom_pull reads the return value from extract_incidents task
        s3_key = context["ti"].xcom_pull(task_ids="extract_incidents")

        s3_client = boto3.client("s3", region_name=aws_region)

        response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        file_size = response["ContentLength"]

        print(f"Verified: s3://{bucket_name}/{s3_key}")
        print(f"File size: {file_size} bytes")

        if file_size == 0:
            raise Exception("Upload verification failed — file is empty!")

        print("Verification passed!")

    # ── wire the tasks together ──────────────────────────────────────────────
    # PythonOperator runs a Python function as an Airflow task
    task_extract = PythonOperator(
        task_id="extract_incidents",
        python_callable=extract_incidents,
    )

    task_verify = PythonOperator(
        task_id="verify_upload",
        python_callable=verify_upload,
        provide_context=True,
    )

    # >> means "task_extract must finish before task_verify starts"
    # this is the core of DAG design — defining task order
    task_extract >> task_verify