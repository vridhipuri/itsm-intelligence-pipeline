# import the libraries we need
import requests
import json
import os
import boto3
from datetime import datetime
from dotenv import load_dotenv

# load credentials from our .env file
load_dotenv()

SNOW_INSTANCE = os.getenv("SNOW_INSTANCE")
SNOW_USERNAME = os.getenv("SNOW_USERNAME")
SNOW_PASSWORD = os.getenv("SNOW_PASSWORD")

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME       = os.getenv("AWS_BUCKET_NAME")
AWS_REGION            = os.getenv("AWS_REGION")

# build the ServiceNow API base URL
BASE_URL = f"https://{SNOW_INSTANCE}.service-now.com/api/now/table"


def extract_incidents():
    # the fields we want to pull from each incident
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
        "sys_updated_on"
    ]

    # build the request parameters
    params = {
        "sysparm_fields": ",".join(fields),
        "sysparm_limit": 1000,
        "sysparm_display_value": "true"
    }

    # call the ServiceNow API
    print("Calling ServiceNow API...")
    response = requests.get(
        f"{BASE_URL}/incident",
        auth=(SNOW_USERNAME, SNOW_PASSWORD),
        params=params
    )

    # check the response was successful
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        return

    # extract the list of incidents from the response
    incidents = response.json()["result"]
    print(f"Pulled {len(incidents)} incidents from ServiceNow")

    # convert to JSON string — this is what we will upload to S3
    today = datetime.now().strftime("%Y-%m-%d")
    json_data = json.dumps(incidents, indent=2)

    # also save locally as a backup
    os.makedirs("data", exist_ok=True)
    local_file = f"data/incidents_{today}.json"
    with open(local_file, "w") as f:
        f.write(json_data)
    print(f"Saved locally to {local_file}")

    # upload to S3
    # the S3 key is the path inside your bucket — like a folder structure
    # raw/incidents/2026-03-22/incidents_2026-03-22.json
    s3_key = f"raw/incidents/{today}/incidents_{today}.json"

    print(f"Uploading to S3: s3://{AWS_BUCKET_NAME}/{s3_key}")

    s3_client = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    s3_client.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key=s3_key,
        Body=json_data,
        ContentType="application/json"
    )

    print(f"Successfully uploaded to S3!")
    print(f"Location: s3://{AWS_BUCKET_NAME}/{s3_key}")


# run the function when this file is executed
if __name__ == "__main__":
    extract_incidents()