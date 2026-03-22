# import the libraries we need
import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# load credentials from our .env file
load_dotenv()

SNOW_INSTANCE = os.getenv("SNOW_INSTANCE")
SNOW_USERNAME = os.getenv("SNOW_USERNAME")
SNOW_PASSWORD = os.getenv("SNOW_PASSWORD")

# build the API URL — same one you tested in the browser
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

    # call the API — same as typing the URL in your browser
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

    # save to a local JSON file with today's date in the name
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"data/incidents_{today}.json"
    os.makedirs("data", exist_ok=True)

    with open(filename, "w") as f:
        json.dump(incidents, f, indent=2)

    print(f"Saved to {filename}")

# run the function when this file is executed
if __name__ == "__main__":
    extract_incidents()