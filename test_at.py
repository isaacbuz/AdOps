import os
import requests
from dotenv import load_dotenv

load_dotenv()
pat = os.getenv("AIRTABLE_PAT")
base_id = os.getenv("AIRTABLE_BASE_ID")

headers = {
    "Authorization": f"Bearer {pat}"
}

# Try getting records from 'tickets' without any formula
url = f"https://api.airtable.com/v0/{base_id}/tickets?maxRecords=1"
print(f"Requesting: {url}")
r = requests.get(url, headers=headers)
print(r.status_code)
print(r.text)
