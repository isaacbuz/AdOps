from pyairtable import Api
import os
import requests
from dotenv import load_dotenv

load_dotenv()
pat = os.getenv("AIRTABLE_PAT")
base_id = "appoBgMjnQjy6M8Jo"

import json
url = f"https://api.airtable.com/v0/{base_id}/qa_checks"
headers = {"Authorization": f"Bearer {pat}", "Content-Type": "application/json"}
body = {
  "records": [
    {
      "fields": {
        "ticket_id": ["recByk2HA9hr4usHd"],
        "check_name": "Global",
        "result": "Fail",
        "check_details": "No payloads to QA."
      }
    }
  ]
}

r = requests.post(url, headers=headers, json=body)
print(r.status_code)
print(r.text)

