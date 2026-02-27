import os
import requests
import hashlib
import time
from dotenv import load_dotenv

load_dotenv()

class MetaAPIClient:
    def __init__(self):
        self.access_token = os.getenv("META_ACCESS_TOKEN")
        self.ad_account_id = os.getenv("META_AD_ACCOUNT_ID")
        self.pixel_id = os.getenv("META_PIXEL_ID")
        self.api_version = "v19.0"
        self.base_url = f"https://graph.facebook.com/{self.api_version}"

    def build_campaign(self, campaign_name, budget_usd):
        """Creates a campaign in the Meta Sandbox/Live account."""
        if not self.access_token or not self.ad_account_id:
            print("⚠️ Skipping Meta Campaign Creation: Credentials missing in .env")
            return None

        url = f"{self.base_url}/act_{self.ad_account_id}/campaigns"
        
        payload = {
            "name": campaign_name,
            "objective": "OUTCOME_TRAFFIC",
            "status": "PAUSED",
            "special_ad_categories": [],
            "daily_budget": int(budget_usd * 100), # Meta expects cents
            "access_token": self.access_token
        }

        try:
            r = requests.post(url, data=payload)
            r.raise_for_status()
            res = r.json()
            print(f"✅ Successfully created Meta Campaign ID: {res.get('id')} for '{campaign_name}'")
            return res.get("id")
        except Exception as e:
            print(f"❌ Meta API Error creating campaign: {e}")
            if hasattr(e, 'response') and e.response:
                print(e.response.text)
            return None

    def send_capi_event(self, event_name, event_id, user_data, custom_data=None):
        """
        Sends a server-side conversion event to Meta CAPI.
        Event ID must match the front-end pixel event ID for deduplication.
        """
        if not self.access_token or not self.pixel_id:
            print("⚠️ Skipping Meta CAPI: Credentials missing in .env")
            return None

        url = f"{self.base_url}/{self.pixel_id}/events"

        # Ensure user data is hashed using SHA-256 for privacy compliance
        hashed_user_data = {}
        if "email" in user_data:
            em = user_data["email"].strip().lower()
            hashed_user_data["em"] = [hashlib.sha256(em.encode('utf-8')).hexdigest()]
        if "client_ip_address" in user_data:
            hashed_user_data["client_ip_address"] = user_data["client_ip_address"]
        if "client_user_agent" in user_data:
            hashed_user_data["client_user_agent"] = user_data["client_user_agent"]

        data_payload = {
            "event_name": event_name,
            "event_time": int(time.time()),
            "action_source": "website",
            "event_id": event_id,
            "user_data": hashed_user_data
        }
        
        if custom_data:
            data_payload["custom_data"] = custom_data

        payload = {
            "data": [data_payload],
            "access_token": self.access_token
        }

        try:
            r = requests.post(url, json=payload)
            r.raise_for_status()
            res = r.json()
            print(f"🔥 Successfully fired server-side CAPI event '{event_name}' (Events Received: {res.get('events_received')})")
            return res
        except Exception as e:
            print(f"❌ Meta CAPI Error: {e}")
            if hasattr(e, 'response') and e.response:
                print(e.response.text)
            return None

# For quick local testing
if __name__ == "__main__":
    client = MetaAPIClient()
    # client.build_campaign("DIS_Moana_US_Meta_2026", 50000)
    # client.send_capi_event("Subscribe", "evt_123456", {"email": "test@disney.com", "client_ip_address": "127.0.0.1", "client_user_agent": "Mozilla/5.0"})
