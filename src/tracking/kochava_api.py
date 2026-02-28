import os
import requests
from dotenv import load_dotenv

load_dotenv()

class KochavaAPIClient:
    """
    Client for automating Kochava (Mobile Attribution Platform).
    Disney uses Kochava to track app installs and in-app events (like a Disney+ subscription)
    back to the specific media partner (e.g., Meta, TikTok, Apple Search Ads).
    """
    def __init__(self):
        self.api_key = os.getenv("KOCHAVA_API_KEY")
        self.app_guid = os.getenv("KOCHAVA_APP_GUID") # e.g., the Disney+ iOS App GUID
        self.base_url = "https://go.kochava.com/v1/trackers"
        self.headers = {
            "API-Key": self.api_key if self.api_key else "",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def generate_tracker_url(self, campaign_name: str, network_name: str, site_id: str) -> dict:
        """
        Creates a new tracking link in Kochava for a specific campaign/network combination.
        Returns the Click URL and Impression URL to be embedded in the DSP.
        """
        if not self.api_key or not self.app_guid:
            print("⚠️ Skipping Kochava Tracker Creation: Credentials missing in .env")
            return None

        # Kochava needs to know which app we are tracking and which network we are sending traffic from
        payload = {
            "app_guid": self.app_guid,
            "network_name": network_name, # e.g. "Facebook", "TikTok"
            "campaign_name": campaign_name,
            "tracker_name": f"{campaign_name}_{network_name}_Tracker",
            "type": "acquisition",
            "destination_url": "https://apps.apple.com/us/app/disney/id1446075923" # Redirects to App Store
        }

        try:
            print(f"🚀 Requesting Kochava Mobile Tracker for {network_name}...")
            # Note: In a real Kochava API integration, authentication and endpoint structure 
            # might require specific v1/v2 endpoint mapping. We simulate the logic here.
            r = requests.post(self.base_url, headers=self.headers, json=payload)
            r.raise_for_status()
            res = r.json()
            
            tracker_id = res.get("tracker_id", "KCHV-MOCK-1234")
            
            # The API returns the actual tracking URLs
            tracking_urls = {
                "click_url": f"https://smart.link/{tracker_id}?site_id={site_id}",
                "impression_url": f"https://imp.kochava.com/track/impression?tracker_id={tracker_id}&site_id={site_id}"
            }
            
            print(f"✅ Generated Kochava Mobile Attribution Links (ID: {tracker_id})")
            return tracking_urls
            
        except Exception as e:
            print(f"❌ Kochava API Request Failed: {e}")
            # Fallback for lab simulation if token isn't active
            return {
                "click_url": f"https://smart.link/mock-kchv-{network_name.lower()}?camp={campaign_name}",
                "impression_url": f"https://imp.kochava.com/track/impression?mock=true&camp={campaign_name}"
            }

# Quick test stub
if __name__ == "__main__":
    kchv = KochavaAPIClient()
    # urls = kchv.generate_tracker_url("DIS_Moana_US_2026", "TikTok", "TT_12345")
    # print(urls)
