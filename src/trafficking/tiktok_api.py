import os
import requests
from dotenv import load_dotenv

load_dotenv()

class TikTokAPIClient:
    """
    Client for automating campaign creation on the TikTok Marketing API (v1.3).
    Equivalent to the MetaAPIClient for cross-publisher orchestration.
    """
    def __init__(self):
        self.access_token = os.getenv("TIKTOK_ACCESS_TOKEN")
        self.advertiser_id = os.getenv("TIKTOK_ADVERTISER_ID")
        self.base_url = "https://business-api.tiktok.com/open_api/v1.3"
        self.headers = {
            "Access-Token": self.access_token if self.access_token else "",
            "Content-Type": "application/json"
        }

    def build_campaign(self, campaign_name: str, budget_usd: float) -> str:
        """
        Creates a Campaign in TikTok Ads Manager.
        """
        if not self.access_token or not self.advertiser_id:
            print("⚠️ Skipping TikTok Campaign Creation: Credentials missing in .env")
            return None

        url = f"{self.base_url}/campaign/create/"
        
        # TikTok expects minimum daily budget of $50 (but requires passing standard integer logic, sometimes 50.00 depending on locale, we use 50)
        safe_budget = max(int(budget_usd), 50)

        payload = {
            "advertiser_id": self.advertiser_id,
            "campaign_name": campaign_name,
            "objective_type": "TRAFFIC", # Standard for our mock proxy-landing-pages tests
            "budget_mode": "BUDGET_MODE_DAY",
            "budget": safe_budget
        }

        try:
            r = requests.post(url, headers=self.headers, json=payload)
            r.raise_for_status()
            res = r.json()
            
            # TikTok API wraps responses in a 'data' object
            if res.get("code") == 0:
                campaign_id = res["data"]["campaign_id"]
                print(f"✅ Successfully created TikTok Campaign ID: {campaign_id} for '{campaign_name}'")
                return campaign_id
            else:
                print(f"❌ TikTok API Error: {res.get('message')}")
                return None
                
        except Exception as e:
            print(f"❌ TikTok API Request Failed: {e}")
            if hasattr(e, 'response') and e.response:
                print(e.response.text)
            return None

    def build_adgroup(self, campaign_id: str, adgroup_name: str, pixel_code: str, budget: float, geo: str = "US"):
        """
        Creates an AdGroup falling under the previously created campaign.
        This represents the 'Targeting' phase of trafficking.
        """
        if not self.access_token:
            return None
            
        url = f"{self.base_url}/adgroup/create/"
        
        payload = {
            "advertiser_id": self.advertiser_id,
            "campaign_id": campaign_id,
            "adgroup_name": adgroup_name,
            "placement_type": "PLACEMENT_TYPE_NORMAL",
            "placement": ["PLACEMENT_TIKTOK"],
            "location": [geo], 
            "budget_mode": "BUDGET_MODE_DAY",
            "budget": max(int(budget), 20), # Minimum AdGroup budget
            "schedule_type": "SCHEDULE_START_END",
            "optimize_goal": "CLICK",
            "pixel_id": pixel_code # Links back to our tracking pixels from Phase 7!
        }

        try:
            r = requests.post(url, headers=self.headers, json=payload)
            r.raise_for_status()
            res = r.json()
            if res.get("code") == 0:
                adgroup_id = res["data"]["adgroup_id"]
                print(f"✅ Successfully created TikTok AdGroup ID: {adgroup_id}")
                return adgroup_id
            else:
                print(f"❌ TikTok API Error: {res.get('message')}")
                return None
        except Exception as e:
            print(f"❌ TikTok API Request Failed: {e}")
            return None

# Quick test stub
if __name__ == "__main__":
    client = TikTokAPIClient()
    # mock_camp_id = client.build_campaign("DIS_Moana_US_TikTok_2026", 500)
    # if mock_camp_id:
    #     client.build_adgroup(mock_camp_id, "Targeting_A18-34", "mock_pixel_123", 100)
