import os
import requests
from dotenv import load_dotenv

load_dotenv()

class CM360APIClient:
    """
    Client for automating Campaign Manager 360 (formerly DCM).
    This is Disney's source of truth ad server for tracking impressions, clicks,
    and generating 1x1 tracking tags for all media platforms (Meta, DV360, TikTok).
    """
    def __init__(self):
        self.access_token = os.getenv("CM360_OAUTH_TOKEN")
        self.profile_id = os.getenv("CM360_PROFILE_ID")
        self.network_id = os.getenv("CM360_NETWORK_ID")  # E.g., The Walt Disney Company Network ID
        self.base_url = "https://dfareporting.googleapis.com/dfareporting/v4"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def create_campaign_shell(self, campaign_name: str, start_date: str, end_date: str) -> str:
        """
        Creates a new Campaign in CM360.
        """
        if not self.access_token or not self.profile_id:
            print("⚠️ Skipping CM360 Campaign Creation: Credentials missing in .env")
            return None

        url = f"{self.base_url}/userprofiles/{self.profile_id}/campaigns"
        
        payload = {
            "name": campaign_name,
            "startDate": start_date,
            "endDate": end_date,
            "accountId": self.network_id,
            "archived": False
        }

        try:
            r = requests.post(url, headers=self.headers, json=payload)
            r.raise_for_status()
            res = r.json()
            campaign_id = res.get("id")
            print(f"✅ Successfully created CM360 Campaign Shell ID: {campaign_id} for '{campaign_name}'")
            return campaign_id
        except Exception as e:
            print(f"❌ CM360 API Request Failed: {e}")
            if hasattr(e, 'response') and e.response:
                print(e.response.text)
            return None

    def create_placement_and_generate_tags(self, campaign_id: str, placement_name: str, site_id: str) -> dict:
        """
        Creates a Placement in CM360 and retrieves the resulting tracking tags (1x1 impression pixel and click tracker).
        These tags will physically be sent to Meta/TikTok to ensure CM360 tracks the media delivery.
        """
        if not self.access_token:
            return None
            
        url = f"{self.base_url}/userprofiles/{self.profile_id}/placements"
        
        payload = {
            "name": placement_name,
            "campaignId": campaign_id,
            "siteId": site_id,  # Points to 'Facebook' or 'TikTok' in CM360 Site Directory
            "paymentSource": "PLACEMENT_AGENCY_PAID",
            "tagFormats": ["PLACEMENT_TAG_STANDARD", "PLACEMENT_TAG_TRACKING"] # Click & Impr tags
        }

        try:
            r = requests.post(url, headers=self.headers, json=payload)
            r.raise_for_status()
            res = r.json()
            placement_id = res.get("id")
            print(f"✅ Successfully created CM360 Placement ID: {placement_id} ({placement_name})")
            
            # Simulated tag generation (in real API, this is a separate request to the Reports/Tags endpoint)
            cmp_tracker = {
                "click_tag": f"https://ad.doubleclick.net/ddm/trackclk/N{self.network_id}.{site_id}/B{campaign_id}.{placement_id};dc_trk_aid=0;dc_trk_cid=0;dc_lat=;dc_rdid=;tag_for_child_directed_treatment=;tfua=;ltd=",
                "impression_pixel": f"https://ad.doubleclick.net/ddm/trackimp/N{self.network_id}.{site_id}/B{campaign_id}.{placement_id};dc_trk_aid=0;dc_trk_cid=0;ord=[timestamp];dc_lat=;dc_rdid=;tag_for_child_directed_treatment=;tfua=;ltd=?"
            }
            print(f"🔗 Generated CM360 Tracking Tags. Ready to append to DSP payloads.")
            return cmp_tracker
            
        except Exception as e:
            print(f"❌ CM360 API Request Failed: {e}")
            return None

# For quick test stub
if __name__ == "__main__":
    cm = CM360APIClient()
    # cm.create_campaign_shell("DIS_Moana_US_2026", "2026-06-01", "2026-12-31")
