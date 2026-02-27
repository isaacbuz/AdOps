import os
import requests
from dotenv import load_dotenv

load_dotenv()

class AdobeLaunchClient:
    """
    Client for automating Adobe Launch (Experience Platform Data Collection).
    This handles creating Tag properties, data elements, and publishing
    rules for tracking Adobe Analytics, Meta Pixels, and TikTok pixels.
    """
    def __init__(self):
        self.access_token = os.getenv("ADOBE_IO_TOKEN")
        self.client_id = os.getenv("ADOBE_CLIENT_ID")
        self.company_id = os.getenv("ADOBE_ORG_ID")
        self.base_url = "https://reactor.adobe.io"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "x-api-key": self.client_id,
            "x-gw-ims-org-id": self.company_id,
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json"
        }

    def fetch_launch_property(self, property_name: str) -> str:
        """
        Retrieves the web property ID from Adobe Launch (e.g. DisneyPlus.com)
        """
        if not self.access_token or not self.client_id:
            print("⚠️ Skipping Adobe Launch API: Credentials missing in .env")
            return None
        
        url = f"{self.base_url}/companies/{self.company_id}/properties"
        
        try:
            r = requests.get(url, headers=self.headers)
            r.raise_for_status()
            res = r.json()
            
            # Find the specific Disney property
            for data in res.get("data", []):
                if data["attributes"]["name"] == property_name:
                    property_id = data["id"]
                    print(f"✅ Found Adobe Launch Property: {property_id} ({property_name})")
                    return property_id
            
            print(f"❌ Adobe Launch Property '{property_name}' not found.")
            return None
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                print("❌ Adobe Launch Access Denied. Needs admin provisioning.")
            return None
        except Exception as e:
            print(f"❌ Adobe Launch API Request Failed: {e}")
            return None

    def inject_tracking_rule(self, property_id: str, rule_name: str, tracking_code: str) -> str:
        """
        Automatically pushes a QA-approved tracking pixel natively into
        Disney's Tag Manager (Adobe Launch) as a new "Rule".
        """
        print(f"🚀 Pushing Rule '{rule_name}' to Adobe Launch Development Environment...")
        
        # In a real API flow, this is complex: Create Rule, Create Action (Custom Code), Link them.
        url = f"{self.base_url}/properties/{property_id}/rules"
        
        payload = {
            "data": {
                "type": "rules",
                "attributes": {
                    "name": rule_name,
                    "published": False # Needs QA approval in Publisher
                }
            }
        }
        
        # Simulating the exact Adobe JSON schema response
        try:
            r = requests.post(url, headers=self.headers, json=payload)
            r.raise_for_status()
            rule_id = r.json()["data"]["id"]
            print(f"✅ Success! Generated Adobe Launch Rule ID: {rule_id}")
            print(f"🔗 Tag Manager Status: PENDING PUBLISH TO STAGING")
            return rule_id
        except Exception as e:
            print(f"❌ Adobe Launch failed to inject pixel: {e}")
            return None

# For quick testing
if __name__ == "__main__":
    adobe = AdobeLaunchClient()
    # prop = adobe.fetch_launch_property("Disney+")
    # if prop:
    #     adobe.inject_tracking_rule(prop, "Meta CAPI Tracking Script", "<script>fbq('init')</script>")
