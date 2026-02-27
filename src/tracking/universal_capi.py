import os
import requests
import hashlib
import time
from dotenv import load_dotenv

load_dotenv()

class UniversalCAPIEngine:
    """
    Universal Conversions API (CAPI) Engine.
    Routes secure, server-side conversion events to multiple advertising publishers.
    """
    def __init__(self):
        # Meta (Facebook) Credentials
        self.meta_token = os.getenv("META_ACCESS_TOKEN")
        self.meta_pixel_id = os.getenv("META_PIXEL_ID")
        
        # TikTok Credentials
        self.tiktok_token = os.getenv("TIKTOK_ACCESS_TOKEN")
        self.tiktok_pixel_code = os.getenv("TIKTOK_PIXEL_CODE")
        
        # Snapchat Credentials
        self.snap_token = os.getenv("SNAP_ACCESS_TOKEN")
        self.snap_pixel_id = os.getenv("SNAP_PIXEL_ID")

    def _hash_data(self, data: str) -> str:
        """All CAPI platforms require PII to be SHA-256 hashed."""
        if not data:
            return ""
        return hashlib.sha256(data.strip().lower().encode('utf-8')).hexdigest()

    def process_conversion(self, publisher: str, event_name: str, event_id: str, user_data: dict, custom_data: dict = None):
        """
        Master router for sending server-side events to any supported publisher.
        
        :param publisher: str (e.g. 'meta', 'tiktok', 'snapchat')
        :param event_name: str (e.g. 'Purchase', 'CompleteRegistration')
        :param event_id: str (Mandatory for deduplication with browser pixel)
        :param user_data: dict (email, phone, ip_address, user_agent)
        :param custom_data: dict (value, currency, content_name)
        """
        pub = publisher.lower()
        if pub == "meta":
            return self._send_meta_capi(event_name, event_id, user_data, custom_data)
        elif pub == "tiktok":
            return self._send_tiktok_capi(event_name, event_id, user_data, custom_data)
        elif pub == "snapchat":
            return self._send_snapchat_capi(event_name, event_id, user_data, custom_data)
        else:
            print(f"❌ Unsupported Publisher mapping for CAPI: {publisher}")
            return None

    def _send_meta_capi(self, event_name, event_id, user_data, custom_data):
        if not self.meta_token or not self.meta_pixel_id:
            print("⚠️ Meta CAPI: Credentials missing.")
            return

        url = f"https://graph.facebook.com/v19.0/{self.meta_pixel_id}/events"
        hashed_user = {}
        if "email" in user_data: hashed_user["em"] = [self._hash_data(user_data["email"])]
        if "phone" in user_data: hashed_user["ph"] = [self._hash_data(user_data["phone"])]
        if "ip_address" in user_data: hashed_user["client_ip_address"] = user_data["ip_address"]
        if "user_agent" in user_data: hashed_user["client_user_agent"] = user_data["user_agent"]

        payload = {
            "data": [{
                "event_name": event_name,
                "event_time": int(time.time()),
                "action_source": "website",
                "event_id": event_id,
                "user_data": hashed_user,
                "custom_data": custom_data or {}
            }],
            "access_token": self.meta_token
        }

        try:
            r = requests.post(url, json=payload)
            r.raise_for_status()
            print(f"✅ Meta CAPI Success | Event: {event_name}")
            return r.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Meta CAPI Failed: {e.response.text if hasattr(e, 'response') and e.response else e}")
            return None

    def _send_tiktok_capi(self, event_name, event_id, user_data, custom_data):
        if not self.tiktok_token or not self.tiktok_pixel_code:
            print("⚠️ TikTok CAPI: Credentials missing.")
            return

        url = "https://business-api.tiktok.com/open_api/v1.3/pixel/track/"
        
        # TikTok specific hashing requirements
        hashed_user = {}
        if "email" in user_data: hashed_user["email"] = self._hash_data(user_data["email"])
        if "phone" in user_data: hashed_user["phone_number"] = self._hash_data(user_data["phone"])
        
        context = {}
        if "ip_address" in user_data: context["ip"] = user_data["ip_address"]
        if "user_agent" in user_data: context["user_agent"] = user_data["user_agent"]

        payload = {
            "pixel_code": self.tiktok_pixel_code,
            "event": event_name,
            "event_id": event_id,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "context": context,
            "user": hashed_user,
            "properties": custom_data or {}
        }
        
        headers = {
            "Access-Token": self.tiktok_token,
            "Content-Type": "application/json"
        }

        try:
            r = requests.post(url, headers=headers, json=payload)
            r.raise_for_status()
            print(f"✅ TikTok CAPI Success | Event: {event_name}")
            return r.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ TikTok CAPI Failed: {e.response.text if hasattr(e, 'response') and e.response else e}")
            return None

    def _send_snapchat_capi(self, event_name, event_id, user_data, custom_data):
        # Snapchat CAPI implementation template
        print(f"✅ Snapchat CAPI Mock Success | Event: {event_name}")
        return {"status": "success", "platform": "snapchat"}

# Example Usage
if __name__ == "__main__":
    engine = UniversalCAPIEngine()
    
    # 1. Grab data that was captured securely on the backend (e.g. at user Login)
    user_info = {
        "email": "mickey@disney.com",
        "ip_address": "192.168.1.1",
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    }
    
    # 2. Fire identical deduplicated event ID to multiple publishers
    shared_event_id = f"evt_{int(time.time())}"
    
    # Route to Meta
    engine.process_conversion(
        publisher="meta",
        event_name="Subscribe",
        event_id=shared_event_id,
        user_data=user_info,
        custom_data={"currency": "USD", "value": 7.99}
    )
    
    # Route to TikTok
    engine.process_conversion(
        publisher="tiktok",
        event_name="Subscribe",
        event_id=shared_event_id,
        user_data=user_info,
        custom_data={"currency": "USD", "value": 7.99}
    )
