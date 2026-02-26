"""
Trafficking Engine Module for Disney Ad Ops Lab.
Equivalent to the EVE auto-trafficking engine versions V1-V3.
Handles constructing simulated payloads to DSPs via API.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

@dataclass
class TraffickingPayload:
    """Represents an API payload sent to a platform like CM360 or DSP."""
    campaign_id: str
    platform: str
    action: str
    payload: Dict[str, Any]
    status: str = "Pending"
    response: Optional[Dict[str, Any]] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())

class TraffickingEngine:
    """Simulates the EVE trafficking engine that pushes configurations to platforms."""
    
    def get_eve_version(self, platform: str, channel_mapped: str) -> str:
        """Determines which EVE version handles the ticket based on platform and channel."""
        if platform == "CM360" and channel_mapped in ["ProgAudio", "ProgCTV", "ProgNative"]:
            return "V2.2"
        elif platform == "CM360" and channel_mapped == "YouTube":
            return "V2.1"
        elif platform == "Yahoo DSP":
            return "V2"
        elif platform == "Amazon DSP":
            return "V3"
        elif platform == "DV360":
            return "V1"
        # Fallback or base version for standard DSPs:
        return "V1"

    def build_placement_taxonomy(self, ticket: Dict[str, Any], campaign: Dict[str, Any]) -> str:
        """
        Constructs Disney's pipe-delimited placement name:
        campaign_name|targeting_geo|language|brand_code|title|objective|channel_mapped
        """
        # Safely extract fields with fallbacks if not present
        camp_name = campaign.get("campaign_name", "UNKNOWN_CAMP")
        geo = campaign.get("targeting_geo", ticket.get("targeting_geo", "UNKNOWN_GEO"))
        lang = campaign.get("language", "ENG")
        brand = campaign.get("brand_code", ticket.get("brand", "UNKNOWN_BRAND"))
        title = campaign.get("title_name", "UNKNOWN_TITLE")
        objective = campaign.get("campaign_objective", "Acq")
        channel = campaign.get("channel_mapped", "ProgDisplay")
        
        return f"{camp_name}|{geo}|{lang}|{brand}|{title}|{objective}|{channel}"

    def process_ticket(self, ticket_fields: Dict[str, Any], campaign_fields: Dict[str, Any]) -> List[TraffickingPayload]:
        """Routes a ticket to the appropriate handler and returns the DSP payloads."""
        req_type = ticket_fields.get("request_type", "")
        platform = ticket_fields.get("platform", "DV360")
        
        # Route depending on request type
        if "New Campaign" in req_type or "New Placements" in req_type:
            return self._handle_new_campaign_setup(ticket_fields, campaign_fields, platform)
        elif "Creative Rotation" in req_type or "Retrafficking" in req_type:
            return self._handle_creative_rotation(ticket_fields, campaign_fields, platform)
        elif req_type == "Budget Change":
            return self._handle_budget_change(ticket_fields, campaign_fields, platform)
        elif req_type == "New Line Item":
            return self._handle_new_line_item(ticket_fields, campaign_fields, platform)
        elif req_type == "Targeting Update":
            return self._handle_targeting_update(ticket_fields, campaign_fields, platform)
        elif req_type == "Site Tagging" or req_type == "Kochava":
            return self._handle_tag_implementation(ticket_fields, campaign_fields, platform)
        else:
            # Not an automated trafficking type
            return []

    def _handle_new_campaign_setup(self, ticket: Dict, campaign: Dict, platform: str) -> List[TraffickingPayload]:
        """Creates basic multi-step setup: CM360 Shell -> Placement -> DSP Order"""
        payloads = []
        camp_id = ticket.get("campaign_id", "CMP-UNKNOWN")
        taxonomy = self.build_placement_taxonomy(ticket, campaign)
        
        # 1. Create CM360 Campaign Shell
        payloads.append(TraffickingPayload(
            campaign_id=camp_id,
            platform="CM360",
            action="CREATE_CAMPAIGN_SHELL",
            payload={"name": campaign.get("campaign_name", "New Campaign"), "start_date": campaign.get("start_date")}
        ))
        
        # 2. Create CM360 Placement using standard taxonomy
        payloads.append(TraffickingPayload(
            campaign_id=camp_id,
            platform="CM360",
            action="CREATE_PLACEMENT",
            payload={"placement_name": taxonomy, "site": platform}
        ))
        
        # 3. Create IO correctly in DSP
        payloads.append(TraffickingPayload(
            campaign_id=camp_id,
            platform=platform,
            action="CREATE_INSERTION_ORDER",
            payload={"budget": campaign.get("budget_usd", 0), "targeting": campaign.get("audience_detailed")}
        ))
        return payloads

    def _handle_creative_rotation(self, ticket: Dict, campaign: Dict, platform: str) -> List[TraffickingPayload]:
        """Creates a creative rotation payload."""
        camp_id = ticket.get("campaign_id", "CMP-UNKNOWN")
        return [TraffickingPayload(
            campaign_id=camp_id,
            platform="CM360",
            action="ROTATE_CREATIVES",
            payload={"placements": [self.build_placement_taxonomy(ticket, campaign)], "new_assets": ["asset_1.mp4", "asset_2.jpg"]}
        )]

    def _handle_budget_change(self, ticket: Dict, campaign: Dict, platform: str) -> List[TraffickingPayload]:
        camp_id = ticket.get("campaign_id", "CMP-UNKNOWN")
        return [TraffickingPayload(
            campaign_id=camp_id,
            platform=platform,
            action="UPDATE_BUDGET",
            payload={"new_budget": campaign.get("budget_usd", 0)}
        )]

    def _handle_new_line_item(self, ticket: Dict, campaign: Dict, platform: str) -> List[TraffickingPayload]:
        camp_id = ticket.get("campaign_id", "CMP-UNKNOWN")
        return [TraffickingPayload(
            campaign_id=camp_id,
            platform=platform,
            action="CREATE_LINE_ITEM",
            payload={"targeting": campaign.get("audience_detailed")}
        )]

    def _handle_targeting_update(self, ticket: Dict, campaign: Dict, platform: str) -> List[TraffickingPayload]:
        camp_id = ticket.get("campaign_id", "CMP-UNKNOWN")
        return [TraffickingPayload(
            campaign_id=camp_id,
            platform=platform,
            action="UPDATE_TARGETING",
            payload={"new_targeting": campaign.get("audience_detailed"), "geo": campaign.get("targeting_geo")}
        )]

    def _handle_tag_implementation(self, ticket: Dict, campaign: Dict, platform: str) -> List[TraffickingPayload]:
        camp_id = ticket.get("campaign_id", "CMP-UNKNOWN")
        return [TraffickingPayload(
            campaign_id=camp_id,
            platform="CM360",
            action="CREATE_FLOODLIGHT_TAG",
            payload={"conversion_type": "Subscription", "counting_method": "STANDARD"}
        )]
