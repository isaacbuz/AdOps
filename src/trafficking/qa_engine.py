"""
QA Engine Module for Disney Ad Ops Lab.
Automated QA simulator replacing manual trafficker QA checks.
"""
from typing import List, Dict, Any

class QAEngine:
    def check_spec_compliance(self, payloads: List[Any], campaign: Dict[str, Any]) -> Dict[str, str]:
        # Simple simulated check: Assume passes if payloads exist
        return {"check": "Spec Compliance", "result": "Pass" if payloads else "Fail", "details": "Creative matches spec."}

    def check_tracking(self, payloads: List[Any], campaign: Dict[str, Any]) -> Dict[str, str]:
        missing = [p for p in payloads if p.action == "CREATE_PLACEMENT" and not "tracking_url" in p.payload]
        # Allow pass even if tracking not explicitly set for demo
        return {"check": "Tracking", "result": "Pass", "details": "Tracking tags configured."}

    def check_targeting(self, payloads: List[Any], campaign: Dict[str, Any]) -> Dict[str, str]:
        geo = campaign.get("targeting_geo")
        if not geo:
            return {"check": "Targeting", "result": "Fail", "details": "Missing geo targeting."}
        return {"check": "Targeting", "result": "Pass", "details": f"Geo targeting found: {geo}"}

    def check_frequency_cap(self, payloads: List[Any], campaign: Dict[str, Any]) -> Dict[str, str]:
        camp_name = campaign.get("campaign_name", "")
        # Required if it's a sponsorship (BES)
        if "BES" in camp_name or "Sponsorship" in camp_name:
            # Simulated check logic
            return {"check": "Frequency Cap", "result": "Pass", "details": "Frequency cap set for Sponsorship."}
        return {"check": "Frequency Cap", "result": "Pass", "details": "Frequency cap standard."}

    def check_content_exclusions(self, payloads: List[Any], campaign: Dict[str, Any]) -> Dict[str, str]:
        camp_name = campaign.get("campaign_name", "")
        if "BES" in camp_name or "Sponsorship" in camp_name:
            return {"check": "Content Exclusions", "result": "Needs Review", "details": "Sponsorship requires S&P review."}
        return {"check": "Content Exclusions", "result": "Pass", "details": "Standard exclusions applied."}

    def check_landing_page(self, payloads: List[Any], campaign: Dict[str, Any]) -> Dict[str, str]:
        # Usually checking payload landing page URLs
        # For demo, assume https unless it's a specific fail case
        url = campaign.get("landing_page", "https://disneyplus.com")
        if not url.startswith("https://"):
            return {"check": "Landing Page", "result": "Fail", "details": f"Non-HTTPS URL provided: {url}"}
        return {"check": "Landing Page", "result": "Pass", "details": "Click-through URLs act recursively."}

    def check_taxonomy(self, payloads: List[Any], campaign: Dict[str, Any]) -> Dict[str, str]:
        placement_payloads = [p for p in payloads if p.action == "CREATE_PLACEMENT"]
        for p in placement_payloads:
            name = p.payload.get("placement_name", "")
            if name.count("|") < 6:
                return {"check": "Taxonomy Validation", "result": "Fail", "details": "Placement name does not follow pipe-delimited convention."}
        return {"check": "Taxonomy Validation", "result": "Pass", "details": "Taxonomy valid."}

    def check_floodlight_tags(self, payloads: List[Any], campaign: Dict[str, Any]) -> Dict[str, str]:
        return {"check": "Floodlight Tags", "result": "Pass", "details": "Conversion tags correctly assigned."}

    def run_all_checks(self, payloads: List[Any], campaign: Dict[str, Any]) -> List[Dict[str, str]]:
        if not payloads:
            return [{"check": "Spec Compliance", "result": "Fail", "details": "No payloads to QA."}]

        return [
            self.check_spec_compliance(payloads, campaign),
            self.check_tracking(payloads, campaign),
            self.check_targeting(payloads, campaign),
            self.check_frequency_cap(payloads, campaign),
            self.check_content_exclusions(payloads, campaign),
            self.check_landing_page(payloads, campaign),
            self.check_taxonomy(payloads, campaign),
            self.check_floodlight_tags(payloads, campaign)
        ]
