"""
Orchestrator Module for Disney Ad Ops Lab.
Runs the end-to-end pipeline linking Airtable, EVE, QA, and Alerting.
"""
import time
from src.airtable.client import AirtableClient
from src.trafficking.engine import TraffickingEngine
from src.trafficking.qa_engine import QAEngine
from src.alerting.pipeline import AlertPipeline
from src.trafficking.meta_api import MetaAPIClient
from src.trafficking.tiktok_api import TikTokAPIClient

class Orchestrator:
    def __init__(self):
        self.airtable = AirtableClient()
        self.engine = TraffickingEngine()
        self.qa = QAEngine()
        self.alerter = AlertPipeline()

    def run_pipeline(self):
        print("Starting Ad Ops Automation Pipeline...")
        
        # 1. Get Tickets in Trafficking stage
        tickets = self.airtable.get_pending_tickets()
        print(f"[{len(tickets)}] tickets found in Trafficking stage.")

        for ticket in tickets:
            fields = ticket.get("fields", {})
            req_type = fields.get("request_type", "Unknown")
            campaign_list = fields.get("campaign_id", [])
            campaign_id = campaign_list[0] if campaign_list else None
            
            print(f"\nProcessing Ticket: {fields.get('Ticket ID')} - {req_type}")
            
            # 2. Get Campaign
            campaign = self.airtable.get_campaign(campaign_id) if campaign_id else None
            campaign_fields = campaign.get("fields", {}) if campaign else {}

            # Fallback mock data if API doesn't work locally for demo
            if not campaign_fields:
                campaign_fields = {"campaign_name": "Demo Campaign", "targeting_geo": "US", "budget_usd": 1000}

            # 3. Simulate EVE Trafficking Payload Generation
            # Map Airtable keys manually to simulate what Engine expects:
            tkt_mapped = {
                "request_type": fields.get("request_type"),
                "platform": fields.get("platform", "DV360"),
                "targeting_geo": fields.get("targeting_geo"),
                "brand": fields.get("brand"),
                "campaign_id": fields.get("campaign_id")
            }
            cmp_mapped = {
                "campaign_name": campaign_fields.get("campaign_name", "DEMO_CAMP"),
                "targeting_geo": campaign_fields.get("targeting_geo", "US"),
                "brand_code": campaign_fields.get("brand_code", "DIS"),
                "title_name": campaign_fields.get("title_name", "Loki"),
                "campaign_objective": campaign_fields.get("campaign_objective", "Acq"),
                "channel_mapped": campaign_fields.get("channel_mapped", "ProgDisplay"),
                "start_date": campaign_fields.get("start_date", "2026-03-01"),
                "budget_usd": campaign_fields.get("budget_usd", 10000),
                "audience_detailed": campaign_fields.get("audience_detailed", "A18-34")
            }

            payloads = self.engine.process_ticket(tkt_mapped, cmp_mapped)
            if payloads:
                print(f" -> Generated {len(payloads)} payload(s) for {tkt_mapped['platform']}")
            else:
                print(" -> No automated payload required for this request type.")

            # 4. Run QA
            check_results = self.qa.run_all_checks(payloads, cmp_mapped)
            
            # 5. Write QA results
            failures = []
            for res in check_results:
                self.airtable.create_qa_check(ticket["id"], res["check"], res["result"], res["details"])
                if res["result"] in ["Fail", "Needs Review"]:
                    failures.append(res)
            
            # 6. Set Next Stage
            if failures:
                print(f" -> ❌ QA Failed ({len(failures)} issues). Moving to QA and Alerting.")
                notes = "Automated QA identified issues preventing launch."
                self.airtable.update_ticket_stage(ticket["id"], "QA", notes=notes)
                self.alerter.send_qa_failure_alert(ticket, failures)
            else:
                print(" -> ✅ QA Passed. Moving to Ready to Launch.")
                
                # --- PHASE 2 LIVE API INTEGRATION ---
                # Check if this payload is destined for Meta or TikTok
                is_new_camp = "New Campaign" in req_type
                platform_dest = tkt_mapped.get("platform", "Unknown")
                
                campaign_taxonomy = cmp_mapped.get("campaign_name", "UNKNOWN")
                
                if platform_dest == "Meta" and is_new_camp:
                    print(" -> 🚀 Initiating Live API Deployment to Meta Sandbox...")
                    meta = MetaAPIClient()
                    for pl in payloads:
                        if pl.action == "CREATE_INSERTION_ORDER":
                            budget = pl.payload.get("budget", 1000)
                            meta_id = meta.build_campaign(campaign_taxonomy, budget)
                            if meta_id:
                                notes = f"Deployed dynamically to Meta API - ID: {meta_id}"
                                self.airtable.update_ticket_stage(ticket["id"], "Ready to Launch", notes=notes)
                            break

                elif platform_dest == "TikTok" and is_new_camp:
                    print(" -> 🎵 Initiating Live API Deployment to TikTok Sandbox...")
                    tiktok = TikTokAPIClient()
                    for pl in payloads:
                        if pl.action == "CREATE_INSERTION_ORDER":
                            budget = pl.payload.get("budget", 500)
                            tt_id = tiktok.build_campaign(campaign_taxonomy, budget)
                            if tt_id:
                                notes = f"Deployed dynamically to TikTok API - ID: {tt_id}"
                                self.airtable.update_ticket_stage(ticket["id"], "Ready to Launch", notes=notes)
                            break
                else:
                    self.airtable.update_ticket_stage(ticket["id"], "Ready to Launch")
        
        # 7. Health Check
        print("\nRunning Health Check...")
        breached = self.airtable.get_breached_tickets()
        if breached:
            print(f"Found {len(breached)} breached tickets! Sending Alert.")
            self.alerter.send_sla_breach_alert(breached)

    def demo(self):
        """Simulate a demo run without needing real Airtable connection."""
        print("====== DISNEY AD OPS DEMO START ======")
        print("1. Fetching tickets from Airtable...")
        time.sleep(1)
        # Mock ticket
        ticket_id = "recABC123"
        print(f"-> Found 1 pending ticket: TKT-00001 (New Campaign, CM360)")
        
        print("\n2. Engine constructing payload taxonomy...")
        time.sleep(1)
        print("-> Taxonomy built: PLUS_Loki_Acq_US_ProgDisplay")
        payload = {"site": "DV360", "placement_name": "PLUS_Loki_Acq_US_ProgDisplay"}
        print(f"-> Payload action: CREATE_PLACEMENT")
        
        print("\n3. Running automated QA against created assets...")
        time.sleep(1.5)
        print("-> [Pass] Spec Compliance")
        print("-> [Pass] Taxonomy Validation")
        print("-> [Pass] Geo Target (US)")
        
        print("\n4. Marking ticket as Ready to Launch...")
        print("====== DEMO PIPELINE FINISHED ======")

if __name__ == "__main__":
    orc = Orchestrator()
    orc.run_pipeline()
