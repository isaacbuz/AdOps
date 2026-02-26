"""
Airtable Client Module for Disney Ad Ops Lab.
Equivalent to the integration layer connecting BOAT to Python services.
"""
import os
from pyairtable import Api
from dotenv import load_dotenv

load_dotenv()

class AirtableClient:
    def __init__(self):
        pat = os.getenv("AIRTABLE_PAT")
        base_id = os.getenv("AIRTABLE_BASE_ID")
        if not pat or not base_id:
            print("WARNING: AIRTABLE_PAT or AIRTABLE_BASE_ID missing from .env")
            self.api = None
            self.base_id = None
        else:
            self.api = Api(pat)
            self.base_id = base_id
            
    def _get_table(self, table_name):
        return self.api.table(self.base_id, table_name) if self.api else None

    def get_pending_tickets(self):
        """Returns tickets in Trafficking stage with an Assignee."""
        table = self._get_table("tickets")
        if not table: return []
        formula = "AND({stage}='Trafficking', NOT({assignee}=''))"
        return table.all(formula=formula)

    def get_tickets_needing_qa(self):
        """Returns tickets in QA stage."""
        table = self._get_table("tickets")
        if not table: return []
        formula = "{stage}='QA'"
        return table.all(formula=formula)

    def get_eve_eligible_tickets(self):
        """Returns tickets eligible for EVE in Trafficking stage."""
        table = self._get_table("tickets")
        if not table: return []
        formula = "AND({eve_eligible}=1, {stage}='Trafficking')"
        return table.all(formula=formula)

    def update_ticket_stage(self, record_id: str, new_stage: str, notes: str = ""):
        """Updates the stage and optionally notes for a given ticket."""
        table = self._get_table("tickets")
        if not table: return
        fields = {"stage": new_stage}
        if notes:
            fields["notes"] = notes
        table.update(record_id, fields, typecast=True)

    def create_qa_check(self, ticket_record_id: str, check_name: str, result: str, details: str):
        """Creates a QA check log linked to a ticket."""
        table = self._get_table("qa_checks")
        if not table: return
        fields = {
            "ticket_id": [ticket_record_id],
            "check_name": check_name,
            "result": result,
            "check_details": details
        }
        table.create(fields, typecast=True)

    def get_breached_tickets(self):
        """Returns non-completed tickets with Breached SLA Status."""
        table = self._get_table("tickets")
        if not table: return []
        formula = "AND({SLA Status}='Breached', NOT({stage}='Completed'))"
        return table.all(formula=formula)

    def get_campaign(self, campaign_record_id: str):
        """Looks up a campaign by Airtable record ID."""
        table = self._get_table("campaigns")
        if not table: return None
        return table.get(campaign_record_id)

    def get_unassigned_tickets(self):
        """Returns tickets with empty Assignee."""
        table = self._get_table("tickets")
        if not table: return []
        formula = "{assignee}=''"
        return table.all(formula=formula)

    def assign_ticket(self, record_id: str, assignee_name: str):
        """Assigns a ticket to a specific assignee name."""
        table = self._get_table("tickets")
        if not table: return
        table.update(record_id, {"Assignee": assignee_name})
