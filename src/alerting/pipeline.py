"""
Alerting Pipeline Module for Disney Ad Ops Lab.
Handles automated webhook alerts (Slack/Teams).
"""
import os
import requests
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

class AlertPipeline:
    def __init__(self):
        self.slack_url = os.getenv("SLACK_WEBHOOK_URL")
        self.teams_url = os.getenv("TEAMS_WEBHOOK_URL")

    def _send(self, payload: Dict[str, Any], service: str = "slack"):
        """Internal helper to dispatch webhook or fallback to console."""
        url = self.slack_url if service == "slack" else self.teams_url
        if not url:
            print(f"[{service.upper()} FALLBACK] {payload.get('text', payload)}")
            return
        
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code not in (200, 201, 204):
                print(f"Failed to send {service} alert: {resp.status_code} - {resp.text}")
        except Exception as e:
            print(f"Error sending {service} alert: {e}")

    def send_zero_delivery_alert(self, campaigns: List[Dict[str, Any]]):
        if not campaigns: return
        text = "🚨 *Zero Delivery Alert*\nThe following campaigns had 0 impressions yesterday:\n"
        for c in campaigns:
            text += f"- {c.get('campaign_name')} | Platform: {c.get('platform', 'N/A')}\n"
        self._send({"text": text}, "slack")

    def send_sla_breach_alert(self, tickets: List[Dict[str, Any]]):
        if not tickets: return
        text = "⚠️ *SLA Breach Alert*\nThe following tickets missed their SLA deadline:\n"
        for t in tickets:
            fields = t.get("fields", {})
            t_id = fields.get("Ticket ID", t.get("id"))
            urgency = fields.get("Urgency", "N/A")
            assignee = fields.get("Assignee", "Unassigned")
            text += f"- {t_id} | Priority: {urgency} | Assignee: {assignee}\n"
        self._send({"text": text}, "slack")

    def send_pacing_alert(self, underpacing: List[Dict[str, Any]], overpacing: List[Dict[str, Any]]):
        text = "📊 *Weekly Pacing Summary*\n"
        text += f"Under-pacing Campaigns: {len(underpacing)}\n"
        text += f"Over-pacing Campaigns: {len(overpacing)}\n"
        self._send({"text": text}, "slack")

    def send_qa_failure_alert(self, ticket: Dict[str, Any], failures: List[Dict[str, str]]):
        fields = ticket.get("fields", {})
        t_id = fields.get("Ticket ID", ticket.get("id"))
        assignee = fields.get("Assignee", "Unassigned")
        text = f"🛑 *QA Failed for Ticket {t_id}* (Assignee: {assignee})\nFailures:\n"
        for f in failures:
            text += f"- {f['check']}: {f['details']}\n"
        self._send({"text": text}, "teams")  # example: route QA failures to teams
