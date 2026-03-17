# HEARTBEAT.md — AdOps-Repo Scheduled Routines

## Periodic Tasks
This repository orchestrates several automated processes:

1. **BOAT Orchestrator**: The primary continuous execution loop managing Airtable processing, Databricks validation, Alerting, and Analytics operations.
2. **QA Check Suite**: The 8-check suite runs as part of the orchestration cycle.
3. **Trafficking Engine Run**: Pushes updates to external platforms (DV360, Yahoo, CM360, Amazon).

*If an Agent is tasked with scheduled modifications here, treat the Orchestrator pipeline as the primary heartbeat.*
