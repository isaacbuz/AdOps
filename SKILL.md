# SKILL.md — AdOps-Repo Quick Reference

## Key Paths

| Item | Path |
|------|------|
| Source Code | `src/` |
| Airtable Module | `src/airtable/` |
| Trafficking Engine | `src/trafficking/` |
| QA Engine | `src/analytics/` |
| Alerting | `src/alerting/` |
| Orchestrator | `src/orchestrator/` |
| Notebooks | `notebooks/` |
| Tests | `tests/` |

## Quick Commands

```bash
# Setup
pip install -r requirements.txt
cp .env.example .env  # Fill in API keys

# Run BOAT orchestrator
python src/orchestrator/main.py

# Run QA suite
python -m pytest tests/

# Check trafficking
python src/trafficking/trafficker.py --dry-run
```

## Pipeline Flow

```
Airtable BOAT → EVE Python Orchestrator → Databricks Lakehouse
                    │
                    ├── Trafficking Engine (DV360, Yahoo, CM360, Amazon)
                    ├── QA Engine (8-check suite)
                    ├── Alert Pipeline (Slack, Teams)
                    └── Analytics (Gold tables)
```

## Environment Variables

```bash
AIRTABLE_API_TOKEN=        # Airtable PAT
SLACK_WEBHOOK_URL=         # Slack alerts
DATABRICKS_HOST=           # Databricks workspace
DATABRICKS_TOKEN=          # Databricks PAT
```

## Knowledge Base Entity

Entity name: `AdOps-Repo`
