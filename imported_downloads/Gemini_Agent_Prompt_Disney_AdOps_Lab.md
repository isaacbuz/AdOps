# Disney Ad Ops Automation Lab — Full Execution Prompt

## ROLE & CONTEXT

You are acting as a senior full-stack engineer building a weekend demo project for Isaac Buziba, a new Software Engineer contractor at Disney Streaming's Growth Marketing team. Isaac starts Monday and needs hands-on experience with the platforms he'll use daily: **Airtable, Databricks (PySpark/Delta Lake), Python API integrations, and webhook-based alerting.**

The project mirrors Disney's real ad ops automation stack:
- **BOAT (Base of Ad Tags)** = Airtable-based ticket/workflow management
- **EVE** = Python-based auto-trafficking engine that pushes campaigns to CM360/DV360/other DSPs
- **Databricks** = Campaign analytics, delivery monitoring, alerting pipelines
- **Mission Control** = Disney's proprietary ad server (we simulate this)

## OBJECTIVE

Execute the following 5-phase project end-to-end, producing all code files, Databricks notebooks, Airtable configuration instructions, and integration scripts. Every deliverable must be production-quality, well-documented, and runnable.

---

## PHASE 0: PROJECT SETUP & DATA GENERATION

### 0.1 Create Project Structure

```
disney-adops-lab/
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── data_generator.py
│   ├── orchestrator.py
│   ├── airtable/
│   │   ├── __init__.py
│   │   └── client.py
│   ├── trafficking/
│   │   ├── __init__.py
│   │   ├── engine.py
│   │   └── qa_engine.py
│   └── alerting/
│       ├── __init__.py
│       └── pipeline.py
├── notebooks/
│   ├── 01_data_ingestion.py
│   └── 02_campaign_health.py
├── data/           # Generated CSVs go here
└── tests/
    ├── test_trafficking.py
    └── test_qa.py
```

Create all files with proper `__init__.py` files, a `.gitignore` (include .env, __pycache__, .venv, data/*.csv), and a `requirements.txt` with: pyairtable, requests, pandas, python-dotenv, databricks-sdk, pytest, black, ruff.

### 0.2 Data Generator

Create `src/data_generator.py` that generates 11 CSV files using the following real Disney reference data. This is critical — every field, value, and mapping comes from Disney's actual BOAT (Base of Ad Tags) system documentation.

#### Reference Data to Embed in the Generator

**Markets & Regions (from BOAT "Markets & Regions" table — 30 markets):**

```python
MARKETS = [
    {"code": "US (ENG)", "geo": "US", "country": "United States", "lang": "ENG", "cluster": "US", "region": "North America"},
    {"code": "US (ES)", "geo": "US", "country": "United States", "lang": "ES", "cluster": "US", "region": "North America"},
    {"code": "CA (ENG)", "geo": "CA", "country": "Canada", "lang": "ENG", "cluster": "CA", "region": "North America"},
    {"code": "CA (FR)", "geo": "CA", "country": "Canada", "lang": "FR", "cluster": "CA", "region": "North America"},
    {"code": "GB (ENG-GB)", "geo": "GB", "country": "United Kingdom", "lang": "ENG-GB", "cluster": "UKI", "region": "EMEA"},
    {"code": "IE (ENG-GB)", "geo": "IE", "country": "Ireland", "lang": "ENG-GB", "cluster": "UKI", "region": "EMEA"},
    {"code": "DE (DE)", "geo": "DE", "country": "Germany", "lang": "DE", "cluster": "GSA", "region": "EMEA"},
    {"code": "AT (AT)", "geo": "AT", "country": "Austria", "lang": "AT", "cluster": "GSA", "region": "EMEA"},
    {"code": "CH (DE)", "geo": "CH", "country": "Switzerland", "lang": "DE", "cluster": "GSA", "region": "EMEA"},
    {"code": "FR (FR)", "geo": "FR", "country": "France", "lang": "FR", "cluster": "France", "region": "EMEA"},
    {"code": "ES (ES)", "geo": "ES", "country": "Spain", "lang": "ES", "cluster": "Iberia", "region": "EMEA"},
    {"code": "PT (PT)", "geo": "PT", "country": "Portugal", "lang": "PT", "cluster": "Iberia", "region": "EMEA"},
    {"code": "IT (IT)", "geo": "IT", "country": "Italy", "lang": "IT", "cluster": "Italy", "region": "EMEA"},
    {"code": "NL (DUT)", "geo": "NL", "country": "Netherlands", "lang": "DUT", "cluster": "BeNeLux", "region": "EMEA"},
    {"code": "BE (FR)", "geo": "BE", "country": "Belgium", "lang": "FR", "cluster": "BeNeLux", "region": "EMEA"},
    {"code": "SE (SV)", "geo": "SE", "country": "Sweden", "lang": "SV", "cluster": "Nordics", "region": "EMEA"},
    {"code": "NO (NB)", "geo": "NO", "country": "Norway", "lang": "NB", "cluster": "Nordics", "region": "EMEA"},
    {"code": "DK (DA)", "geo": "DK", "country": "Denmark", "lang": "DA", "cluster": "Nordics", "region": "EMEA"},
    {"code": "AU (ENG)", "geo": "AU", "country": "Australia", "lang": "ENG", "cluster": "AU", "region": "APAC"},
    {"code": "NZ (ENG)", "geo": "NZ", "country": "New Zealand", "lang": "ENG", "cluster": "NZ", "region": "APAC"},
    {"code": "SG (ENG)", "geo": "SG", "country": "Singapore", "lang": "ENG", "cluster": "SG", "region": "APAC"},
    {"code": "JP (JA)", "geo": "JP", "country": "Japan", "lang": "JA", "cluster": "JP", "region": "APAC"},
    {"code": "KR (KO)", "geo": "KR", "country": "Korea", "lang": "KO", "cluster": "KR", "region": "APAC"},
    {"code": "TW (ZH)", "geo": "TW", "country": "Taiwan", "lang": "ZH", "cluster": "TW", "region": "APAC"},
    {"code": "HK (ZH)", "geo": "HK", "country": "Hong Kong", "lang": "ZH", "cluster": "HK", "region": "APAC"},
    {"code": "MX (ES)", "geo": "MX", "country": "Mexico", "lang": "ES", "cluster": "MX", "region": "LATAM"},
    {"code": "BR (PT)", "geo": "BR", "country": "Brazil", "lang": "PT", "cluster": "BR", "region": "LATAM"},
    {"code": "AR (ES)", "geo": "AR", "country": "Argentina", "lang": "ES", "cluster": "AR", "region": "LATAM"},
    {"code": "CL (ES)", "geo": "CL", "country": "Chile", "lang": "ES", "cluster": "ClPeCo", "region": "LATAM"},
    {"code": "CO (ES)", "geo": "CO", "country": "Colombia", "lang": "ES", "cluster": "ClPeCo", "region": "LATAM"},
]
```

**Brand Mapping (from BOAT VLOOKUP Tables — maps Airtable values to Central Grid codes):**

```python
BRANDS = [
    {"airtable_value": "Disney+ Standalone", "central_grid": "PLUS", "product": "Disney+", "code": "PLUS"},
    {"airtable_value": "Bundle", "central_grid": "DBUN", "product": "Bundle", "code": "DBUN"},
    {"airtable_value": "Disney", "central_grid": "DIS", "product": "Disney+", "code": "DIS"},
    {"airtable_value": "Marvel", "central_grid": "MAR", "product": "Disney+", "code": "MAR"},
    {"airtable_value": "Star Wars", "central_grid": "SW", "product": "Disney+", "code": "SW"},
    {"airtable_value": "Pixar", "central_grid": "PIX", "product": "Disney+", "code": "PIX"},
    {"airtable_value": "National Geographic", "central_grid": "NG", "product": "Disney+", "code": "NG"},
    {"airtable_value": "Star", "central_grid": "STAR", "product": "Star", "code": "STAR"},
    {"airtable_value": "STAR+", "central_grid": "STAR+", "product": "Star+", "code": "STAR+"},
    {"airtable_value": "COMBO+", "central_grid": "COMBO+", "product": "Combo+", "code": "COMBO+"},
]
```

**Channel Mapping (from BOAT VLOOKUP Tables):**

```python
CHANNELS = [
    {"airtable_value": "Display Static", "central_grid": "ProgDisplay", "platform_tax": "CM + DV360"},
    {"airtable_value": "Display Video", "central_grid": "ProgVideo", "platform_tax": "CM + DV360"},
    {"airtable_value": "Display Native", "central_grid": "ProgNative", "platform_tax": "CM + DV360"},
    {"airtable_value": "Display Animated", "central_grid": "ProgDisplay", "platform_tax": "CM + DV360"},
    {"airtable_value": "Social Static", "central_grid": "Social", "platform_tax": "Meta/TikTok/Snap"},
    {"airtable_value": "Social Video", "central_grid": "Social", "platform_tax": "Meta/TikTok/Snap"},
    {"airtable_value": "Search", "central_grid": "Search", "platform_tax": "Google Ads"},
    {"airtable_value": "CTV", "central_grid": "ProgCTV", "platform_tax": "CM + DV360"},
    {"airtable_value": "Audio", "central_grid": "ProgAudio", "platform_tax": "CM + DV360/Spotify"},
    {"airtable_value": "YouTube", "central_grid": "YouTube", "platform_tax": "CM + DV360"},
]
```

**User Roles (from BOAT User Roles Glossary):**

```python
USERS = [
    {"name": "Isaac Buziba", "email": "isaac.buziba@disney.com", "role": "Engineer", "team": "Growth Marketing"},
    {"name": "Craig Shank", "email": "craig.shank@disney.com", "role": "Engineer", "team": "Growth Marketing"},
    {"name": "Carlton Clemens", "email": "carlton.clemens@disney.com", "role": "Project Manager", "team": "Ad Ops PMO"},
    {"name": "Maurice Dib", "email": "maurice.dib@disney.com", "role": "Trafficker", "team": "Ad Ops"},
    {"name": "Chris Cha", "email": "chris.cha@disney.com", "role": "Trafficker", "team": "Ad Ops"},
    {"name": "Evan Weinstein", "email": "evan.weinstein@disney.com", "role": "Trafficker", "team": "Ad Ops"},
    {"name": "Kim Tran", "email": "kim.tran@disney.com", "role": "Trafficker", "team": "Ad Ops"},
    {"name": "Laila Jaffry", "email": "laila.jaffry@disney.com", "role": "Trafficker", "team": "Ad Ops"},
    {"name": "Amanda Zafonte", "email": "amanda.zafonte@disney.com", "role": "Trafficker", "team": "Ad Ops"},
    {"name": "Michael Burgner", "email": "michael.burgner@disney.com", "role": "Trafficker", "team": "Ad Ops"},
    {"name": "Ken Lin", "email": "ken.lin@disney.com", "role": "Trafficker", "team": "Ad Ops"},
    {"name": "Elizabeth Mak", "email": "elizabeth.mak@disney.com", "role": "Trafficker", "team": "Ad Ops"},
    {"name": "Cynthia Sanchez", "email": "cynthia.sanchez@disney.com", "role": "Project Manager", "team": "Ad Ops PMO"},
]
```

**Ticket Types with Role Routing (from BOAT User Roles Glossary):**

```python
TICKET_TYPES = [
    {"type": "New Campaign", "routed_to": "Trafficker", "sla_hours": 24, "eve_eligible": True},
    {"type": "New Placements", "routed_to": "Trafficker", "sla_hours": 24, "eve_eligible": True},
    {"type": "Creative Rotation", "routed_to": "Trafficker", "sla_hours": 8, "eve_eligible": True},
    {"type": "Retrafficking", "routed_to": "Trafficker", "sla_hours": 24, "eve_eligible": True},
    {"type": "URL Change", "routed_to": "Trafficker", "sla_hours": 8, "eve_eligible": False},
    {"type": "Placement Name Change", "routed_to": "Trafficker", "sla_hours": 8, "eve_eligible": False},
    {"type": "Discrepancy Investigation", "routed_to": "Trafficker", "sla_hours": 48, "eve_eligible": False},
    {"type": "Site Tagging", "routed_to": "Trafficker", "sla_hours": 24, "eve_eligible": False},
    {"type": "Kochava", "routed_to": "Trafficker", "sla_hours": 24, "eve_eligible": False},
    {"type": "Automation Bug Request", "routed_to": "Engineer", "sla_hours": 48, "eve_eligible": False},
    {"type": "Automation Feature Request", "routed_to": "Engineer", "sla_hours": 72, "eve_eligible": False},
    {"type": "Product Bug Fix", "routed_to": "Engineer", "sla_hours": 24, "eve_eligible": False},
    {"type": "New Entity Launch QA", "routed_to": "Trafficker", "sla_hours": 48, "eve_eligible": False},
    {"type": "Conversion Tagging QA", "routed_to": "Trafficker", "sla_hours": 24, "eve_eligible": False},
    {"type": "MLP QA", "routed_to": "Trafficker", "sla_hours": 24, "eve_eligible": False},
    {"type": "Login Request", "routed_to": "Project Manager", "sla_hours": 72, "eve_eligible": False},
    {"type": "CM Site Request", "routed_to": "Project Manager", "sla_hours": 72, "eve_eligible": False},
    {"type": "Training/Onboarding", "routed_to": "Trafficker", "sla_hours": 72, "eve_eligible": False},
    {"type": "Prisma Mapping Request", "routed_to": "Trafficker", "sla_hours": 48, "eve_eligible": False},
    {"type": "2ND GEAR New Campaign", "routed_to": "Trafficker", "sla_hours": 24, "eve_eligible": True},
    {"type": "2ND GEAR New Placements", "routed_to": "Trafficker", "sla_hours": 24, "eve_eligible": True},
    {"type": "2ND GEAR Creative Rotation", "routed_to": "Trafficker", "sla_hours": 8, "eve_eligible": True},
    {"type": "Other Request", "routed_to": "Trafficker", "sla_hours": 48, "eve_eligible": False},
]
```

**Audience Targeting (from real Placement Analysis — top segments by frequency):**

```python
AUDIENCES = [
    {"tactic": "Prospecting", "strategy": "Demo", "detailed": "A18+", "source": "3P-GA"},
    {"tactic": "Prospecting", "strategy": "Demo", "detailed": "A35-54", "source": "3P-GA"},
    {"tactic": "Prospecting", "strategy": "Demo", "detailed": "A18-34", "source": "3P-GA"},
    {"tactic": "Prospecting", "strategy": "Demo", "detailed": "A25-34", "source": "3P-GA"},
    {"tactic": "Prospecting", "strategy": "Demo", "detailed": "A18-44", "source": "3P-GA"},
    {"tactic": "Prospecting", "strategy": "Behavior", "detailed": "Action & Adventure Movie Fans", "source": "3P-Oath"},
    {"tactic": "Prospecting", "strategy": "Behavior", "detailed": "Entertainment Affinity", "source": "3P-GA"},
    {"tactic": "Prospecting", "strategy": "Behavior", "detailed": "Sci-Fi Fans", "source": "3P-GA"},
    {"tactic": "Prospecting", "strategy": "Behavior", "detailed": "Parents", "source": "3P-GA"},
    {"tactic": "Prospecting", "strategy": "Behavior", "detailed": "Gamers Affinity", "source": "3P-GA"},
    {"tactic": "Prospecting", "strategy": "Contextual", "detailed": "NA", "source": "3P-Oath"},
    {"tactic": "Retargeting", "strategy": "Behavior", "detailed": "MLP All", "source": "1P"},
    {"tactic": "Retargeting", "strategy": "Behavior", "detailed": "Churners", "source": "1P"},
    {"tactic": "Retargeting", "strategy": "Behavior", "detailed": "MLP Welcome", "source": "1P"},
    {"tactic": "Retargeting", "strategy": "Behavior", "detailed": "MLP Email", "source": "1P"},
]
```

**Content Titles:**

```python
TITLES = [
    "Loki Season 3", "Moana Live Action", "Andor Season 2", "The Mandalorian S4",
    "Inside Out 3", "Thunderbolts", "Daredevil Born Again", "Skeleton Crew S2",
    "Ironheart", "Agatha All Along S2", "Percy Jackson S2", "Avatar Fire & Ash",
    "Alien Earth", "The Bear S4", "Only Murders S5", "Bluey Movie",
    "Monsters at Work S3", "Zootopia 2", "Incredibles 3", "Tron Ares",
]
```

**Campaign Objectives:** `["Acq", "Ret", "Win-back", "Upsell", "Brand", "Engagement"]`

**Platforms:** `["CM360", "DV360", "Amazon DSP", "Yahoo DSP", "Meta", "TikTok", "Snapchat", "Spotify"]`

**Data Hierarchy:** Title → Campaign → Market → Channel → Asset → Platform. Campaign names follow the taxonomy pattern: `{BRAND_CODE}_{TITLE}_{OBJECTIVE}_{GEO}_{CHANNEL_MAPPED}`

**QA Check Types:**
```python
QA_CHECKS = [
    ("Spec Compliance", "Creative matches size/duration/format spec"),
    ("Tracking", "All tags fire correctly on click and view events"),
    ("Targeting", "Geo/demo/device/content targeting verified"),
    ("Landing Page", "Click-through URL resolves and matches IO"),
    ("Frequency Cap", "Frequency cap set per flight requirements"),
    ("Content Exclusions", "Rating exclusions and genre blocks applied"),
    ("Taxonomy Validation", "Placement name follows taxonomy convention"),
    ("Floodlight Tags", "Conversion tags configured and firing"),
]
```

**Ticket Stages:** `["New", "In Review", "Trafficking", "QA", "Ready to Launch", "Live", "Completed", "Blocked"]`

#### Generator Requirements

The generator must produce these 11 CSV files:

| File | Records | Content |
|------|---------|---------|
| 01_titles.csv | 20 | Title records with brand assignments |
| 02_campaigns.csv | 60 | Campaigns using taxonomy naming, linked to titles, with market/channel/audience data |
| 03_delivery.csv | ~500+ | Daily delivery metrics per active/completed campaign (include 5% zero-delivery events and occasional VAST errors) |
| 04_tickets.csv | 120 | BOAT-style tickets with real ticket types, correct role routing, urgency-based SLA overrides |
| 05_qa_checks.csv | ~300+ | QA checks for tickets in QA/Ready to Launch/Live/Completed stages |
| 06_brand_mapping.csv | 10 | VLOOKUP brand reference |
| 07_channel_mapping.csv | 10 | VLOOKUP channel reference |
| 08_markets.csv | 30 | Full markets & regions reference |
| 09_users.csv | 13 | User roles glossary |
| 10_ticket_types.csv | 23 | Ticket type definitions with routing and EVE eligibility |
| 11_audiences.csv | 15 | Audience targeting glossary |

Role routing logic: Engineer tickets go to Isaac Buziba or Craig Shank. PM tickets go to Carlton Clemens or Cynthia Sanchez. Trafficker tickets go to the other 9 traffickers or "Unassigned". SLA overrides: Critical urgency caps at 4 hours, High at 8 hours, regardless of ticket type default.

Run the generator and save all CSVs to `data/`.

---

## PHASE 1: DATABRICKS DATA FOUNDATION (Notebook 01)

Create `notebooks/01_data_ingestion.py` as a Databricks notebook (use `# COMMAND ----------` separators and `# MAGIC %md` for markdown cells).

### Requirements:
1. Upload instructions for getting CSVs into DBFS
2. Define explicit schemas for type safety (use StructType)
3. Read all CSVs and write as Delta tables to `adops_lab` database
4. Run `SHOW TABLES` to verify
5. Exploratory analysis cells:
   - Campaign distribution by type and platform (with budget aggregation)
   - Zero delivery detection (impressions == 0) joined to campaigns for context
   - VAST error report (campaigns with vast_errors > 0, aggregated)
   - SLA analysis: compute days_to_due, flag overdue tickets
   - Ticket distribution by request_type and routed_to_role
   - EVE-eligible ticket breakdown

Each analysis cell should use `display()` for interactive table output.

---

## PHASE 2: DATABRICKS ANALYTICS (Notebook 02)

Create `notebooks/02_campaign_health.py` — the campaign health monitor that replaces Disney's email-based reports.

### Sections to Build:

**1. Zero Delivery Alert** (replaces daily Zero Delivery email)
- Filter delivery where impressions == 0 for the most recent date
- Join to campaigns for campaign_name, advertiser, platform, budget
- Order by budget descending (highest-budget zero-delivery is most urgent)

**2. VAST Error Report** (replaces recurring VAST Error email)
- Aggregate vast_errors by campaign
- Compute error_rate = vast_errors / (impressions + vast_errors) * 100
- Rank by total errors descending

**3. Campaign Pacing Analysis** (replaces Global Pacing doc)
- Compute delivered impressions vs. impressions_goal
- Compute expected pacing % based on days elapsed vs. total flight days
- Classify: Under-pacing (<80% of expected), Over-pacing (>120%), On Track
- Include campaign_name, brand, platform, budget, pacing_pct, expected_pacing_pct, pacing_status

**4. Ticket SLA Metrics** (replaces AOS Newsletter metrics)
- Ticket counts by urgency and stage
- SLA breach rate by assignee (overdue open tickets / total open tickets)
- EVE-eligible tickets by stage (shows automation pipeline health)
- Request type distribution (shows workload patterns)

---

## PHASE 3: PYTHON AUTOMATION ENGINE

### 3.1 Airtable API Client (`src/airtable/client.py`)

Wrapper using `pyairtable` that provides:
- `get_pending_tickets()` — Stage = "Trafficking" and Assignee not empty
- `get_tickets_needing_qa()` — Stage = "QA"
- `get_eve_eligible_tickets()` — EVE Eligible = TRUE and Stage = "Trafficking"
- `update_ticket_stage(record_id, new_stage, notes="")` — move ticket forward
- `create_qa_check(ticket_id, check_name, result, details)` — write QA result
- `get_breached_tickets()` — SLA Status = "Breached" and Stage not Completed
- `get_campaign(campaign_id)` — lookup campaign details
- `get_unassigned_tickets()` — Assignee is empty
- `assign_ticket(record_id, assignee_name)` — set assignee

Load credentials from .env (AIRTABLE_PAT, AIRTABLE_BASE_ID).

### 3.2 Trafficking Engine (`src/trafficking/engine.py`)

**EVE-style auto-trafficking engine.** Maps to EVE versions:
- V1: CM360 × DV360
- V2: CM360 × Yahoo
- V2.1: CM360 × YouTube
- V2.2: ProgAudio, ProgCTV, ProgNative
- V3: CM360 × Amazon (in development)

Requirements:
- `TraffickingPayload` dataclass with: campaign_id, platform, action, payload dict, status, response, created_at
- `TraffickingEngine` class with `process_ticket(ticket_fields, campaign_fields)` method
- Route to correct handler based on request_type + platform
- Handlers for: New Campaign Setup (multi-step: CM360 campaign shell → CM360 placement → DSP-specific insertion order), Creative Rotation (rotational placement swap), Budget Change, New Line Item, Targeting Update, Tag Implementation (Floodlight setup)
- `get_eve_version(platform, channel_mapped)` — determines which EVE version handles the ticket
- `build_placement_taxonomy(ticket, campaign)` — constructs Disney placement name using pipe-delimited taxonomy: `campaign_name|targeting_geo|language|brand_code|title|objective|channel_mapped`
- Each handler generates simulated API payloads (we don't call real APIs)

### 3.3 QA Engine (`src/trafficking/qa_engine.py`)

Automated QA that runs these checks on trafficking payloads:
- **Spec Compliance** — verify creative dimensions exist
- **Tracking** — verify Floodlight tag exists when placements are created
- **Targeting** — verify geo targeting is specified
- **Frequency Cap** — verify frequency cap exists (required for BES Sponsorships)
- **Content Exclusions** — flag BES/Sponsorship tickets for S&P review
- **Landing Page** — verify HTTPS URLs
- **Taxonomy Validation** — verify placement name follows pipe-delimited convention
- **Floodlight Tags** — verify conversion tracking is configured

Each check returns `{"check": str, "result": "Pass"|"Fail"|"Needs Review", "details": str}`.

`run_all_checks(payloads, campaign)` returns a list of all check results.

### 3.4 Alerting Pipeline (`src/alerting/pipeline.py`)

Webhook-based alerting with methods for:
- `send_zero_delivery_alert(campaigns)` — formatted Slack blocks with campaign details
- `send_sla_breach_alert(tickets)` — ticket IDs, urgency, assignee, due date
- `send_pacing_alert(underpacing, overpacing)` — weekly pacing summary
- `send_qa_failure_alert(ticket, failures)` — immediate alert when QA blocks a launch

Support both Slack and Teams webhooks (read from .env). If neither configured, print formatted alerts to console as fallback.

### 3.5 Orchestrator (`src/orchestrator.py`)

End-to-end pipeline:
1. Get all tickets in "Trafficking" stage
2. For each ticket: get linked campaign, generate trafficking payloads via engine, run QA checks
3. Write QA results back to Airtable
4. If all QA passes → move ticket to "Ready to Launch"
5. If QA fails → hold ticket in "QA", add notes explaining failures
6. Run health check: find SLA-breached tickets, send alerts
7. Print summary of all actions taken

Include a `demo()` method that runs the full sequence with timing and formatted output.

---

## PHASE 4: AIRTABLE CONFIGURATION GUIDE

Since you can't directly configure Airtable via API setup (the base structure must be done in the UI), produce a detailed **Airtable Setup Script** as a markdown file (`AIRTABLE_SETUP.md`) with exact instructions for:

### Tables to Create (9 total)

Import CSVs in this order:
1. **Markets** ← 08_markets.csv
2. **Brand Mapping** ← 06_brand_mapping.csv
3. **Channel Mapping** ← 07_channel_mapping.csv
4. **Users** ← 09_users.csv
5. **Ticket Types** ← 10_ticket_types.csv
6. **Audiences** ← 11_audiences.csv
7. **Titles** ← 01_titles.csv
8. **Campaigns** ← 02_campaigns.csv
9. **Tickets** ← 04_tickets.csv
10. **QA Checks** ← 05_qa_checks.csv

### Field Type Conversions (per table)

For each table, list every field that needs to be converted from "Single line text" to its correct type: Single Select (with color assignments), Date, Currency, Number, Checkbox, Link to another record, Lookup, Formula.

### Linked Records to Create

- Titles.Brand → Link to Brand Mapping
- Campaigns.Title → Link to Titles
- Campaigns.Targeting Geo → Link to Markets
- Campaigns.Channel → Link to Channel Mapping
- Tickets.Campaign → Link to Campaigns
- QA Checks.Ticket → Link to Tickets

### Formulas

```
SLA Status (Tickets):
IF(IS_AFTER(NOW(), DATEADD({Created Date}, {SLA Hours}, "hours")),
  "Breached",
  IF(IS_AFTER(NOW(), DATEADD({Created Date}, {SLA Hours} * 0.75, "hours")),
    "At Risk",
    "On Track"
  )
)

Needs QA? (Tickets):
IF(
  AND(
    OR({Stage} = "Ready to Launch", {Stage} = "QA"),
    OR(
      FIND("Fail", ARRAYJOIN({QA Results})) > 0,
      FIND("Needs Review", ARRAYJOIN({QA Results})) > 0
    )
  ),
  "BLOCKED - QA Issues",
  ""
)
```

### Views to Create (12 views)

| View | Table | Type | Filter | Group/Sort |
|------|-------|------|--------|------------|
| My Queue | Tickets | Grid | Assignee = Isaac, Stage ≠ Completed | Sort: Due Date asc |
| Kanban Board | Tickets | Kanban | None | Group: Stage |
| SLA Dashboard | Tickets | Grid | SLA Status = Breached OR At Risk | Group: Urgency |
| EVE Ready | Tickets | Grid | EVE Eligible = TRUE, Stage = Trafficking | Sort: Created Date asc |
| Engineer Backlog | Tickets | Grid | Routed To Role = Engineer, Stage ≠ Completed | Sort: Urgency, Due Date |
| By Region | Tickets | Grid | None | Group: Region (lookup) |
| By Platform | Tickets | Grid | None | Group: Platform |
| QA Gate | Tickets | Grid | Stage = QA OR Needs QA? not empty | Sort: Urgency |
| Campaign Calendar | Campaigns | Calendar | None | Date: Start Date |
| Release Slate | Titles | Calendar | None | Date: Release Date |
| Global Campaigns | Campaigns | Grid | None | Group: Region, then Channel |
| Brand Performance | Campaigns | Grid | None | Group: Brand Code |

### Automations to Build (4)

1. **SLA Breach Alert**: Trigger = record matches SLA Status = "Breached". Action = send email with Ticket ID, Title, Urgency, Assignee, Due Date.
2. **QA Gate**: Trigger = Stage changes to "Ready to Launch". Condition = Needs QA? is not empty. Actions = revert Stage to "QA", send email to assignee.
3. **Ticket Auto-Routing**: Trigger = record created in Tickets. Condition = Assignee is empty. Action = set Routed To Role based on Request Type, assign round-robin to team members with that role.
4. **EVE Eligibility Flag**: Trigger = record created or Request Type changed. Condition = Request Type in EVE-eligible list. Action = set EVE Eligible checkbox to TRUE.

### Color Coding Rules

Apply row coloring in Ticket views:
- SLA Status = Breached → Red row
- SLA Status = At Risk → Orange row
- Stage = Blocked → Gray row
- EVE Eligible + Stage = Trafficking → Purple row
- Assignee is empty → Yellow row

Request Type single select colors:
- Campaign Setup types (New Campaign, New Placements, 2ND GEAR variants) → Blue
- Creative Work (Creative Rotation, Retrafficking) → Teal
- Investigations (Discrepancy, QA types) → Orange
- Engineering (Automation Bug/Feature, Product Bug) → Purple
- Admin/PM (Login, CM Site, Training) → Gray
- Tracking (Site Tagging, Kochava, URL/Name Change) → Green

---

## PHASE 5: TESTS

Create `tests/test_trafficking.py` and `tests/test_qa.py`:

**test_trafficking.py:**
- Test that TraffickingEngine.process_ticket returns payloads for each request type
- Test that EVE version routing returns correct version for each platform/channel combo
- Test that placement taxonomy builder produces pipe-delimited strings
- Test that a New Campaign ticket generates at least 2 payloads (CM360 shell + DSP order)

**test_qa.py:**
- Test that all QA checks return valid result values (Pass/Fail/Needs Review)
- Test that missing geo targeting triggers a Fail
- Test that BES/Sponsorship tickets get flagged for Content Exclusions review
- Test that non-HTTPS landing pages trigger a Fail
- Test that run_all_checks returns one result per check type

---

## EXECUTION INSTRUCTIONS

1. Create every file listed above with complete, runnable code
2. Run the data generator and verify output counts
3. Run tests and ensure all pass
4. Produce a README.md with: project overview, setup instructions, phase-by-phase execution guide, and architecture diagram (ASCII)
5. Produce the AIRTABLE_SETUP.md with exact UI configuration steps

When complete, list all files created and their line counts.

---

## QUALITY REQUIREMENTS

- All Python code must pass `black` formatting and `ruff` linting
- All functions must have docstrings
- All modules must have module-level docstrings explaining their Disney system equivalent
- Use dataclasses where appropriate
- Use type hints throughout
- Use `python-dotenv` for all configuration
- Handle missing env vars gracefully (print warnings, use console fallback for alerting)
- All file I/O should use pathlib
- Random seeds for reproducibility (seed=42)
