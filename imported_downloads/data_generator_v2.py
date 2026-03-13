"""
Disney Ad Ops Lab — Synthetic Data Generator
=============================================
Generates realistic campaign, delivery, ticket, and QA data
modeled on the actual BOAT (Base of Ad Tags) system.

Reference data sourced from:
- BOAT Backlog (AOBB) — Markets & Regions, VLOOKUP Tables, User Roles
- Performance Marketing Airtable Workflow — data hierarchy
- BOAT x EVE User Guide — ticket types, DSP configurations
- Placement Analysis — audience targeting patterns
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
import os

random.seed(42)
np.random.seed(42)

# ═══════════════════════════════════════════════════════════════
# REFERENCE DATA — Sourced from BOAT Backlog (AOBB)
# ═══════════════════════════════════════════════════════════════

# Markets & Regions table (from BOAT "Markets & Regions" tab)
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

# Brand mapping (from BOAT VLOOKUP Tables)
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

# Channel mapping (from BOAT VLOOKUP Tables)
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

# User roles (from BOAT User Roles Glossary)
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

# Ticket types (from BOAT User Roles Glossary — ticket type → role routing)
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

# Campaign objectives (from BOAT Central Grid)
CAMPAIGN_OBJECTIVES = ["Acq", "Ret", "Win-back", "Upsell", "Brand", "Engagement"]

# EVE versions (from BOAT x EVE User Guide)
EVE_VERSIONS = [
    {"version": "V1", "desc": "CM360 x DV360", "platforms": ["CM360", "DV360"]},
    {"version": "V2", "desc": "CM360 x Yahoo", "platforms": ["CM360", "Yahoo DSP"]},
    {"version": "V2.1", "desc": "CM360 x YouTube", "platforms": ["CM360", "YouTube"]},
    {"version": "V2.2", "desc": "ProgAudio/CTV/Native", "platforms": ["CM360", "DV360"]},
    {"version": "V3", "desc": "CM360 x Amazon (dev)", "platforms": ["CM360", "Amazon DSP"]},
]

# DSP platforms
PLATFORMS = ["CM360", "DV360", "Amazon DSP", "Yahoo DSP", "Meta", "TikTok", "Snapchat", "Spotify"]

# Audience targeting (from Placement Analysis — top audience segments)
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

# Disney+ content titles (realistic examples for campaign names)
TITLES = [
    "Loki Season 3", "Moana Live Action", "Andor Season 2", "The Mandalorian S4",
    "Inside Out 3", "Thunderbolts", "Daredevil Born Again", "Skeleton Crew S2",
    "Ironheart", "Agatha All Along S2", "Percy Jackson S2", "Avatar Fire & Ash",
    "Alien Earth", "The Bear S4", "Only Murders S5", "Bluey Movie",
    "Monsters at Work S3", "Zootopia 2", "Incredibles 3", "Tron Ares",
    "The Fantastic Four", "Captain America Brave New World", "Spider-Man Animated",
    "Star Wars Dawn of the Jedi", "National Geographic Arctic", "ESPN+ UFC 320",
]


# ═══════════════════════════════════════════════════════════════
# DATA HIERARCHY: Title → Campaign → Market → Channel → Asset → Platform
# (from Performance Marketing Airtable Workflow)
# ═══════════════════════════════════════════════════════════════

def generate_titles(n=20):
    """Generate title records (content releases)"""
    titles = []
    for i, name in enumerate(TITLES[:n]):
        brand = random.choice(BRANDS)
        release = datetime(2026, 2, 1) + timedelta(days=random.randint(0, 120))
        titles.append({
            "title_id": f"TTL-{i+1:04d}",
            "title_name": name,
            "brand": brand["airtable_value"],
            "brand_code": brand["code"],
            "product": brand["product"],
            "release_date": release.strftime("%Y-%m-%d"),
            "content_type": random.choice(["Series", "Film", "Special", "Live Event"]),
        })
    return pd.DataFrame(titles)


def generate_campaigns(titles_df, n=60):
    """Generate campaigns (Title x Campaign Objective)"""
    campaigns = []
    for i in range(n):
        title = titles_df.sample(1).iloc[0]
        objective = random.choice(CAMPAIGN_OBJECTIVES)
        market = random.choice(MARKETS)
        channel = random.choice(CHANNELS)
        start = datetime(2026, 2, 1) + timedelta(days=random.randint(0, 28))
        end = start + timedelta(days=random.randint(7, 90))
        budget = random.choice([25000, 50000, 100000, 250000, 500000, 750000, 1000000])

        # Build campaign name using Disney taxonomy pattern
        campaign_name = f"{title['brand_code']}_{title['title_name']}_{objective}_{market['geo']}_{channel['central_grid']}"

        campaigns.append({
            "campaign_id": f"CMP-{i+1:04d}",
            "title_id": title["title_id"],
            "title_name": title["title_name"],
            "brand": title["brand"],
            "brand_code": title["brand_code"],
            "product": title["product"],
            "campaign_name": campaign_name,
            "campaign_objective": objective,
            "targeting_geo": market["geo"],
            "country": market["country"],
            "language": market["lang"],
            "geo_cluster": market["cluster"],
            "region": market["region"],
            "channel": channel["airtable_value"],
            "channel_mapped": channel["central_grid"],
            "platform": random.choice(["CM360", "DV360", "Meta", "TikTok", "Amazon DSP", "Yahoo DSP"]),
            "budget_usd": budget,
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "status": random.choices(
                ["Active", "Paused", "Completed", "Pending Launch"],
                weights=[50, 10, 20, 20]
            )[0],
            "impressions_goal": budget * random.randint(8, 15),
            "flight_priority": random.choice([1, 2, 3]),
            "audience_tactic": random.choice(AUDIENCES)["tactic"],
            "audience_strategy": random.choice(AUDIENCES)["strategy"],
            "audience_detailed": random.choice(AUDIENCES)["detailed"],
        })
    return pd.DataFrame(campaigns)


def generate_delivery(campaigns_df, days=30):
    """Generate daily delivery metrics per campaign"""
    records = []
    for _, camp in campaigns_df.iterrows():
        if camp["status"] in ["Active", "Completed"]:
            daily_goal = camp["impressions_goal"] / max(
                (pd.to_datetime(camp["end_date"]) - pd.to_datetime(camp["start_date"])).days, 1
            )
            for day_offset in range(min(days, 30)):
                date = pd.to_datetime(camp["start_date"]) + timedelta(days=day_offset)
                if date > datetime(2026, 3, 1):
                    break
                delivery_factor = np.random.normal(1.0, 0.3)
                if random.random() < 0.05:
                    delivery_factor = 0.0  # Zero delivery event
                impressions = max(0, int(daily_goal * delivery_factor))
                clicks = int(impressions * random.uniform(0.001, 0.015))
                records.append({
                    "delivery_id": str(uuid.uuid4())[:8],
                    "campaign_id": camp["campaign_id"],
                    "date": date.strftime("%Y-%m-%d"),
                    "impressions": impressions,
                    "clicks": clicks,
                    "ctr": round(clicks / max(impressions, 1), 6),
                    "spend_usd": round(impressions * random.uniform(0.005, 0.025), 2),
                    "vast_errors": random.choices([0, 0, 0, 0, random.randint(1, 50)], weights=[70, 10, 10, 5, 5])[0],
                    "viewability_rate": round(random.uniform(0.40, 0.95), 3),
                })
    return pd.DataFrame(records)


def generate_tickets(campaigns_df, n=120):
    """Generate BOAT-style trafficking tickets with real ticket types and role routing"""
    tickets = []
    traffickers = [u for u in USERS if u["role"] == "Trafficker"]
    engineers = [u for u in USERS if u["role"] == "Engineer"]
    pms = [u for u in USERS if u["role"] == "Project Manager"]

    STAGES = ["New", "In Review", "Trafficking", "QA", "Ready to Launch",
              "Live", "Completed", "Blocked"]
    REQUESTERS = ["Agency Partner", "Product Marketing", "Analytics",
                  "Media Planning", "Account Management", "Performance Strategy"]

    for i in range(n):
        camp = campaigns_df.sample(1).iloc[0]
        ticket_type = random.choice(TICKET_TYPES)
        created = datetime(2026, 2, 1) + timedelta(days=random.randint(0, 25))
        urgency = random.choices(["Critical", "High", "Medium", "Low"], weights=[5, 15, 50, 30])[0]

        # Override SLA based on urgency for critical/high
        if urgency == "Critical":
            sla_hrs = min(ticket_type["sla_hours"], 4)
        elif urgency == "High":
            sla_hrs = min(ticket_type["sla_hours"], 8)
        else:
            sla_hrs = ticket_type["sla_hours"]

        # Route to correct role
        if ticket_type["routed_to"] == "Engineer":
            assignee = random.choice(engineers)
        elif ticket_type["routed_to"] == "Project Manager":
            assignee = random.choice(pms)
        else:
            assignee = random.choice(traffickers + [{"name": "Unassigned", "email": "", "role": "", "team": ""}])

        tickets.append({
            "ticket_id": f"TKT-{i+1:05d}",
            "campaign_id": camp["campaign_id"],
            "title": f"{ticket_type['type']} - {camp['title_name']} {camp['targeting_geo']} {camp['channel_mapped']}",
            "request_type": ticket_type["type"],
            "routed_to_role": ticket_type["routed_to"],
            "eve_eligible": ticket_type["eve_eligible"],
            "urgency": urgency,
            "stage": random.choice(STAGES),
            "platform": camp["platform"],
            "targeting_geo": camp["targeting_geo"],
            "brand": camp["brand_code"],
            "requested_by": random.choice(REQUESTERS),
            "created_date": created.strftime("%Y-%m-%d"),
            "due_date": (created + timedelta(hours=sla_hrs)).strftime("%Y-%m-%d %H:%M"),
            "assignee": assignee["name"],
            "assignee_role": assignee.get("role", ""),
            "sla_hours": sla_hrs,
            "notes": "",
        })
    return pd.DataFrame(tickets)


def generate_qa_checks(tickets_df):
    """Generate QA check records per ticket"""
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
    records = []
    qa_team = [u for u in USERS if u["role"] == "Trafficker"][:5]  # First 5 traffickers do QA

    for _, ticket in tickets_df.iterrows():
        if ticket["stage"] in ["QA", "Ready to Launch", "Live", "Completed"]:
            checks_to_run = random.sample(QA_CHECKS, k=random.randint(3, len(QA_CHECKS)))
            for check_name, check_detail in checks_to_run:
                checker = random.choice(qa_team)
                records.append({
                    "qa_id": f"QA-{uuid.uuid4().hex[:6].upper()}",
                    "ticket_id": ticket["ticket_id"],
                    "check_name": check_name,
                    "check_details": check_detail,
                    "result": random.choices(["Pass", "Fail", "Needs Review"], weights=[60, 15, 25])[0],
                    "checked_by": checker["name"],
                    "checked_at": (datetime.now() - timedelta(hours=random.randint(1, 72))).isoformat(),
                })
    return pd.DataFrame(records)


def generate_brand_mapping():
    """Export the VLOOKUP brand mapping table"""
    return pd.DataFrame(BRANDS)


def generate_channel_mapping():
    """Export the VLOOKUP channel mapping table"""
    return pd.DataFrame(CHANNELS)


def generate_markets():
    """Export the full markets & regions table"""
    return pd.DataFrame(MARKETS)


def generate_users():
    """Export the user roles glossary"""
    return pd.DataFrame(USERS)


def generate_ticket_types():
    """Export the ticket type definitions with routing rules"""
    return pd.DataFrame(TICKET_TYPES)


def generate_audiences():
    """Export the audience targeting glossary"""
    return pd.DataFrame(AUDIENCES)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    print("Generating Disney Ad Ops Lab data...\n")

    # Core operational data
    titles = generate_titles(20)
    campaigns = generate_campaigns(titles, 60)
    delivery = generate_delivery(campaigns)
    tickets = generate_tickets(campaigns, 120)
    qa_checks = generate_qa_checks(tickets)

    # Reference/lookup tables
    brand_map = generate_brand_mapping()
    channel_map = generate_channel_mapping()
    markets = generate_markets()
    users = generate_users()
    ticket_types = generate_ticket_types()
    audiences = generate_audiences()

    # Save core data
    titles.to_csv("data/01_titles.csv", index=False)
    campaigns.to_csv("data/02_campaigns.csv", index=False)
    delivery.to_csv("data/03_delivery.csv", index=False)
    tickets.to_csv("data/04_tickets.csv", index=False)
    qa_checks.to_csv("data/05_qa_checks.csv", index=False)

    # Save reference tables
    brand_map.to_csv("data/06_brand_mapping.csv", index=False)
    channel_map.to_csv("data/07_channel_mapping.csv", index=False)
    markets.to_csv("data/08_markets.csv", index=False)
    users.to_csv("data/09_users.csv", index=False)
    ticket_types.to_csv("data/10_ticket_types.csv", index=False)
    audiences.to_csv("data/11_audiences.csv", index=False)

    # Summary
    print(f"{'='*60}")
    print(f"CORE DATA")
    print(f"{'='*60}")
    print(f"  Titles:        {len(titles):>6} records")
    print(f"  Campaigns:     {len(campaigns):>6} records")
    print(f"  Delivery:      {len(delivery):>6} records")
    print(f"  Tickets:       {len(tickets):>6} records")
    print(f"  QA Checks:     {len(qa_checks):>6} records")
    print(f"\n{'='*60}")
    print(f"REFERENCE TABLES")
    print(f"{'='*60}")
    print(f"  Brands:        {len(brand_map):>6} entries")
    print(f"  Channels:      {len(channel_map):>6} entries")
    print(f"  Markets:       {len(markets):>6} entries")
    print(f"  Users:         {len(users):>6} entries")
    print(f"  Ticket Types:  {len(ticket_types):>6} entries")
    print(f"  Audiences:     {len(audiences):>6} entries")
    print(f"\nAll files saved to data/")

    # Quick stats
    print(f"\n{'='*60}")
    print(f"QUICK STATS")
    print(f"{'='*60}")
    print(f"  Regions covered: {campaigns['region'].nunique()} ({', '.join(campaigns['region'].unique())})")
    print(f"  Markets covered: {campaigns['targeting_geo'].nunique()}")
    print(f"  Brands: {campaigns['brand_code'].nunique()}")
    print(f"  Platforms: {campaigns['platform'].nunique()}")
    print(f"  EVE-eligible tickets: {tickets[tickets['eve_eligible']==True].shape[0]}")
    print(f"  Engineer tickets: {tickets[tickets['routed_to_role']=='Engineer'].shape[0]}")
    print(f"  Trafficker tickets: {tickets[tickets['routed_to_role']=='Trafficker'].shape[0]}")
    print(f"  Zero delivery events: {(delivery['impressions']==0).sum()}")
    print(f"  VAST error events: {(delivery['vast_errors']>0).sum()}")
    print(f"  QA failures: {(qa_checks['result']=='Fail').sum()}")
    print(f"  QA needs review: {(qa_checks['result']=='Needs Review').sum()}")
