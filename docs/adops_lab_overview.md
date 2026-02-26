# The Disney Ad Ops Lab: Complete System Overview

Welcome to the Disney Ad Ops Lab! You are building a simulated, end-to-end version of the technological infrastructure that powers Disney's multi-million dollar advertising operations. 

To understand what we are building, you must first understand the problem we are solving.

## The Core Problem in Ad Operations
When Disney launches a new marketing campaign (e.g., *Moana Live Action*), they don't just put one billboard up. They run thousands of digital ads simultaneously across Facebook, YouTube, TikTok, connected TVs (Hulu), and programmatic display networks across 30+ different countries. 

If humans have to manually type out the names, tags, and budgets for all those thousands of ads across different platforms, three things happen:
1. **It takes forever** (Missed deadlines, SLA breaches).
2. **Humans make typos** (Someone types "Moaba" instead of "Moana" in the tracking link).
3. **Data breaks down** (Because of the typo, the analytics team in Databricks can't track how much money that specific ad made, leading to millions of dollars in "lost" or untrackable spend).

## The Solution: A Two-Part Automation System
The goal of this lab is to build an ecosystem that completely eliminates human error via rigid data structures and Python automation. We built this using two entirely different platforms that work together:

### Part 1: Airtable (The "BOAT" System - Base of Ad Tags)
This is the human-facing operational dashboard. It replaces chaotic email threads, Jira tickets, and messy Google Sheets. When a media planner wants to launch a campaign, they come here.

#### The Tables & Why They Relate
We didn't just dump all data into one massive spreadsheet. We built a **Relational Database Model**.

**1. The "Single Source of Truth" (Reference Tables)**
*   **Tables:** `Markets`, `Brand Mapping`, `Channel Mapping`, `Audiences`, `Users`, `Ticket Types`.
*   **Why:** We never want a human to type "United States". We want them to *select* it from the `Markets` table. This guarantees that 100% of campaigns use the exact same spelling, taxonomy, and internal codes. 

**2. The Campaign Hierarchy (The Core Business)**
*   **Tables:** `Titles` -> `Campaigns` -> `Tickets`.
*   **Why they relate:** 
    *   A **Title** (e.g., *Loki Season 3*) is the intellectual property. 
    *   A **Campaign** is the specific marketing push (e.g., "Loki UK Display Ads"). The Campaign *links* to the Title so it inherits all the brand rules.
    *   A **Ticket** is the actual work order asking a trafficker to build the ads. It *links* to the Campaign.
*   **The benefit:** If the release date for *Loki Season 3* changes in the `Titles` table, every single Campaign and Ticket linked to it instantly updates. No one has to manually update 50 different spreadsheets.

**3. The Safety Net (QA)**
*   **Table:** `QA Checks`. 
*   **Why it relates:** It links directly back to the `Tickets` table. A ticket cannot move from "Trafficking" to "Ready to Launch" unless all corresponding QA checks (which are automated by our Python script) pass.

### Part 2: The Python Orchestrator (The "EVE" Engine)
This is the invisible "brain" running on your machine (`src/orchestrator.py`).
Instead of humans manually logging into Facebook or Google (CM360) to build the ads, this engine:
1. Constantly scans Airtable for new `Tickets` that are in the "Trafficking" stage.
2. Reads the perfectly linked data (Brand Code + Title + Geo + Platform).
3. Constructs flawless, machine-readable pipeline instructions (the "Taxonomy" string).
4. Runs automated tests (the `QA Engine`) to ensure the ad isn't breaking brand safety rules (e.g., ensuring R-rated content isn't targeting kids).
5. Updates the Airtable status automatically and sends Slack/Teams alerts if there is a breach.

### Part 3: Databricks (The Analytics Data Lakehouse)
Airtable is great for managing a few thousand tasks, but once those ads go live on the internet, they generate millions or billions of rows of impression and click data every single day. Airtable would crash instantly if you tried to load that data.

This is why we leverage **Databricks**:
1. **The Heavy Lifter:** The massive, simulated `delivery.csv` data (impressions, spend, errors) lives *only* in Databricks.
2. **The "Join":** However, that massive delivery data only contains random IDs (like `CMP-0042`). We also upload the `Campaigns` data (from Airtable) into Databricks. Databricks then uses SQL to seamlessly join those two datasets together. 
3. **The Dashboards:** Databricks crunches the billions of rows to spit out the `Campaign Health` dashboards you ran earlier (e.g., "Which campaigns are under-pacing?", "Which campaigns have VAST video errors?", "Which adops team members are breaching their 24-hour SLAs?").

## Summary of Accomplishments
By building this lab, you have effectively architected a modern, enterprise-grade data pipeline:
*   **Structured UI (Airtable):** For flawless human data entry and task management.
*   **Automation Engine (Python):** To execute trafficking tasks without human intervention and enforce QA rules.
*   **Big Data Analytics (Databricks):** To track performance, pacing, and systemic errors at a massive scale.
