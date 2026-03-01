"""
Silver Layer Transforms — Disney Ad Ops Lab
=============================================
PURPOSE:
  Silver is where data gets CLEANED and CONFORMED. You take the messy raw data
  from Bronze and turn it into reliable, queryable business data.

  Think of it like a quality control line in a factory:
  - Remove duplicates (same delivery row loaded twice)
  - Fix data types (strings that should be dates)
  - Validate business rules (budget can't be negative)
  - Standardize values (Meta vs. meta vs. META → "Meta")
  - Join reference data (campaign_id → full campaign details)

WHY THIS MATTERS IN AD OPS:
  - Platform APIs often send duplicate events
  - Different markets use different naming conventions
  - You need ONE source of truth for reporting, not 5 conflicting ones
  - Data quality issues here cascade into wrong pacing reports and bad decisions

KEY CONCEPTS:
  1. Slowly Changing Dimensions (SCD Type 2): Track campaign status changes over time
  2. Deduplication: Same delivery event from multiple API calls = count it once
  3. Data quality expectations: Enforce rules and quarantine bad data
  4. Surrogate keys: Generate stable row IDs independent of source systems
"""

from typing import Dict, List, Optional


# ─── Data Quality Rules ─────────────────────────────────────────────────────
# These rules define what "clean" data looks like.
# Any row that violates these gets quarantined (sent to a _quarantine table)
# instead of silently corrupting your Silver tables.

QUALITY_RULES = {
    "campaigns": [
        {
            "name": "budget_positive",
            "column": "budget_usd",
            "rule": "budget_usd > 0",
            "severity": "error",
            "description": "Campaign budget must be positive. Zero/negative budgets indicate data entry errors."
        },
        {
            "name": "valid_status",
            "column": "status",
            "rule": "status IN ('Active', 'Paused', 'Completed', 'Cancelled', 'Draft')",
            "severity": "error",
            "description": "Campaign status must be one of the allowed values."
        },
        {
            "name": "end_after_start",
            "column": "end_date",
            "rule": "end_date >= start_date",
            "severity": "error",
            "description": "Campaign end date cannot be before start date. This breaks pacing calculations."
        },
        {
            "name": "valid_objective",
            "column": "campaign_objective",
            "rule": "campaign_objective IN ('Acq', 'Awareness', 'Engagement', 'Retention', 'Conversion')",
            "severity": "warn",
            "description": "Campaign objective should match Disney's standard taxonomy."
        },
        {
            "name": "has_impressions_goal",
            "column": "impressions_goal",
            "rule": "impressions_goal > 0",
            "severity": "warn",
            "description": "Every active campaign should have an impressions goal for pacing."
        },
    ],
    "delivery": [
        {
            "name": "non_negative_impressions",
            "column": "impressions",
            "rule": "impressions >= 0",
            "severity": "error",
            "description": "Negative impressions indicate corrupted delivery data."
        },
        {
            "name": "non_negative_clicks",
            "column": "clicks",
            "rule": "clicks >= 0",
            "severity": "error",
            "description": "Negative clicks are impossible — quarantine these rows."
        },
        {
            "name": "clicks_leq_impressions",
            "column": "clicks",
            "rule": "clicks <= impressions",
            "severity": "warn",
            "description": "More clicks than impressions is suspicious (possible bot traffic)."
        },
        {
            "name": "valid_ctr",
            "column": "ctr",
            "rule": "ctr >= 0 AND ctr <= 1.0",
            "severity": "warn",
            "description": "CTR should be between 0% and 100%. Values over 100% indicate a calculation error."
        },
        {
            "name": "valid_viewability",
            "column": "viewability_rate",
            "rule": "viewability_rate >= 0 AND viewability_rate <= 1.0",
            "severity": "warn",
            "description": "Viewability rate is a percentage (0.0 to 1.0)."
        },
        {
            "name": "non_negative_spend",
            "column": "spend_usd",
            "rule": "spend_usd >= 0",
            "severity": "error",
            "description": "Negative spend indicates refund data that should be handled separately."
        },
    ],
    "tickets": [
        {
            "name": "valid_urgency",
            "column": "urgency",
            "rule": "urgency IN ('Low', 'Medium', 'High', 'Critical')",
            "severity": "warn",
            "description": "Urgency must match the standard SLA tiers."
        },
        {
            "name": "positive_sla",
            "column": "sla_hours",
            "rule": "sla_hours > 0",
            "severity": "error",
            "description": "SLA hours must be positive for breach tracking."
        },
    ],
}


# ─── Value Standardization Maps ─────────────────────────────────────────────
# Different API sources use different casing/naming for the same thing.
# Silver layer normalizes everything to a single canonical form.

PLATFORM_STANDARDIZATION = {
    "meta": "Meta",
    "Meta": "Meta",
    "META": "Meta",
    "facebook": "Meta",
    "Facebook": "Meta",
    "instagram": "Meta",
    "tiktok": "TikTok",
    "TikTok": "TikTok",
    "TIKTOK": "TikTok",
    "amazon dsp": "Amazon DSP",
    "Amazon DSP": "Amazon DSP",
    "cm360": "CM360",
    "CM360": "CM360",
    "dcm": "CM360",
    "DCM": "CM360",
    "dv360": "DV360",
    "DV360": "DV360",
    "snap": "Snapchat",
    "Snapchat": "Snapchat",
    "SNAP": "Snapchat",
}


def generate_silver_campaign_sql(catalog: str = "hive_metastore",
                                  bronze_schema: str = "adops_bronze",
                                  silver_schema: str = "adops_silver") -> str:
    """
    Generates the Silver campaigns table from Bronze.
    
    SCD TYPE 2 EXPLAINED (Slowly Changing Dimensions):
    When a campaign status changes from "Active" to "Paused", you don't just
    UPDATE the row. Instead, you:
    1. Mark the old row as expired (set _valid_to = today)
    2. Insert a new row with the current status (set _valid_from = today)
    
    This gives you a complete history: "Campaign ABC was Active from Jan 1-15,
    then Paused from Jan 15-20, then Active again from Jan 20 onward."
    
    Why? Because when your VP asks "how many campaigns were active on Jan 10th?"
    you can answer that precisely instead of just knowing the current state.
    """
    return f"""
-- =====================================================================
-- Silver Campaigns: Cleaned, deduplicated, with SCD Type 2 history
-- =====================================================================
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.campaigns AS

WITH deduplicated AS (
    -- Step 1: DEDUPLICATE
    -- Bronze might have the same campaign loaded multiple times (re-ingestion)
    -- ROW_NUMBER() keeps only the latest version of each campaign
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id 
            ORDER BY _ingested_at DESC  -- Keep the most recently ingested version
        ) as row_num
    FROM {catalog}.{bronze_schema}.campaigns
),
cleaned AS (
    -- Step 2: CLEAN & STANDARDIZE
    SELECT
        campaign_id,
        title_id,
        TRIM(title_name) as title_name,
        TRIM(brand) as brand,
        UPPER(TRIM(brand_code)) as brand_code,  -- Standardize brand codes to uppercase
        TRIM(product) as product,
        TRIM(campaign_name) as campaign_name,
        TRIM(campaign_objective) as campaign_objective,
        UPPER(TRIM(targeting_geo)) as targeting_geo,  -- ISO country codes are uppercase
        TRIM(country) as country,
        UPPER(TRIM(language)) as language,
        TRIM(geo_cluster) as geo_cluster,
        TRIM(region) as region,
        TRIM(channel) as channel,
        TRIM(channel_mapped) as channel_mapped,
        
        -- Platform standardization: "meta" → "Meta", "dcm" → "CM360"
        CASE
            WHEN LOWER(TRIM(platform)) IN ('meta', 'facebook', 'instagram') THEN 'Meta'
            WHEN LOWER(TRIM(platform)) IN ('tiktok') THEN 'TikTok'
            WHEN LOWER(TRIM(platform)) IN ('cm360', 'dcm') THEN 'CM360'
            WHEN LOWER(TRIM(platform)) IN ('dv360') THEN 'DV360'
            WHEN LOWER(TRIM(platform)) IN ('amazon dsp') THEN 'Amazon DSP'
            WHEN LOWER(TRIM(platform)) IN ('snap', 'snapchat') THEN 'Snapchat'
            ELSE TRIM(platform)
        END as platform,
        
        -- Budget validation: Negative budgets → NULL with flag
        CASE WHEN budget_usd > 0 THEN budget_usd ELSE NULL END as budget_usd,
        CASE WHEN budget_usd <= 0 THEN true ELSE false END as _budget_flagged,
        
        start_date,
        end_date,
        
        CASE 
            WHEN TRIM(status) IN ('Active', 'Paused', 'Completed', 'Cancelled', 'Draft') 
            THEN TRIM(status)
            ELSE 'Unknown'
        END as status,
        
        CASE WHEN impressions_goal > 0 THEN impressions_goal ELSE NULL END as impressions_goal,
        flight_priority,
        TRIM(audience_tactic) as audience_tactic,
        TRIM(audience_strategy) as audience_strategy,
        TRIM(audience_detailed) as audience_detailed,
        
        -- Computed fields (derived in Silver, not in Bronze)
        datediff(end_date, start_date) as flight_duration_days,
        CASE 
            WHEN impressions_goal > 0 AND budget_usd > 0 
            THEN ROUND(budget_usd / (impressions_goal / 1000.0), 4)
            ELSE NULL 
        END as target_cpm,
        
        -- Metadata propagation
        _ingested_at,
        _source_file,
        current_timestamp() as _silver_processed_at
        
    FROM deduplicated
    WHERE row_num = 1  -- Only keep the latest version
)
SELECT * FROM cleaned
WHERE campaign_id IS NOT NULL;  -- Final safety: drop rows with no key
"""


def generate_silver_delivery_sql(catalog: str = "hive_metastore",
                                  bronze_schema: str = "adops_bronze",
                                  silver_schema: str = "adops_silver") -> str:
    """
    Silver delivery: Deduplicated daily delivery facts with computed metrics.
    
    This is the table most ad ops analysts query daily. It answers:
    - "How many impressions did campaign X deliver yesterday?"
    - "What's our viewability rate across Meta vs CM360?"
    - "Are we overspending on any platform?"
    """
    return f"""
-- =====================================================================
-- Silver Delivery: Deduplicated facts with derived metrics
-- =====================================================================
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.delivery AS

WITH deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY delivery_id
            ORDER BY _ingested_at DESC
        ) as row_num
    FROM {catalog}.{bronze_schema}.delivery
),
cleaned AS (
    SELECT
        delivery_id,
        campaign_id,
        date as delivery_date,  -- Rename for clarity
        
        -- Clamp negative values to 0 with flag
        GREATEST(impressions, 0) as impressions,
        GREATEST(clicks, 0) as clicks,
        GREATEST(spend_usd, 0) as spend_usd,
        GREATEST(vast_errors, 0) as vast_errors,
        
        -- Recompute CTR from source values (don't trust pre-computed values)
        CASE 
            WHEN impressions > 0 THEN ROUND(clicks * 1.0 / impressions, 6)
            ELSE 0.0
        END as ctr,
        
        -- Clamp viewability to valid range
        CASE
            WHEN viewability_rate BETWEEN 0 AND 1 THEN viewability_rate
            WHEN viewability_rate > 1 THEN 1.0  -- Cap at 100%
            ELSE 0.0
        END as viewability_rate,
        
        -- Derived metrics that ad ops teams care about
        CASE 
            WHEN impressions > 0 THEN ROUND(spend_usd / (impressions / 1000.0), 4)
            ELSE NULL
        END as cpm,  -- Cost per mille (per 1000 impressions)
        
        CASE 
            WHEN clicks > 0 THEN ROUND(spend_usd / clicks, 4)
            ELSE NULL
        END as cpc,  -- Cost per click
        
        CASE 
            WHEN impressions + vast_errors > 0 
            THEN ROUND(vast_errors * 1.0 / (impressions + vast_errors), 6)
            ELSE 0.0
        END as vast_error_rate,
        
        -- Data quality flags
        CASE WHEN clicks > impressions THEN true ELSE false END as _suspicious_ctr,
        CASE WHEN impressions = 0 AND spend_usd > 0 THEN true ELSE false END as _zero_delivery_nonzero_spend,
        
        _ingested_at,
        _source_file,
        current_timestamp() as _silver_processed_at
        
    FROM deduplicated
    WHERE row_num = 1
)
SELECT * FROM cleaned
WHERE delivery_id IS NOT NULL
  AND campaign_id IS NOT NULL;
"""


def generate_silver_tickets_sql(catalog: str = "hive_metastore",
                                 bronze_schema: str = "adops_bronze",
                                 silver_schema: str = "adops_silver") -> str:
    """
    Silver tickets: Clean trafficking tickets with SLA computations.
    """
    return f"""
-- =====================================================================
-- Silver Tickets: Cleaned with SLA breach calculations
-- =====================================================================
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.tickets AS

WITH deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ticket_id
            ORDER BY _ingested_at DESC
        ) as row_num
    FROM {catalog}.{bronze_schema}.tickets
),
cleaned AS (
    SELECT
        ticket_id,
        campaign_id,
        TRIM(title) as title,
        TRIM(request_type) as request_type,
        TRIM(routed_to_role) as routed_to_role,
        eve_eligible,
        
        CASE 
            WHEN TRIM(urgency) IN ('Low', 'Medium', 'High', 'Critical') THEN TRIM(urgency)
            ELSE 'Medium'  -- Default unknown urgency to Medium
        END as urgency,
        
        TRIM(stage) as stage,
        
        -- Standardize platform same as campaigns
        CASE
            WHEN LOWER(TRIM(platform)) IN ('meta', 'facebook') THEN 'Meta'
            WHEN LOWER(TRIM(platform)) IN ('tiktok') THEN 'TikTok'
            WHEN LOWER(TRIM(platform)) IN ('cm360', 'dcm') THEN 'CM360'
            ELSE TRIM(platform)
        END as platform,
        
        UPPER(TRIM(targeting_geo)) as targeting_geo,
        TRIM(brand) as brand,
        TRIM(requested_by) as requested_by,
        created_date,
        
        -- Parse due_date from string to timestamp
        TRY_CAST(due_date AS TIMESTAMP) as due_date,
        
        TRIM(assignee) as assignee,
        TRIM(assignee_role) as assignee_role,
        GREATEST(sla_hours, 1) as sla_hours,
        notes,
        
        -- SLA calculations
        CASE 
            WHEN TRY_CAST(due_date AS TIMESTAMP) < current_timestamp() 
                 AND TRIM(stage) NOT IN ('Completed', 'Live')
            THEN true
            ELSE false
        END as is_breached,
        
        CASE 
            WHEN TRY_CAST(due_date AS TIMESTAMP) IS NOT NULL 
            THEN ROUND(
                (unix_timestamp(TRY_CAST(due_date AS TIMESTAMP)) - unix_timestamp(current_timestamp())) / 3600.0,
                1
            )
            ELSE NULL
        END as hours_until_due,
        
        _ingested_at,
        current_timestamp() as _silver_processed_at
        
    FROM deduplicated
    WHERE row_num = 1
)
SELECT * FROM cleaned
WHERE ticket_id IS NOT NULL;
"""


def generate_quarantine_sql(table_name: str, catalog: str = "hive_metastore",
                             bronze_schema: str = "adops_bronze",
                             quarantine_schema: str = "adops_quarantine") -> str:
    """
    Generates quarantine table SQL for rows that fail data quality checks.
    
    QUARANTINE PATTERN:
    Instead of silently dropping bad rows (you lose data) or keeping them
    (they corrupt reports), you move them to a separate quarantine table.
    
    Data engineers review quarantined rows periodically and either:
    1. Fix the source system and re-ingest
    2. Apply manual corrections and promote to Silver
    3. Confirm they're truly bad and archive them
    """
    if table_name not in QUALITY_RULES:
        return f"-- No quality rules defined for {table_name}"
    
    rules = QUALITY_RULES[table_name]
    error_rules = [r for r in rules if r["severity"] == "error"]
    
    conditions = []
    for rule in error_rules:
        conditions.append(f"NOT ({rule['rule']})")
    
    if not conditions:
        return f"-- No error-level rules for {table_name}"
    
    where_clause = " OR ".join(conditions)
    
    return f"""
-- Quarantine: Rows from {table_name} that fail critical quality checks
CREATE OR REPLACE TABLE {catalog}.{quarantine_schema}.{table_name}_quarantine AS
SELECT 
    *,
    current_timestamp() as _quarantined_at,
    '{', '.join(r['name'] for r in error_rules)}' as _failed_checks
FROM {catalog}.{bronze_schema}.{table_name}
WHERE {where_clause};
"""
