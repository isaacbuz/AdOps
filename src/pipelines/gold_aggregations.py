"""
Gold Layer Aggregations — Disney Ad Ops Lab
=============================================
PURPOSE:
  Gold is the BUSINESS layer. These tables are designed to be queried directly
  by stakeholders, dashboards, and BI tools. No joins needed — everything is
  pre-computed and optimized for fast reads.

  Think of Gold tables as the "answers" to business questions:
  - "What's our pacing status across all campaigns?"
  - "Which campaigns need immediate attention?"
  - "How is each platform performing this week vs. last week?"
  - "What's our operational efficiency (EVE automation rate)?"

WHY THIS MATTERS IN AD OPS:
  - Your VP should be able to open a dashboard and see pacing status in 2 seconds
  - Campaign managers need daily "action items" (under-pacing, SLA breaches)
  - Finance needs accurate spend/pacing data for budget reconciliation
  - Data gets stale fast — Gold tables are refreshed on a schedule

KEY CONCEPTS:
  1. Pre-aggregated KPIs: Don't make users write complex SQL
  2. Window functions: Week-over-week comparisons, running totals
  3. Materialized views: Trade storage for query speed
  4. Business-friendly naming: No abbreviations, no internal codes
"""


def generate_gold_campaign_performance_sql(
    catalog: str = "hive_metastore",
    silver_schema: str = "adops_silver",
    gold_schema: str = "adops_gold"
) -> str:
    """
    Campaign Performance Summary — the #1 table for ad ops managers.
    
    This answers: "For every active campaign, what's our delivery status,
    pacing health, and spend efficiency?"
    
    PACING EXPLAINED:
    If a campaign runs Jan 1-31 with a 100K impression goal:
    - On Jan 15 (50% of time elapsed), you should have ~50K impressions
    - If you have 70K → Over-pacing (you'll exhaust budget early)
    - If you have 30K → Under-pacing (you won't hit the goal)
    - Pacing ratio = actual_delivery% / expected_delivery%
    """
    return f"""
-- =====================================================================
-- Gold: Campaign Performance Summary
-- Refreshed daily. This is what dashboards pull from.
-- =====================================================================
CREATE OR REPLACE TABLE {catalog}.{gold_schema}.campaign_performance AS

WITH delivery_totals AS (
    -- Aggregate all delivery data per campaign
    SELECT
        campaign_id,
        COUNT(DISTINCT delivery_date) as days_with_delivery,
        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(spend_usd) as total_spend,
        SUM(vast_errors) as total_vast_errors,
        AVG(viewability_rate) as avg_viewability,
        AVG(cpm) as avg_cpm,
        AVG(cpc) as avg_cpc,
        MAX(delivery_date) as last_delivery_date,
        MIN(delivery_date) as first_delivery_date,
        
        -- Trailing 7-day metrics (last week's performance)
        SUM(CASE WHEN delivery_date >= date_sub(current_date(), 7) THEN impressions ELSE 0 END) as impressions_last_7d,
        SUM(CASE WHEN delivery_date >= date_sub(current_date(), 7) THEN spend_usd ELSE 0 END) as spend_last_7d,
        SUM(CASE WHEN delivery_date >= date_sub(current_date(), 7) THEN clicks ELSE 0 END) as clicks_last_7d,
        
        -- Count anomalous days
        SUM(CASE WHEN impressions = 0 THEN 1 ELSE 0 END) as zero_delivery_days,
        SUM(CASE WHEN _suspicious_ctr THEN 1 ELSE 0 END) as suspicious_ctr_days
        
    FROM {catalog}.{silver_schema}.delivery
    GROUP BY campaign_id
),
campaign_pacing AS (
    SELECT
        c.campaign_id,
        c.campaign_name,
        c.brand,
        c.brand_code,
        c.product,
        c.title_name,
        c.campaign_objective,
        c.platform,
        c.region,
        c.targeting_geo,
        c.country,
        c.channel_mapped,
        c.status,
        c.budget_usd,
        c.impressions_goal,
        c.start_date,
        c.end_date,
        c.flight_duration_days,
        c.target_cpm,
        c.audience_tactic,
        
        -- Delivery totals
        COALESCE(d.total_impressions, 0) as total_impressions,
        COALESCE(d.total_clicks, 0) as total_clicks,
        COALESCE(d.total_spend, 0) as total_spend,
        COALESCE(d.total_vast_errors, 0) as total_vast_errors,
        d.avg_viewability,
        d.avg_cpm,
        d.avg_cpc,
        d.last_delivery_date,
        d.days_with_delivery,
        COALESCE(d.zero_delivery_days, 0) as zero_delivery_days,
        
        -- Trailing window
        COALESCE(d.impressions_last_7d, 0) as impressions_last_7d,
        COALESCE(d.spend_last_7d, 0) as spend_last_7d,
        
        -- Pacing calculations
        CASE WHEN c.impressions_goal > 0 
            THEN ROUND(COALESCE(d.total_impressions, 0) * 100.0 / c.impressions_goal, 2)
            ELSE NULL
        END as delivery_pct,
        
        CASE WHEN c.flight_duration_days > 0
            THEN ROUND(
                GREATEST(datediff(LEAST(current_date(), c.end_date), c.start_date), 0) 
                * 100.0 / c.flight_duration_days, 2
            )
            ELSE NULL
        END as time_elapsed_pct,
        
        -- Budget utilization
        CASE WHEN c.budget_usd > 0
            THEN ROUND(COALESCE(d.total_spend, 0) * 100.0 / c.budget_usd, 2)
            ELSE NULL
        END as budget_utilization_pct
        
    FROM {catalog}.{silver_schema}.campaigns c
    LEFT JOIN delivery_totals d ON c.campaign_id = d.campaign_id
)
SELECT
    *,
    
    -- PACING STATUS: The key business logic
    -- Compares delivery_pct vs time_elapsed_pct
    CASE
        WHEN status != 'Active' THEN 'N/A'
        WHEN delivery_pct IS NULL OR time_elapsed_pct IS NULL THEN 'Unknown'
        WHEN time_elapsed_pct = 0 THEN 'Not Started'
        WHEN delivery_pct / time_elapsed_pct < 0.8 THEN '🔴 Under-Pacing'
        WHEN delivery_pct / time_elapsed_pct > 1.2 THEN '🟡 Over-Pacing'
        ELSE '🟢 On Track'
    END as pacing_status,
    
    -- PACING RATIO for sorting/filtering
    CASE 
        WHEN time_elapsed_pct > 0 THEN ROUND(delivery_pct / time_elapsed_pct, 3)
        ELSE NULL
    END as pacing_ratio,
    
    -- ALERT PRIORITY: What needs attention RIGHT NOW?
    CASE
        WHEN status = 'Active' AND zero_delivery_days >= 3 THEN '🚨 Critical: No Delivery'
        WHEN status = 'Active' AND delivery_pct IS NOT NULL AND time_elapsed_pct > 0
             AND delivery_pct / time_elapsed_pct < 0.5 THEN '🚨 Critical: Severe Under-Pacing'
        WHEN status = 'Active' AND budget_utilization_pct > 90 
             AND delivery_pct < 80 THEN '⚠️ High: Budget Draining Fast'
        WHEN status = 'Active' AND delivery_pct IS NOT NULL AND time_elapsed_pct > 0
             AND delivery_pct / time_elapsed_pct < 0.8 THEN '⚠️ High: Under-Pacing'
        WHEN status = 'Active' AND delivery_pct IS NOT NULL AND time_elapsed_pct > 0
             AND delivery_pct / time_elapsed_pct > 1.5 THEN '⚠️ High: Severe Over-Pacing'
        ELSE '✅ Normal'
    END as alert_priority,
    
    -- Forecasted end-of-flight delivery
    CASE 
        WHEN days_with_delivery > 0 AND flight_duration_days > 0
        THEN ROUND(
            (total_impressions * 1.0 / GREATEST(days_with_delivery, 1)) * flight_duration_days,
            0
        )
        ELSE NULL
    END as forecasted_total_impressions,
    
    current_timestamp() as _gold_refreshed_at

FROM campaign_pacing;
"""


def generate_gold_daily_ops_summary_sql(
    catalog: str = "hive_metastore",
    silver_schema: str = "adops_silver",
    gold_schema: str = "adops_gold"
) -> str:
    """
    Daily Operations Summary — one row per day with all key metrics.
    Perfect for time-series dashboards and trend analysis.
    """
    return f"""
-- =====================================================================
-- Gold: Daily Operations Summary
-- One row per day — ideal for trend charts and daily standup reports
-- =====================================================================
CREATE OR REPLACE TABLE {catalog}.{gold_schema}.daily_ops_summary AS

SELECT
    d.delivery_date,
    
    -- Volume metrics
    COUNT(DISTINCT d.campaign_id) as active_campaigns,
    SUM(d.impressions) as total_impressions,
    SUM(d.clicks) as total_clicks,
    SUM(d.spend_usd) as total_spend,
    
    -- Efficiency metrics
    ROUND(AVG(d.ctr) * 100, 4) as avg_ctr_pct,
    ROUND(AVG(d.cpm), 2) as avg_cpm,
    ROUND(AVG(d.viewability_rate) * 100, 2) as avg_viewability_pct,
    
    -- Quality metrics
    SUM(d.vast_errors) as total_vast_errors,
    ROUND(SUM(d.vast_errors) * 100.0 / NULLIF(SUM(d.impressions) + SUM(d.vast_errors), 0), 4) as vast_error_rate_pct,
    SUM(CASE WHEN d.impressions = 0 THEN 1 ELSE 0 END) as zero_delivery_count,
    
    -- Platform breakdown
    COUNT(DISTINCT CASE WHEN c.platform = 'Meta' THEN d.campaign_id END) as meta_campaigns,
    COUNT(DISTINCT CASE WHEN c.platform = 'CM360' THEN d.campaign_id END) as cm360_campaigns,
    COUNT(DISTINCT CASE WHEN c.platform = 'TikTok' THEN d.campaign_id END) as tiktok_campaigns,
    COUNT(DISTINCT CASE WHEN c.platform = 'Amazon DSP' THEN d.campaign_id END) as amazon_campaigns,
    
    -- Week-over-week comparisons using window functions
    LAG(SUM(d.impressions), 7) OVER (ORDER BY d.delivery_date) as impressions_7d_ago,
    ROUND(
        (SUM(d.impressions) - LAG(SUM(d.impressions), 7) OVER (ORDER BY d.delivery_date))
        * 100.0 / NULLIF(LAG(SUM(d.impressions), 7) OVER (ORDER BY d.delivery_date), 0),
        2
    ) as impressions_wow_change_pct,
    
    current_timestamp() as _gold_refreshed_at

FROM {catalog}.{silver_schema}.delivery d
LEFT JOIN {catalog}.{silver_schema}.campaigns c ON d.campaign_id = c.campaign_id
GROUP BY d.delivery_date
ORDER BY d.delivery_date;
"""


def generate_gold_platform_scorecard_sql(
    catalog: str = "hive_metastore",
    silver_schema: str = "adops_silver",
    gold_schema: str = "adops_gold"
) -> str:
    """
    Platform Scorecard — compare performance across Meta, CM360, TikTok, etc.
    This is what gets presented in weekly platform review meetings.
    """
    return f"""
-- =====================================================================
-- Gold: Platform Scorecard
-- Compare performance across ad platforms at a glance
-- =====================================================================
CREATE OR REPLACE TABLE {catalog}.{gold_schema}.platform_scorecard AS

SELECT
    c.platform,
    c.region,
    
    -- Campaign metrics
    COUNT(DISTINCT c.campaign_id) as total_campaigns,
    COUNT(DISTINCT CASE WHEN c.status = 'Active' THEN c.campaign_id END) as active_campaigns,
    SUM(c.budget_usd) as total_budget,
    
    -- Delivery metrics
    SUM(d.total_impressions) as total_impressions,
    SUM(d.total_clicks) as total_clicks,
    SUM(d.total_spend) as total_spend,
    
    -- Efficiency
    ROUND(SUM(d.total_spend) * 100.0 / NULLIF(SUM(c.budget_usd), 0), 2) as budget_utilization_pct,
    ROUND(SUM(d.total_spend) / NULLIF(SUM(d.total_impressions) / 1000.0, 0), 2) as blended_cpm,
    ROUND(SUM(d.total_clicks) * 100.0 / NULLIF(SUM(d.total_impressions), 0), 4) as blended_ctr_pct,
    
    -- Quality
    ROUND(AVG(d.avg_viewability) * 100, 2) as avg_viewability_pct,
    SUM(d.total_vast_errors) as total_vast_errors,
    
    current_timestamp() as _gold_refreshed_at

FROM {catalog}.{silver_schema}.campaigns c
LEFT JOIN (
    SELECT 
        campaign_id,
        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(spend_usd) as total_spend,
        AVG(viewability_rate) as avg_viewability,
        SUM(vast_errors) as total_vast_errors
    FROM {catalog}.{silver_schema}.delivery
    GROUP BY campaign_id
) d ON c.campaign_id = d.campaign_id
GROUP BY c.platform, c.region
ORDER BY total_budget DESC;
"""


def generate_gold_ops_efficiency_sql(
    catalog: str = "hive_metastore",
    silver_schema: str = "adops_silver",
    gold_schema: str = "adops_gold"
) -> str:
    """
    Operational Efficiency — EVE automation metrics and team workload.
    Shows how much manual trafficking work EVE is automating.
    """
    return f"""
-- =====================================================================
-- Gold: Operational Efficiency / EVE Automation Metrics
-- Tracks automation rate, SLA performance, and team workload
-- =====================================================================
CREATE OR REPLACE TABLE {catalog}.{gold_schema}.ops_efficiency AS

SELECT
    -- Automation metrics
    COUNT(*) as total_tickets,
    SUM(CASE WHEN eve_eligible THEN 1 ELSE 0 END) as eve_eligible_tickets,
    ROUND(SUM(CASE WHEN eve_eligible THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as automation_rate_pct,
    
    -- SLA performance
    SUM(CASE WHEN is_breached THEN 1 ELSE 0 END) as breached_tickets,
    ROUND(SUM(CASE WHEN is_breached THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as breach_rate_pct,
    
    -- Workload by urgency
    SUM(CASE WHEN urgency = 'Critical' THEN 1 ELSE 0 END) as critical_tickets,
    SUM(CASE WHEN urgency = 'High' THEN 1 ELSE 0 END) as high_tickets,
    SUM(CASE WHEN urgency = 'Medium' THEN 1 ELSE 0 END) as medium_tickets,
    SUM(CASE WHEN urgency = 'Low' THEN 1 ELSE 0 END) as low_tickets,
    
    -- Stage distribution
    SUM(CASE WHEN stage = 'Completed' THEN 1 ELSE 0 END) as completed_tickets,
    SUM(CASE WHEN stage = 'Live' THEN 1 ELSE 0 END) as live_tickets,
    SUM(CASE WHEN stage IN ('QA', 'Pending', 'In Progress') THEN 1 ELSE 0 END) as in_progress_tickets,
    
    -- Per-assignee breakdown (top 10 busiest)
    current_timestamp() as _gold_refreshed_at
    
FROM {catalog}.{silver_schema}.tickets;

-- Supplemental: Per-assignee workload for manager dashboards
CREATE OR REPLACE TABLE {catalog}.{gold_schema}.assignee_workload AS
SELECT
    assignee,
    assignee_role,
    COUNT(*) as total_tickets,
    SUM(CASE WHEN is_breached THEN 1 ELSE 0 END) as breached_tickets,
    SUM(CASE WHEN urgency IN ('Critical', 'High') THEN 1 ELSE 0 END) as urgent_tickets,
    SUM(CASE WHEN stage NOT IN ('Completed', 'Live') THEN 1 ELSE 0 END) as open_tickets,
    ROUND(AVG(hours_until_due), 1) as avg_hours_until_due,
    current_timestamp() as _gold_refreshed_at
FROM {catalog}.{silver_schema}.tickets
WHERE assignee IS NOT NULL AND assignee != ''
GROUP BY assignee, assignee_role
ORDER BY breached_tickets DESC, urgent_tickets DESC;
"""
