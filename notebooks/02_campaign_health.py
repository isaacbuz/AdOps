# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2: Campaign Health Monitor
# MAGIC **Disney Ad Ops Lab — Databricks Analytics**
# MAGIC
# MAGIC Replaces daily/weekly email-based reporting with dynamic views.

# COMMAND ----------
# MAGIC %sql
# MAGIC USE adops_lab;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Zero Delivery Alert
# MAGIC Identifies active campaigns that recorded 0 impressions on the most recent tracking date.

# COMMAND ----------
zero_delivery_sql = """
WITH max_date AS (
   SELECT MAX(date) AS last_date FROM delivery
)
SELECT 
    c.campaign_name, 
    c.brand as advertiser, 
    c.platform, 
    c.budget_usd
FROM delivery d
JOIN campaigns c ON d.campaign_id = c.campaign_id
JOIN max_date md ON d.date = md.last_date
WHERE d.impressions = 0
  AND c.status = 'Active'
ORDER BY c.budget_usd DESC
"""
display(spark.sql(zero_delivery_sql))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. VAST Error Report
# MAGIC Summarizes VAST errors over the life of the campaign to find high-failure video assets.

# COMMAND ----------
vast_err_sql = """
WITH agg AS (
    SELECT 
        campaign_id,
        SUM(vast_errors) as total_errors,
        SUM(impressions) as total_imps
    FROM delivery
    GROUP BY campaign_id
)
SELECT 
    c.campaign_name,
    c.platform,
    a.total_errors as vast_errors,
    CASE WHEN a.total_imps + a.total_errors > 0 
         THEN ROUND((a.total_errors / (a.total_imps + a.total_errors)) * 100, 2)
         ELSE 0.0 END as error_rate_pct
FROM agg a
JOIN campaigns c ON a.campaign_id = c.campaign_id
WHERE a.total_errors > 0
ORDER BY a.total_errors DESC
"""
display(spark.sql(vast_err_sql))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Campaign Pacing Analysis
# MAGIC Classifies pacing into "Under-pacing", "Over-pacing", and "On Track".

# COMMAND ----------
pacing_sql = """
WITH campaign_delivery AS (
    SELECT 
        campaign_id,
        SUM(impressions) as delivered_impressions,
        MAX(date) as last_delivery_date
    FROM delivery
    GROUP BY campaign_id
),
pacing_data AS (
    SELECT 
        c.campaign_name,
        c.brand,
        c.platform,
        c.budget_usd,
        c.impressions_goal,
        d.delivered_impressions,
        c.status,
        -- Total flight days
        datediff(c.end_date, c.start_date) as flight_days,
        -- Elapsed days up to last delivery
        datediff(d.last_delivery_date, c.start_date) as days_elapsed,
        
        -- Percentage of impressions delivered so far
        (d.delivered_impressions / NULLIF(c.impressions_goal, 0)) * 100 as pacing_pct,
        
        -- Percentage of time elapsed
        (datediff(d.last_delivery_date, c.start_date) / NULLIF(datediff(c.end_date, c.start_date), 0)) * 100 as expected_pacing_pct
    FROM campaigns c
    JOIN campaign_delivery d ON c.campaign_id = d.campaign_id
    WHERE c.status = 'Active' 
      AND datediff(c.end_date, c.start_date) > 0
)
SELECT 
    campaign_name,
    brand,
    platform,
    budget_usd,
    ROUND(pacing_pct, 2) as pacing_pct,
    ROUND(expected_pacing_pct, 2) as expected_pacing_pct,
    CASE 
        WHEN pacing_pct < (expected_pacing_pct * 0.8) THEN 'Under-pacing'
        WHEN pacing_pct > (expected_pacing_pct * 1.2) THEN 'Over-pacing'
        ELSE 'On Track'
    END as pacing_status
FROM pacing_data
ORDER BY pacing_status
"""
display(spark.sql(pacing_sql))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Ticket SLA Metrics

# COMMAND ----------
# MAGIC %md
# MAGIC ### Urgent Tickets by Stage

# COMMAND ----------
display(spark.sql("""
    SELECT urgency, stage, COUNT(*) as ticket_count
    FROM tickets
    GROUP BY urgency, stage
    ORDER BY urgency, stage
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ### SLA Breach Rate by Assignee

# COMMAND ----------
breach_rate_sql = """
SELECT 
    assignee,
    COUNT(*) as total_open_tickets,
    SUM(CASE WHEN CAST(due_date AS TIMESTAMP) < current_timestamp() THEN 1 ELSE 0 END) as overdue_tickets,
    ROUND(SUM(CASE WHEN CAST(due_date AS TIMESTAMP) < current_timestamp() THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as breach_rate
FROM tickets
WHERE stage NOT IN ('Completed', 'Live') AND assignee IS NOT NULL AND assignee != ''
GROUP BY assignee
ORDER BY breach_rate DESC
"""
display(spark.sql(breach_rate_sql))

# COMMAND ----------
# MAGIC %md
# MAGIC ### EVE-Eligible Health Status

# COMMAND ----------
display(spark.sql("""
    SELECT stage, count(*) as count
    FROM tickets
    WHERE eve_eligible = true
    GROUP BY stage
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Workload Request Types

# COMMAND ----------
display(spark.sql("""
    SELECT request_type, COUNT(*) as volume
    FROM tickets
    GROUP BY request_type
    ORDER BY volume DESC
"""))
