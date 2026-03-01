# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 5: Medallion Architecture — Gold Layer
# MAGIC **Disney Ad Ops Lab — Business-Ready KPIs & Dashboards**
# MAGIC
# MAGIC ## What is the Gold Layer?
# MAGIC Gold tables are the **finished product**. They answer specific business
# MAGIC questions without requiring stakeholders to write SQL or understand joins.
# MAGIC
# MAGIC Every Gold table is designed for a specific audience:
# MAGIC - **Campaign Performance** → Campaign managers & VPs
# MAGIC - **Daily Ops Summary** → Morning standup reports
# MAGIC - **Platform Scorecard** → Weekly platform review meetings
# MAGIC - **Ops Efficiency** → Automation/EVE team metrics
# MAGIC
# MAGIC ### Gold tables = Pre-computed answers
# MAGIC If a stakeholder has to write a JOIN to get their answer, it belongs in Gold.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Campaign Performance Summary
# MAGIC
# MAGIC The **#1 table** for ad ops managers. For every campaign:
# MAGIC - How are we pacing? (Under? Over? On Track?)
# MAGIC - What's our spend efficiency? (CPM, CPC)
# MAGIC - Does anything need immediate attention? (Alerts)
# MAGIC
# MAGIC ### PACING LOGIC:
# MAGIC ```
# MAGIC pacing_ratio = delivery_pct / time_elapsed_pct
# MAGIC
# MAGIC If pacing_ratio < 0.8  → Under-Pacing (won't hit goal)
# MAGIC If pacing_ratio > 1.2  → Over-Pacing (will exhaust budget early)
# MAGIC Otherwise              → On Track ✅
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE adops_gold.campaign_performance AS
# MAGIC
# MAGIC WITH delivery_agg AS (
# MAGIC     SELECT
# MAGIC         campaign_id,
# MAGIC         COUNT(DISTINCT delivery_date) as days_with_delivery,
# MAGIC         SUM(impressions) as total_impressions,
# MAGIC         SUM(clicks) as total_clicks,
# MAGIC         SUM(spend_usd) as total_spend,
# MAGIC         SUM(vast_errors) as total_vast_errors,
# MAGIC         ROUND(AVG(viewability_rate), 4) as avg_viewability,
# MAGIC         ROUND(AVG(cpm), 2) as avg_cpm,
# MAGIC         ROUND(AVG(cpc), 2) as avg_cpc,
# MAGIC         MAX(delivery_date) as last_delivery_date,
# MAGIC
# MAGIC         -- Trailing 7-day metrics
# MAGIC         SUM(CASE WHEN delivery_date >= date_sub(current_date(), 7)
# MAGIC             THEN impressions ELSE 0 END) as impressions_last_7d,
# MAGIC         SUM(CASE WHEN delivery_date >= date_sub(current_date(), 7)
# MAGIC             THEN spend_usd ELSE 0 END) as spend_last_7d,
# MAGIC
# MAGIC         -- Zero delivery tracking
# MAGIC         SUM(CASE WHEN impressions = 0 THEN 1 ELSE 0 END) as zero_delivery_days
# MAGIC
# MAGIC     FROM adops_silver.delivery
# MAGIC     GROUP BY campaign_id
# MAGIC ),
# MAGIC pacing AS (
# MAGIC     SELECT
# MAGIC         c.*,
# MAGIC         COALESCE(d.total_impressions, 0) as total_impressions,
# MAGIC         COALESCE(d.total_clicks, 0) as total_clicks,
# MAGIC         COALESCE(d.total_spend, 0) as total_spend,
# MAGIC         COALESCE(d.total_vast_errors, 0) as total_vast_errors,
# MAGIC         d.avg_viewability,
# MAGIC         d.avg_cpm,
# MAGIC         d.avg_cpc,
# MAGIC         d.last_delivery_date,
# MAGIC         d.days_with_delivery,
# MAGIC         COALESCE(d.zero_delivery_days, 0) as zero_delivery_days,
# MAGIC         COALESCE(d.impressions_last_7d, 0) as impressions_last_7d,
# MAGIC         COALESCE(d.spend_last_7d, 0) as spend_last_7d,
# MAGIC
# MAGIC         -- Pacing %: How much of the impression goal has been delivered
# MAGIC         CASE WHEN c.impressions_goal > 0
# MAGIC             THEN ROUND(COALESCE(d.total_impressions, 0) * 100.0 / c.impressions_goal, 2)
# MAGIC             ELSE NULL
# MAGIC         END as delivery_pct,
# MAGIC
# MAGIC         -- Time elapsed %: How far through the flight are we
# MAGIC         CASE WHEN c.flight_duration_days > 0
# MAGIC             THEN ROUND(
# MAGIC                 GREATEST(datediff(LEAST(current_date(), c.end_date), c.start_date), 0)
# MAGIC                 * 100.0 / c.flight_duration_days, 2
# MAGIC             )
# MAGIC             ELSE NULL
# MAGIC         END as time_elapsed_pct,
# MAGIC
# MAGIC         -- Budget utilization %
# MAGIC         CASE WHEN c.budget_usd > 0
# MAGIC             THEN ROUND(COALESCE(d.total_spend, 0) * 100.0 / c.budget_usd, 2)
# MAGIC             ELSE NULL
# MAGIC         END as budget_utilization_pct
# MAGIC
# MAGIC     FROM adops_silver.campaigns c
# MAGIC     LEFT JOIN delivery_agg d ON c.campaign_id = d.campaign_id
# MAGIC )
# MAGIC SELECT
# MAGIC     *,
# MAGIC
# MAGIC     -- PACING STATUS
# MAGIC     CASE
# MAGIC         WHEN status != 'Active' THEN 'N/A'
# MAGIC         WHEN delivery_pct IS NULL OR time_elapsed_pct IS NULL THEN 'Unknown'
# MAGIC         WHEN time_elapsed_pct = 0 THEN 'Not Started'
# MAGIC         WHEN delivery_pct / time_elapsed_pct < 0.8 THEN 'Under-Pacing'
# MAGIC         WHEN delivery_pct / time_elapsed_pct > 1.2 THEN 'Over-Pacing'
# MAGIC         ELSE 'On Track'
# MAGIC     END as pacing_status,
# MAGIC
# MAGIC     -- PACING RATIO (for sorting)
# MAGIC     CASE WHEN time_elapsed_pct > 0
# MAGIC         THEN ROUND(delivery_pct / time_elapsed_pct, 3)
# MAGIC         ELSE NULL
# MAGIC     END as pacing_ratio,
# MAGIC
# MAGIC     -- ALERT PRIORITY
# MAGIC     CASE
# MAGIC         WHEN status = 'Active' AND zero_delivery_days >= 3
# MAGIC             THEN 'Critical: No Delivery'
# MAGIC         WHEN status = 'Active' AND delivery_pct IS NOT NULL AND time_elapsed_pct > 0
# MAGIC              AND delivery_pct / time_elapsed_pct < 0.5
# MAGIC             THEN 'Critical: Severe Under-Pacing'
# MAGIC         WHEN status = 'Active' AND budget_utilization_pct > 90 AND delivery_pct < 80
# MAGIC             THEN 'High: Budget Draining Fast'
# MAGIC         WHEN status = 'Active' AND delivery_pct IS NOT NULL AND time_elapsed_pct > 0
# MAGIC              AND delivery_pct / time_elapsed_pct < 0.8
# MAGIC             THEN 'High: Under-Pacing'
# MAGIC         ELSE 'Normal'
# MAGIC     END as alert_priority,
# MAGIC
# MAGIC     -- FORECAST: If current daily rate continues, what's end-of-flight total?
# MAGIC     CASE WHEN days_with_delivery > 0 AND flight_duration_days > 0
# MAGIC         THEN ROUND(
# MAGIC             (total_impressions * 1.0 / GREATEST(days_with_delivery, 1)) * flight_duration_days,
# MAGIC             0
# MAGIC         )
# MAGIC         ELSE NULL
# MAGIC     END as forecasted_total_impressions,
# MAGIC
# MAGIC     current_timestamp() as _gold_refreshed_at
# MAGIC
# MAGIC FROM pacing;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Daily Operations Summary
# MAGIC
# MAGIC One row per day — perfect for time-series charts and trend analysis.
# MAGIC Includes **week-over-week comparisons** using window functions.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE adops_gold.daily_ops_summary AS
# MAGIC
# MAGIC SELECT
# MAGIC     d.delivery_date,
# MAGIC     COUNT(DISTINCT d.campaign_id) as active_campaigns,
# MAGIC     SUM(d.impressions) as total_impressions,
# MAGIC     SUM(d.clicks) as total_clicks,
# MAGIC     SUM(d.spend_usd) as total_spend,
# MAGIC
# MAGIC     ROUND(AVG(d.ctr) * 100, 4) as avg_ctr_pct,
# MAGIC     ROUND(AVG(d.cpm), 2) as avg_cpm,
# MAGIC     ROUND(AVG(d.viewability_rate) * 100, 2) as avg_viewability_pct,
# MAGIC
# MAGIC     SUM(d.vast_errors) as total_vast_errors,
# MAGIC     SUM(CASE WHEN d.impressions = 0 THEN 1 ELSE 0 END) as zero_delivery_count,
# MAGIC
# MAGIC     -- Platform breakdown
# MAGIC     COUNT(DISTINCT CASE WHEN c.platform = 'Meta' THEN d.campaign_id END) as meta_campaigns,
# MAGIC     COUNT(DISTINCT CASE WHEN c.platform = 'CM360' THEN d.campaign_id END) as cm360_campaigns,
# MAGIC     COUNT(DISTINCT CASE WHEN c.platform = 'TikTok' THEN d.campaign_id END) as tiktok_campaigns,
# MAGIC     COUNT(DISTINCT CASE WHEN c.platform = 'Amazon DSP' THEN d.campaign_id END) as amazon_campaigns,
# MAGIC
# MAGIC     -- WoW trend using LAG window function
# MAGIC     LAG(SUM(d.impressions), 7) OVER (ORDER BY d.delivery_date) as impressions_7d_ago,
# MAGIC     ROUND(
# MAGIC         (SUM(d.impressions) - LAG(SUM(d.impressions), 7) OVER (ORDER BY d.delivery_date))
# MAGIC         * 100.0 / NULLIF(LAG(SUM(d.impressions), 7) OVER (ORDER BY d.delivery_date), 0),
# MAGIC         2
# MAGIC     ) as impressions_wow_change_pct,
# MAGIC
# MAGIC     current_timestamp() as _gold_refreshed_at
# MAGIC
# MAGIC FROM adops_silver.delivery d
# MAGIC LEFT JOIN adops_silver.campaigns c ON d.campaign_id = c.campaign_id
# MAGIC GROUP BY d.delivery_date
# MAGIC ORDER BY d.delivery_date;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Platform Scorecard
# MAGIC
# MAGIC Compare performance across Meta, CM360, TikTok, Amazon DSP, etc.
# MAGIC This gets presented in weekly platform review meetings.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE adops_gold.platform_scorecard AS
# MAGIC
# MAGIC SELECT
# MAGIC     c.platform,
# MAGIC     c.region,
# MAGIC     COUNT(DISTINCT c.campaign_id) as total_campaigns,
# MAGIC     COUNT(DISTINCT CASE WHEN c.status = 'Active' THEN c.campaign_id END) as active_campaigns,
# MAGIC     ROUND(SUM(c.budget_usd), 2) as total_budget,
# MAGIC     COALESCE(SUM(d.impressions), 0) as total_impressions,
# MAGIC     COALESCE(SUM(d.clicks), 0) as total_clicks,
# MAGIC     COALESCE(SUM(d.spend_usd), 0) as total_spend,
# MAGIC
# MAGIC     -- Efficiency
# MAGIC     ROUND(COALESCE(SUM(d.spend_usd), 0) * 100.0 / NULLIF(SUM(c.budget_usd), 0), 2) as budget_util_pct,
# MAGIC     ROUND(COALESCE(SUM(d.spend_usd), 0) / NULLIF(COALESCE(SUM(d.impressions), 0) / 1000.0, 0), 2) as blended_cpm,
# MAGIC     ROUND(COALESCE(SUM(d.clicks), 0) * 100.0 / NULLIF(COALESCE(SUM(d.impressions), 0), 0), 4) as blended_ctr_pct,
# MAGIC     ROUND(AVG(d.viewability_rate) * 100, 2) as avg_viewability_pct,
# MAGIC
# MAGIC     current_timestamp() as _gold_refreshed_at
# MAGIC
# MAGIC FROM adops_silver.campaigns c
# MAGIC LEFT JOIN adops_silver.delivery d ON c.campaign_id = d.campaign_id
# MAGIC GROUP BY c.platform, c.region
# MAGIC ORDER BY total_budget DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Operational Efficiency — EVE Automation Metrics
# MAGIC
# MAGIC Tracks how much manual trafficking work EVE is automating.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE adops_gold.ops_efficiency AS
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) as total_tickets,
# MAGIC     SUM(CASE WHEN eve_eligible THEN 1 ELSE 0 END) as eve_eligible,
# MAGIC     ROUND(SUM(CASE WHEN eve_eligible THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as automation_rate_pct,
# MAGIC     SUM(CASE WHEN is_breached THEN 1 ELSE 0 END) as breached_tickets,
# MAGIC     ROUND(SUM(CASE WHEN is_breached THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as breach_rate_pct,
# MAGIC     SUM(CASE WHEN urgency = 'Critical' THEN 1 ELSE 0 END) as critical_tickets,
# MAGIC     SUM(CASE WHEN urgency = 'High' THEN 1 ELSE 0 END) as high_tickets,
# MAGIC     SUM(CASE WHEN stage = 'Completed' THEN 1 ELSE 0 END) as completed_tickets,
# MAGIC     SUM(CASE WHEN stage IN ('QA', 'Pending', 'In Progress') THEN 1 ELSE 0 END) as in_progress_tickets,
# MAGIC     current_timestamp() as _gold_refreshed_at
# MAGIC FROM adops_silver.tickets;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Assignee workload for manager dashboards
# MAGIC CREATE OR REPLACE TABLE adops_gold.assignee_workload AS
# MAGIC SELECT
# MAGIC     assignee,
# MAGIC     assignee_role,
# MAGIC     COUNT(*) as total_tickets,
# MAGIC     SUM(CASE WHEN is_breached THEN 1 ELSE 0 END) as breached_tickets,
# MAGIC     SUM(CASE WHEN urgency IN ('Critical', 'High') THEN 1 ELSE 0 END) as urgent_tickets,
# MAGIC     SUM(CASE WHEN stage NOT IN ('Completed', 'Live') THEN 1 ELSE 0 END) as open_tickets,
# MAGIC     ROUND(AVG(hours_until_due), 1) as avg_hours_until_due,
# MAGIC     current_timestamp() as _gold_refreshed_at
# MAGIC FROM adops_silver.tickets
# MAGIC WHERE assignee IS NOT NULL AND assignee != ''
# MAGIC GROUP BY assignee, assignee_role
# MAGIC ORDER BY breached_tickets DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Dashboard Queries
# MAGIC
# MAGIC These are example queries that power the dashboards.
# MAGIC In Databricks, you'd create these as **SQL Dashboard** widgets.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dashboard: Active campaigns needing attention
# MAGIC SELECT
# MAGIC     campaign_name, brand, platform, region,
# MAGIC     pacing_status, alert_priority,
# MAGIC     CONCAT(CAST(delivery_pct AS STRING), '%') as delivery_pct,
# MAGIC     CONCAT(CAST(budget_utilization_pct AS STRING), '%') as budget_used,
# MAGIC     total_impressions, total_spend, avg_cpm
# MAGIC FROM adops_gold.campaign_performance
# MAGIC WHERE status = 'Active'
# MAGIC ORDER BY
# MAGIC     CASE alert_priority
# MAGIC         WHEN 'Critical: No Delivery' THEN 1
# MAGIC         WHEN 'Critical: Severe Under-Pacing' THEN 2
# MAGIC         WHEN 'High: Budget Draining Fast' THEN 3
# MAGIC         WHEN 'High: Under-Pacing' THEN 4
# MAGIC         ELSE 5
# MAGIC     END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dashboard: Platform comparison
# MAGIC SELECT * FROM adops_gold.platform_scorecard
# MAGIC ORDER BY total_budget DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dashboard: Daily trend
# MAGIC SELECT
# MAGIC     delivery_date,
# MAGIC     total_impressions,
# MAGIC     total_spend,
# MAGIC     avg_cpm,
# MAGIC     avg_viewability_pct,
# MAGIC     impressions_wow_change_pct
# MAGIC FROM adops_gold.daily_ops_summary
# MAGIC ORDER BY delivery_date;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Final Verification

# COMMAND ----------

print("📊 Gold Layer Tables:")
for table in ["campaign_performance", "daily_ops_summary", "platform_scorecard", "ops_efficiency", "assignee_workload"]:
    try:
        count = spark.table(f"adops_gold.{table}").count()
        print(f"  ✅ adops_gold.{table}: {count:,} rows")
    except Exception as e:
        print(f"  ❌ adops_gold.{table}: {e}")
