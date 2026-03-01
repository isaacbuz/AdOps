# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 7: Workflow Orchestration
# MAGIC **Disney Ad Ops Lab — Scheduling & Monitoring Pipelines**
# MAGIC
# MAGIC ## What is a Databricks Workflow?
# MAGIC A **Workflow** (formerly "Job") is a scheduled DAG of tasks.
# MAGIC Each task runs a notebook, and tasks can depend on each other.
# MAGIC
# MAGIC ### Our Pipeline DAG:
# MAGIC ```
# MAGIC ┌─────────────────┐
# MAGIC │ Bronze Ingestion│ ← Runs first: loads raw CSVs into Delta tables
# MAGIC └────────┬────────┘
# MAGIC          │
# MAGIC          ▼
# MAGIC ┌─────────────────┐
# MAGIC │ Silver Transforms│ ← Depends on Bronze: cleans & deduplicates
# MAGIC └────────┬────────┘
# MAGIC          │
# MAGIC          ▼
# MAGIC ┌─────────────────┐
# MAGIC │ Gold Aggregation│ ← Depends on Silver: builds KPIs
# MAGIC └────────┬────────┘
# MAGIC          │
# MAGIC     ┌────┴────┐
# MAGIC     ▼         ▼
# MAGIC ┌────────┐ ┌────────────┐
# MAGIC │ Quality│ │ Alert Check│
# MAGIC │ Report │ │ (Slack/    │
# MAGIC └────────┘ │  Teams)    │
# MAGIC            └────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Scheduling Options:
# MAGIC | Schedule | Use Case | Cron Expression |
# MAGIC |----------|----------|-----------------|
# MAGIC | Daily 6am | Full pipeline refresh | `0 0 6 * * ?` |
# MAGIC | Hourly | Delivery pacing updates | `0 0 * * * ?` |
# MAGIC | Every 15min | Near real-time alerts | `0 0/15 * * * ?` |
# MAGIC
# MAGIC ### How to create this Workflow:
# MAGIC 1. Go to **Workflows** in the left sidebar
# MAGIC 2. Click **Create Job**
# MAGIC 3. Add tasks pointing to notebooks 03, 04, 05
# MAGIC 4. Set dependencies between tasks
# MAGIC 5. Configure schedule and alerts

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Pipeline Health Check
# MAGIC
# MAGIC This cell runs as the quality gate after Gold tables are built.
# MAGIC If any check fails, the pipeline alerts the team.

# COMMAND ----------

from datetime import datetime

print(f"🔄 Pipeline Health Check — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Check all layer tables exist and have data
checks = {
    "Bronze": ["adops_bronze.campaigns", "adops_bronze.delivery", "adops_bronze.tickets"],
    "Silver": ["adops_silver.campaigns", "adops_silver.delivery", "adops_silver.tickets"],
    "Gold": ["adops_gold.campaign_performance", "adops_gold.daily_ops_summary",
             "adops_gold.platform_scorecard", "adops_gold.ops_efficiency"],
}

all_healthy = True

for layer, tables in checks.items():
    print(f"\n  📦 {layer} Layer:")
    for table in tables:
        try:
            count = spark.table(table).count()
            status = "✅" if count > 0 else "⚠️ EMPTY"
            print(f"    {status} {table}: {count:,} rows")
            if count == 0:
                all_healthy = False
        except Exception as e:
            print(f"    ❌ {table}: MISSING — {e}")
            all_healthy = False

print("\n" + "=" * 70)
if all_healthy:
    print("✅ Pipeline Health: ALL HEALTHY")
else:
    print("🚨 Pipeline Health: ISSUES DETECTED — Review above.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Data Freshness Check
# MAGIC
# MAGIC Ensures data has been refreshed recently. Stale data = stale decisions.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check when each Silver table was last processed
# MAGIC SELECT 'campaigns' as table_name,
# MAGIC        MAX(_silver_processed_at) as last_processed,
# MAGIC        TIMESTAMPDIFF(HOUR, MAX(_silver_processed_at), current_timestamp()) as hours_since_refresh
# MAGIC FROM adops_silver.campaigns
# MAGIC UNION ALL
# MAGIC SELECT 'delivery',
# MAGIC        MAX(_silver_processed_at),
# MAGIC        TIMESTAMPDIFF(HOUR, MAX(_silver_processed_at), current_timestamp())
# MAGIC FROM adops_silver.delivery
# MAGIC UNION ALL
# MAGIC SELECT 'tickets',
# MAGIC        MAX(_silver_processed_at),
# MAGIC        TIMESTAMPDIFF(HOUR, MAX(_silver_processed_at), current_timestamp())
# MAGIC FROM adops_silver.tickets;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. SLA & Alert Generation
# MAGIC
# MAGIC Generate alerts for campaigns that need immediate attention.
# MAGIC In production, these would trigger Slack/Teams notifications via webhooks.

# COMMAND ----------

# Alert: Under-pacing campaigns
try:
    alerts_df = spark.sql("""
        SELECT campaign_name, brand, platform, 
               pacing_status, alert_priority,
               delivery_pct, time_elapsed_pct, budget_utilization_pct
        FROM adops_gold.campaign_performance
        WHERE alert_priority != 'Normal'
          AND status = 'Active'
        ORDER BY 
            CASE alert_priority
                WHEN 'Critical: No Delivery' THEN 1
                WHEN 'Critical: Severe Under-Pacing' THEN 2
                WHEN 'High: Budget Draining Fast' THEN 3
                WHEN 'High: Under-Pacing' THEN 4
                ELSE 5
            END
    """)
    
    alert_count = alerts_df.count()
    if alert_count > 0:
        print(f"🚨 {alert_count} campaigns need attention:\n")
        alerts_df.show(truncate=False)
    else:
        print("✅ All campaigns healthy — no alerts.")
except Exception as e:
    print(f"⚠️ Could not check alerts: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. SLA Breach Report

# COMMAND ----------

try:
    breach_df = spark.sql("""
        SELECT assignee, COUNT(*) as breached_count,
               COLLECT_LIST(ticket_id) as breached_tickets
        FROM adops_silver.tickets
        WHERE is_breached = true
        GROUP BY assignee
        ORDER BY breached_count DESC
    """)
    
    breach_count = breach_df.count()
    if breach_count > 0:
        print(f"⚠️ {breach_count} assignees have SLA breaches:\n")
        breach_df.show(truncate=False)
    else:
        print("✅ No SLA breaches detected.")
except Exception as e:
    print(f"⚠️ Could not check SLA breaches: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Row Count Trends
# MAGIC
# MAGIC Track that data volumes are consistent over time.
# MAGIC A sudden drop in delivery rows could mean a broken API integration.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delivery volume by day — should be consistent
# MAGIC SELECT delivery_date, COUNT(*) as row_count, SUM(impressions) as total_impressions
# MAGIC FROM adops_silver.delivery
# MAGIC GROUP BY delivery_date
# MAGIC ORDER BY delivery_date;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Workflow Job Definition (API-based)
# MAGIC
# MAGIC This cell shows how to programmatically create the Workflow Job
# MAGIC using the Databricks REST API. In practice, you'd run this from
# MAGIC your orchestrator (EVE) or CI/CD pipeline.

# COMMAND ----------

# This is the JSON that defines the Workflow Job
# You'd POST this to /api/2.1/jobs/create
import json

workflow_definition = {
    "name": "Disney AdOps - Daily Pipeline",
    "tasks": [
        {
            "task_key": "bronze_ingestion",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/adops_lab/03_medallion_bronze"
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "silver_transforms",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/adops_lab/04_medallion_silver"
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "gold_aggregation",
            "depends_on": [{"task_key": "silver_transforms"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/adops_lab/05_medallion_gold"
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "health_check",
            "depends_on": [{"task_key": "gold_aggregation"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/adops_lab/07_workflow_orchestration",
                "base_parameters": {"run_mode": "quality_only"}
            },
            "timeout_seconds": 600
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",
        "timezone_id": "America/New_York",
        "pause_status": "UNPAUSED"
    },
    "email_notifications": {
        "on_failure": ["adops-alerts@disney.com"]
    },
    "max_concurrent_runs": 1,
    "tags": {
        "team": "ad-ops",
        "environment": "production",
        "pipeline": "daily-refresh"
    }
}

print("📋 Workflow Definition (POST to /api/2.1/jobs/create):")
print(json.dumps(workflow_definition, indent=2))
