# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4: Medallion Architecture — Silver Layer
# MAGIC **Disney Ad Ops Lab — Cleaned & Conformed Data**
# MAGIC
# MAGIC ## What happens in Silver?
# MAGIC Silver is the **quality control** layer. Raw Bronze data gets:
# MAGIC - **Deduplicated** — Same row loaded twice? Keep only the latest.
# MAGIC - **Cleaned** — Fix types, trim whitespace, standardize values.
# MAGIC - **Validated** — Apply business rules. Bad data goes to quarantine.
# MAGIC - **Enriched** — Compute derived fields (CPM, CPC, pacing %).
# MAGIC
# MAGIC ### Silver = "Source of Truth"
# MAGIC When someone asks "how many active campaigns do we have?", the answer
# MAGIC comes from Silver — not from the raw CSV or from a dashboard aggregate.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Silver Campaigns
# MAGIC
# MAGIC ### Transforms Applied:
# MAGIC - Deduplicate by `campaign_id` (keep newest ingestion)
# MAGIC - Standardize platform names (meta → Meta, dcm → CM360)
# MAGIC - Validate budget, dates, status
# MAGIC - Compute `flight_duration_days` and `target_cpm`
# MAGIC - Flag invalid budgets instead of dropping them

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE adops_silver.campaigns AS
# MAGIC
# MAGIC WITH deduplicated AS (
# MAGIC     -- DEDUP: If the same campaign was loaded multiple times,
# MAGIC     -- keep only the most recently ingested version
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY campaign_id
# MAGIC             ORDER BY _ingested_at DESC
# MAGIC         ) as _row_num
# MAGIC     FROM adops_bronze.campaigns
# MAGIC )
# MAGIC SELECT
# MAGIC     campaign_id,
# MAGIC     title_id,
# MAGIC     TRIM(title_name) as title_name,
# MAGIC     TRIM(brand) as brand,
# MAGIC     UPPER(TRIM(brand_code)) as brand_code,
# MAGIC     TRIM(product) as product,
# MAGIC     TRIM(campaign_name) as campaign_name,
# MAGIC     TRIM(campaign_objective) as campaign_objective,
# MAGIC     UPPER(TRIM(targeting_geo)) as targeting_geo,
# MAGIC     TRIM(country) as country,
# MAGIC     UPPER(TRIM(language)) as language,
# MAGIC     TRIM(geo_cluster) as geo_cluster,
# MAGIC     TRIM(region) as region,
# MAGIC     TRIM(channel) as channel,
# MAGIC     TRIM(channel_mapped) as channel_mapped,
# MAGIC
# MAGIC     -- PLATFORM STANDARDIZATION
# MAGIC     -- Different source systems use different names for the same platform
# MAGIC     CASE
# MAGIC         WHEN LOWER(TRIM(platform)) IN ('meta', 'facebook', 'instagram') THEN 'Meta'
# MAGIC         WHEN LOWER(TRIM(platform)) IN ('tiktok') THEN 'TikTok'
# MAGIC         WHEN LOWER(TRIM(platform)) IN ('cm360', 'dcm', 'campaign manager') THEN 'CM360'
# MAGIC         WHEN LOWER(TRIM(platform)) IN ('dv360', 'display video 360') THEN 'DV360'
# MAGIC         WHEN LOWER(TRIM(platform)) IN ('amazon dsp', 'amazon') THEN 'Amazon DSP'
# MAGIC         WHEN LOWER(TRIM(platform)) IN ('snap', 'snapchat') THEN 'Snapchat'
# MAGIC         ELSE TRIM(platform)
# MAGIC     END as platform,
# MAGIC
# MAGIC     -- BUDGET VALIDATION: Don't drop bad data, flag it
# MAGIC     CASE WHEN budget_usd > 0 THEN budget_usd ELSE NULL END as budget_usd,
# MAGIC     CASE WHEN budget_usd <= 0 THEN true ELSE false END as _budget_flagged,
# MAGIC
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC
# MAGIC     -- STATUS VALIDATION
# MAGIC     CASE
# MAGIC         WHEN TRIM(status) IN ('Active', 'Paused', 'Completed', 'Cancelled', 'Draft')
# MAGIC         THEN TRIM(status)
# MAGIC         ELSE 'Unknown'
# MAGIC     END as status,
# MAGIC
# MAGIC     CASE WHEN impressions_goal > 0 THEN impressions_goal ELSE NULL END as impressions_goal,
# MAGIC     flight_priority,
# MAGIC     TRIM(audience_tactic) as audience_tactic,
# MAGIC     TRIM(audience_strategy) as audience_strategy,
# MAGIC     TRIM(audience_detailed) as audience_detailed,
# MAGIC
# MAGIC     -- DERIVED FIELDS (computed in Silver, never in Bronze)
# MAGIC     datediff(end_date, start_date) as flight_duration_days,
# MAGIC
# MAGIC     -- Target CPM = Budget / (ImpressionGoal / 1000)
# MAGIC     CASE
# MAGIC         WHEN impressions_goal > 0 AND budget_usd > 0
# MAGIC         THEN ROUND(budget_usd / (impressions_goal / 1000.0), 4)
# MAGIC         ELSE NULL
# MAGIC     END as target_cpm,
# MAGIC
# MAGIC     -- Metadata propagation (carry lineage from Bronze)
# MAGIC     _ingested_at,
# MAGIC     _source_file,
# MAGIC     current_timestamp() as _silver_processed_at
# MAGIC
# MAGIC FROM deduplicated
# MAGIC WHERE _row_num = 1              -- Keep only latest version
# MAGIC   AND campaign_id IS NOT NULL;  -- Drop rows with no key

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Silver Delivery
# MAGIC
# MAGIC ### Transforms Applied:
# MAGIC - Deduplicate by `delivery_id`
# MAGIC - Clamp negative values to 0 with flags
# MAGIC - **Recompute CTR** from raw values (never trust pre-computed metrics)
# MAGIC - Compute CPM, CPC, VAST error rate
# MAGIC - Flag suspicious patterns (clicks > impressions, zero delivery with spend)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE adops_silver.delivery AS
# MAGIC
# MAGIC WITH deduplicated AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY delivery_id
# MAGIC             ORDER BY _ingested_at DESC
# MAGIC         ) as _row_num
# MAGIC     FROM adops_bronze.delivery
# MAGIC )
# MAGIC SELECT
# MAGIC     delivery_id,
# MAGIC     campaign_id,
# MAGIC     date as delivery_date,
# MAGIC
# MAGIC     -- Clamp negative values (data corruption safety net)
# MAGIC     GREATEST(impressions, 0) as impressions,
# MAGIC     GREATEST(clicks, 0) as clicks,
# MAGIC     GREATEST(spend_usd, 0) as spend_usd,
# MAGIC     GREATEST(vast_errors, 0) as vast_errors,
# MAGIC
# MAGIC     -- RECOMPUTE CTR: Never trust pre-computed values from source systems
# MAGIC     -- Source CTR might be stale or calculated differently by each platform
# MAGIC     CASE
# MAGIC         WHEN impressions > 0 THEN ROUND(CAST(clicks AS DOUBLE) / impressions, 6)
# MAGIC         ELSE 0.0
# MAGIC     END as ctr,
# MAGIC
# MAGIC     -- Viewability: Clamp to valid range [0, 1]
# MAGIC     CASE
# MAGIC         WHEN viewability_rate BETWEEN 0 AND 1 THEN viewability_rate
# MAGIC         WHEN viewability_rate > 1 THEN 1.0
# MAGIC         ELSE 0.0
# MAGIC     END as viewability_rate,
# MAGIC
# MAGIC     -- COMPUTED METRICS that ad ops teams query daily
# MAGIC     -- CPM = Cost Per Mille (cost per 1000 impressions)
# MAGIC     CASE
# MAGIC         WHEN impressions > 0 THEN ROUND(spend_usd / (impressions / 1000.0), 4)
# MAGIC         ELSE NULL
# MAGIC     END as cpm,
# MAGIC
# MAGIC     -- CPC = Cost Per Click
# MAGIC     CASE
# MAGIC         WHEN clicks > 0 THEN ROUND(spend_usd / clicks, 4)
# MAGIC         ELSE NULL
# MAGIC     END as cpc,
# MAGIC
# MAGIC     -- VAST Error Rate (video ad serving template errors)
# MAGIC     CASE
# MAGIC         WHEN impressions + vast_errors > 0
# MAGIC         THEN ROUND(CAST(vast_errors AS DOUBLE) / (impressions + vast_errors), 6)
# MAGIC         ELSE 0.0
# MAGIC     END as vast_error_rate,
# MAGIC
# MAGIC     -- DATA QUALITY FLAGS
# MAGIC     CASE WHEN clicks > impressions THEN true ELSE false END as _suspicious_ctr,
# MAGIC     CASE WHEN impressions = 0 AND spend_usd > 0 THEN true ELSE false END as _zero_delivery_spend,
# MAGIC
# MAGIC     _ingested_at,
# MAGIC     _source_file,
# MAGIC     current_timestamp() as _silver_processed_at
# MAGIC
# MAGIC FROM deduplicated
# MAGIC WHERE _row_num = 1
# MAGIC   AND delivery_id IS NOT NULL
# MAGIC   AND campaign_id IS NOT NULL;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Silver Tickets
# MAGIC
# MAGIC ### Transforms Applied:
# MAGIC - Deduplicate by `ticket_id`
# MAGIC - Standardize platform + urgency values
# MAGIC - Parse `due_date` string → timestamp
# MAGIC - **Compute SLA breach status** and hours until due

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE adops_silver.tickets AS
# MAGIC
# MAGIC WITH deduplicated AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY ticket_id
# MAGIC             ORDER BY _ingested_at DESC
# MAGIC         ) as _row_num
# MAGIC     FROM adops_bronze.tickets
# MAGIC )
# MAGIC SELECT
# MAGIC     ticket_id,
# MAGIC     campaign_id,
# MAGIC     TRIM(title) as title,
# MAGIC     TRIM(request_type) as request_type,
# MAGIC     TRIM(routed_to_role) as routed_to_role,
# MAGIC     eve_eligible,
# MAGIC
# MAGIC     -- Standardize urgency
# MAGIC     CASE
# MAGIC         WHEN TRIM(urgency) IN ('Low', 'Medium', 'High', 'Critical') THEN TRIM(urgency)
# MAGIC         ELSE 'Medium'
# MAGIC     END as urgency,
# MAGIC
# MAGIC     TRIM(stage) as stage,
# MAGIC
# MAGIC     -- Platform standardization (same rules as campaigns)
# MAGIC     CASE
# MAGIC         WHEN LOWER(TRIM(platform)) IN ('meta', 'facebook') THEN 'Meta'
# MAGIC         WHEN LOWER(TRIM(platform)) IN ('tiktok') THEN 'TikTok'
# MAGIC         WHEN LOWER(TRIM(platform)) IN ('cm360', 'dcm') THEN 'CM360'
# MAGIC         ELSE TRIM(platform)
# MAGIC     END as platform,
# MAGIC
# MAGIC     UPPER(TRIM(targeting_geo)) as targeting_geo,
# MAGIC     TRIM(brand) as brand,
# MAGIC     TRIM(requested_by) as requested_by,
# MAGIC     created_date,
# MAGIC     TRY_CAST(due_date AS TIMESTAMP) as due_date,
# MAGIC     TRIM(assignee) as assignee,
# MAGIC     TRIM(assignee_role) as assignee_role,
# MAGIC     GREATEST(sla_hours, 1) as sla_hours,
# MAGIC     notes,
# MAGIC
# MAGIC     -- SLA BREACH DETECTION
# MAGIC     CASE
# MAGIC         WHEN TRY_CAST(due_date AS TIMESTAMP) < current_timestamp()
# MAGIC              AND TRIM(stage) NOT IN ('Completed', 'Live')
# MAGIC         THEN true
# MAGIC         ELSE false
# MAGIC     END as is_breached,
# MAGIC
# MAGIC     -- Hours remaining until SLA expires
# MAGIC     CASE
# MAGIC         WHEN TRY_CAST(due_date AS TIMESTAMP) IS NOT NULL
# MAGIC         THEN ROUND(
# MAGIC             (unix_timestamp(TRY_CAST(due_date AS TIMESTAMP)) - unix_timestamp(current_timestamp())) / 3600.0,
# MAGIC             1
# MAGIC         )
# MAGIC         ELSE NULL
# MAGIC     END as hours_until_due,
# MAGIC
# MAGIC     _ingested_at,
# MAGIC     current_timestamp() as _silver_processed_at
# MAGIC
# MAGIC FROM deduplicated
# MAGIC WHERE _row_num = 1
# MAGIC   AND ticket_id IS NOT NULL;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Silver QA Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE adops_silver.qa_checks AS
# MAGIC
# MAGIC WITH deduplicated AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY qa_id
# MAGIC             ORDER BY _ingested_at DESC
# MAGIC         ) as _row_num
# MAGIC     FROM adops_bronze.qa_checks
# MAGIC )
# MAGIC SELECT
# MAGIC     qa_id,
# MAGIC     ticket_id,
# MAGIC     TRIM(check_name) as check_name,
# MAGIC     TRIM(check_details) as check_details,
# MAGIC     TRIM(result) as result,
# MAGIC     TRIM(checked_by) as checked_by,
# MAGIC     TRY_CAST(checked_at AS TIMESTAMP) as checked_at,
# MAGIC     _ingested_at,
# MAGIC     current_timestamp() as _silver_processed_at
# MAGIC FROM deduplicated
# MAGIC WHERE _row_num = 1 AND qa_id IS NOT NULL;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Data Quality Validation
# MAGIC
# MAGIC Run quality checks against Silver tables to ensure they meet business rules.
# MAGIC This is the "gate" before data flows to Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality check: How many campaigns have invalid budgets?
# MAGIC SELECT
# MAGIC     'campaigns' as table_name,
# MAGIC     COUNT(*) as total_rows,
# MAGIC     SUM(CASE WHEN budget_usd IS NOT NULL THEN 1 ELSE 0 END) as valid_budget,
# MAGIC     SUM(CASE WHEN _budget_flagged THEN 1 ELSE 0 END) as flagged_budget,
# MAGIC     SUM(CASE WHEN status = 'Unknown' THEN 1 ELSE 0 END) as unknown_status,
# MAGIC     SUM(CASE WHEN impressions_goal IS NULL THEN 1 ELSE 0 END) as missing_goal,
# MAGIC     SUM(CASE WHEN flight_duration_days <= 0 THEN 1 ELSE 0 END) as invalid_flight_dates
# MAGIC FROM adops_silver.campaigns;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality check: Delivery anomalies
# MAGIC SELECT
# MAGIC     'delivery' as table_name,
# MAGIC     COUNT(*) as total_rows,
# MAGIC     SUM(CASE WHEN _suspicious_ctr THEN 1 ELSE 0 END) as suspicious_ctr_rows,
# MAGIC     SUM(CASE WHEN _zero_delivery_spend THEN 1 ELSE 0 END) as zero_delivery_with_spend,
# MAGIC     SUM(CASE WHEN cpm > 100 THEN 1 ELSE 0 END) as high_cpm_rows,
# MAGIC     ROUND(AVG(ctr) * 100, 4) as avg_ctr_pct,
# MAGIC     ROUND(AVG(viewability_rate) * 100, 2) as avg_viewability_pct
# MAGIC FROM adops_silver.delivery;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality check: SLA health
# MAGIC SELECT
# MAGIC     COUNT(*) as total_tickets,
# MAGIC     SUM(CASE WHEN is_breached THEN 1 ELSE 0 END) as breached,
# MAGIC     ROUND(SUM(CASE WHEN is_breached THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as breach_rate_pct,
# MAGIC     SUM(CASE WHEN hours_until_due < 4 AND NOT is_breached THEN 1 ELSE 0 END) as at_risk
# MAGIC FROM adops_silver.tickets
# MAGIC WHERE stage NOT IN ('Completed', 'Live');

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Verify Silver Tables

# COMMAND ----------

for table in ["campaigns", "delivery", "tickets", "qa_checks"]:
    try:
        count = spark.table(f"adops_silver.{table}").count()
        print(f"  📊 adops_silver.{table}: {count:,} rows")
    except Exception as e:
        print(f"  ❌ adops_silver.{table}: {e}")
