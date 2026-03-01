# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 6: Delta Live Tables (DLT) Pipeline
# MAGIC **Disney Ad Ops Lab — Declarative ETL**
# MAGIC
# MAGIC ## What are Delta Live Tables?
# MAGIC DLT is Databricks' **declarative** ETL framework. Instead of writing
# MAGIC imperative code ("read this, transform that, write here"), you declare:
# MAGIC
# MAGIC - "I want a table called X"
# MAGIC - "It comes from source Y"
# MAGIC - "It must satisfy quality rules Z"
# MAGIC
# MAGIC DLT handles the rest: scheduling, retries, dependency ordering, error handling.
# MAGIC
# MAGIC ### Why DLT matters for Ad Ops:
# MAGIC 1. **Automatic dependency tracking**: Change a Bronze table, Silver auto-refreshes
# MAGIC 2. **Built-in quality checks**: `@dlt.expect()` enforces rules at pipeline time
# MAGIC 3. **Automatic optimization**: DLT auto-compacts, auto-optimizes Delta files
# MAGIC 4. **Lineage tracking**: See exactly where every column came from
# MAGIC
# MAGIC ### How to use this notebook:
# MAGIC 1. Go to **Workflows** → **Delta Live Tables** → **Create Pipeline**
# MAGIC 2. Point it to this notebook
# MAGIC 3. Set the target schema (e.g., `adops_dlt`)
# MAGIC 4. Click **Start**

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit, trim, upper, lower, round as spark_round,
    when, greatest, coalesce, sum as spark_sum, count, avg, max as spark_max, min as spark_min
)

# Volume path where CSVs are stored
VOLUME_PATH = "/Volumes/workspace/default/adops_lab_data"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer — Raw Ingestion with DLT

# COMMAND ----------

@dlt.table(
    name="bronze_campaigns",
    comment="Raw campaign data from CSV uploads. No transforms applied.",
    table_properties={"quality": "bronze"}
)
def bronze_campaigns():
    """
    DLT automatically:
    - Reads the source
    - Tracks schema changes
    - Handles incremental processing
    """
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_PATH}/02_campaigns.csv")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("02_campaigns.csv"))
    )


@dlt.table(
    name="bronze_delivery",
    comment="Raw daily delivery data. Append-only fact table.",
    table_properties={"quality": "bronze"}
)
def bronze_delivery():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_PATH}/03_delivery.csv")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("03_delivery.csv"))
    )


@dlt.table(
    name="bronze_tickets",
    comment="Raw trafficking tickets.",
    table_properties={"quality": "bronze"}
)
def bronze_tickets():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_PATH}/04_tickets.csv")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("04_tickets.csv"))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Layer — Cleaned with Quality Expectations
# MAGIC
# MAGIC ### DLT Expectations:
# MAGIC - `@dlt.expect("name", "condition")` — Log violations, keep rows
# MAGIC - `@dlt.expect_or_drop("name", "condition")` — Silently drop failing rows
# MAGIC - `@dlt.expect_or_fail("name", "condition")` — Fail the pipeline if ANY row violates

# COMMAND ----------

@dlt.table(
    name="silver_campaigns",
    comment="Cleaned, deduplicated campaigns with standardized platforms and computed fields."
)
# Quality expectations — these run on EVERY pipeline execution
@dlt.expect("valid_campaign_id", "campaign_id IS NOT NULL")
@dlt.expect("positive_budget", "budget_usd > 0 OR budget_usd IS NULL")
@dlt.expect("valid_dates", "end_date >= start_date")
@dlt.expect("valid_status", "status IN ('Active','Paused','Completed','Cancelled','Draft','Unknown')")
def silver_campaigns():
    """
    Clean campaigns from Bronze. Notice how DLT handles the dependency
    automatically — it knows silver_campaigns reads from bronze_campaigns.
    """
    return (
        dlt.read("bronze_campaigns")
        .withColumn("brand_code", upper(trim(col("brand_code"))))
        .withColumn("targeting_geo", upper(trim(col("targeting_geo"))))
        .withColumn("language", upper(trim(col("language"))))
        .withColumn("platform",
            when(lower(trim(col("platform"))).isin("meta", "facebook", "instagram"), "Meta")
            .when(lower(trim(col("platform"))) == "tiktok", "TikTok")
            .when(lower(trim(col("platform"))).isin("cm360", "dcm"), "CM360")
            .when(lower(trim(col("platform"))).isin("dv360", "display video 360"), "DV360")
            .when(lower(trim(col("platform"))).isin("amazon dsp", "amazon"), "Amazon DSP")
            .when(lower(trim(col("platform"))).isin("snap", "snapchat"), "Snapchat")
            .otherwise(trim(col("platform")))
        )
        .withColumn("budget_usd",
            when(col("budget_usd") > 0, col("budget_usd")).otherwise(lit(None))
        )
        .withColumn("status",
            when(trim(col("status")).isin("Active", "Paused", "Completed", "Cancelled", "Draft"),
                 trim(col("status")))
            .otherwise("Unknown")
        )
        .withColumn("_silver_processed_at", current_timestamp())
    )


@dlt.table(
    name="silver_delivery",
    comment="Cleaned delivery data with recomputed CTR, CPM, CPC."
)
@dlt.expect_or_fail("valid_delivery_id", "delivery_id IS NOT NULL")
@dlt.expect_or_fail("valid_campaign_id", "campaign_id IS NOT NULL")
@dlt.expect("non_negative_impressions", "impressions >= 0")
@dlt.expect("non_negative_clicks", "clicks >= 0")
@dlt.expect("non_negative_spend", "spend_usd >= 0")
@dlt.expect("clicks_leq_impressions", "clicks <= impressions")
@dlt.expect("valid_viewability", "viewability_rate BETWEEN 0 AND 1")
def silver_delivery():
    """
    Clean delivery facts. Recomputes CTR/CPM from raw values.
    
    @dlt.expect_or_fail on IDs means: if a row has no delivery_id,
    the ENTIRE pipeline fails — because that's a data corruption signal.
    
    @dlt.expect (without _or_fail) on metrics means: log the violation
    but keep the row — it might be fixable later.
    """
    return (
        dlt.read("bronze_delivery")
        .withColumn("impressions", greatest(col("impressions"), lit(0)))
        .withColumn("clicks", greatest(col("clicks"), lit(0)))
        .withColumn("spend_usd", greatest(col("spend_usd"), lit(0)))
        .withColumn("vast_errors", greatest(col("vast_errors"), lit(0)))
        .withColumn("ctr",
            when(col("impressions") > 0,
                 spark_round(col("clicks").cast("double") / col("impressions"), 6))
            .otherwise(0.0)
        )
        .withColumn("viewability_rate",
            when(col("viewability_rate").between(0, 1), col("viewability_rate"))
            .when(col("viewability_rate") > 1, lit(1.0))
            .otherwise(lit(0.0))
        )
        .withColumn("cpm",
            when(col("impressions") > 0,
                 spark_round(col("spend_usd") / (col("impressions") / 1000.0), 4))
            .otherwise(lit(None))
        )
        .withColumn("cpc",
            when(col("clicks") > 0,
                 spark_round(col("spend_usd") / col("clicks"), 4))
            .otherwise(lit(None))
        )
        .withColumnRenamed("date", "delivery_date")
        .withColumn("_silver_processed_at", current_timestamp())
    )


@dlt.table(
    name="silver_tickets",
    comment="Cleaned tickets with SLA breach detection."
)
@dlt.expect_or_fail("valid_ticket_id", "ticket_id IS NOT NULL")
@dlt.expect("positive_sla", "sla_hours > 0")
@dlt.expect("valid_urgency", "urgency IN ('Low','Medium','High','Critical')")
def silver_tickets():
    return (
        dlt.read("bronze_tickets")
        .withColumn("urgency",
            when(trim(col("urgency")).isin("Low", "Medium", "High", "Critical"),
                 trim(col("urgency")))
            .otherwise("Medium")
        )
        .withColumn("platform",
            when(lower(trim(col("platform"))).isin("meta", "facebook"), "Meta")
            .when(lower(trim(col("platform"))) == "tiktok", "TikTok")
            .when(lower(trim(col("platform"))).isin("cm360", "dcm"), "CM360")
            .otherwise(trim(col("platform")))
        )
        .withColumn("sla_hours", greatest(col("sla_hours"), lit(1)))
        .withColumn("_silver_processed_at", current_timestamp())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer — Business KPIs
# MAGIC
# MAGIC DLT handles dependencies automatically. When silver_delivery updates,
# MAGIC gold tables that read from it are automatically refreshed.

# COMMAND ----------

@dlt.table(
    name="gold_platform_scorecard",
    comment="Cross-platform performance comparison for weekly reviews."
)
def gold_platform_scorecard():
    campaigns = dlt.read("silver_campaigns")
    delivery = dlt.read("silver_delivery")
    
    delivery_agg = (
        delivery.groupBy("campaign_id")
        .agg(
            spark_sum("impressions").alias("total_impressions"),
            spark_sum("clicks").alias("total_clicks"),
            spark_sum("spend_usd").alias("total_spend"),
            avg("viewability_rate").alias("avg_viewability"),
        )
    )
    
    return (
        campaigns
        .join(delivery_agg, "campaign_id", "left")
        .groupBy("platform", "region")
        .agg(
            count("campaign_id").alias("total_campaigns"),
            spark_sum(when(col("status") == "Active", 1).otherwise(0)).alias("active_campaigns"),
            spark_round(spark_sum("budget_usd"), 2).alias("total_budget"),
            coalesce(spark_sum("total_impressions"), lit(0)).alias("total_impressions"),
            coalesce(spark_sum("total_clicks"), lit(0)).alias("total_clicks"),
            coalesce(spark_sum("total_spend"), lit(0)).alias("total_spend"),
            spark_round(avg("avg_viewability") * 100, 2).alias("avg_viewability_pct"),
        )
        .withColumn("_gold_refreshed_at", current_timestamp())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Lineage
# MAGIC
# MAGIC After running this pipeline, go to the DLT **Pipeline Details** page.
# MAGIC You'll see an interactive DAG showing:
# MAGIC ```
# MAGIC bronze_campaigns ──→ silver_campaigns ──→ gold_platform_scorecard
# MAGIC bronze_delivery  ──→ silver_delivery  ──↗
# MAGIC bronze_tickets   ──→ silver_tickets
# MAGIC ```
# MAGIC
# MAGIC Click any table to see:
# MAGIC - Row counts
# MAGIC - Data quality metrics (how many rows passed/failed each expectation)
# MAGIC - Schema
# MAGIC - Refresh history
