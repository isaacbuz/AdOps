# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1: Data Ingestion & Foundation
# MAGIC **Disney Ad Ops Lab — Setup & Delta Tables**
# MAGIC
# MAGIC ### Instructions to upload CSVs:
# MAGIC 1. In the left sidebar, click **Catalog**
# MAGIC 2. Click **Add** -> **Add data**
# MAGIC 3. Upload all 11 CSV files from your local `data/` folder into `/Volumes/workspace/default/adops_lab_data/`
# MAGIC 4. Run the cells below to provision Delta tables.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import col, sum, count, when, current_date, datediff

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Define explicit schemas for type safety

# COMMAND ----------

# Campaigns Schema
campaign_schema = StructType([
    StructField("campaign_id", StringType(), True),
    StructField("title_id", StringType(), True),
    StructField("title_name", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("product", StringType(), True),
    StructField("campaign_name", StringType(), True),
    StructField("campaign_objective", StringType(), True),
    StructField("targeting_geo", StringType(), True),
    StructField("country", StringType(), True),
    StructField("language", StringType(), True),
    StructField("geo_cluster", StringType(), True),
    StructField("region", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("channel_mapped", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("budget_usd", DoubleType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("impressions_goal", IntegerType(), True),
    StructField("flight_priority", IntegerType(), True),
    StructField("audience_tactic", StringType(), True),
    StructField("audience_strategy", StringType(), True),
    StructField("audience_detailed", StringType(), True)
])

# Delivery Schema
delivery_schema = StructType([
    StructField("delivery_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("date", DateType(), True),
    StructField("impressions", IntegerType(), True),
    StructField("clicks", IntegerType(), True),
    StructField("ctr", DoubleType(), True),
    StructField("spend_usd", DoubleType(), True),
    StructField("vast_errors", IntegerType(), True),
    StructField("viewability_rate", DoubleType(), True)
])

# Tickets Schema
ticket_schema = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("request_type", StringType(), True),
    StructField("routed_to_role", StringType(), True),
    StructField("eve_eligible", BooleanType(), True),
    StructField("urgency", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("targeting_geo", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("requested_by", StringType(), True),
    StructField("created_date", DateType(), True),
    StructField("due_date", StringType(), True), # Storing as String datetime initially
    StructField("assignee", StringType(), True),
    StructField("assignee_role", StringType(), True),
    StructField("sla_hours", IntegerType(), True),
    StructField("notes", StringType(), True)
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Read CSVs & Write Delta Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS adops_lab;
# MAGIC USE adops_lab;

# COMMAND ----------

# Setup Base Path (assuming uploads to /FileStore/adops_lab/)
base_path = "/Volumes/workspace/default/adops_lab_data/"

def ingest_table(file_name, table_name, schema=None):
    try:
        if schema:
            df = spark.read.option("header", "true").schema(schema).csv(f"{base_path}{file_name}")
        else:
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{base_path}{file_name}")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Successfully wrote {table_name}")
    except Exception as e:
        print(f"Could not load {file_name}. Ensure it is uploaded to DBFS -> {base_path}")

# Ingest core tables
ingest_table("02_campaigns.csv", "campaigns", campaign_schema)
ingest_table("03_delivery.csv", "delivery", delivery_schema)
ingest_table("04_tickets.csv", "tickets", ticket_schema)
# Ingest rest with inferred schema
ingest_table("01_titles.csv", "titles")
ingest_table("05_qa_checks.csv", "qa_checks")
ingest_table("08_markets.csv", "markets")
ingest_table("09_users.csv", "users")
ingest_table("10_ticket_types.csv", "ticket_types")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Verify Database

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN adops_lab;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Exploratory Analysis

# COMMAND ----------
# MAGIC %md
# MAGIC ### Campaign Distribution by Objective & Platform

# COMMAND ----------

camp_dist = spark.sql("""
    SELECT campaign_objective, platform, count(*) as num_campaigns, sum(budget_usd) as total_budget
    FROM adops_lab.campaigns
    GROUP BY campaign_objective, platform
    ORDER BY total_budget DESC
""")
display(camp_dist)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Zero Delivery Detection

# COMMAND ----------

zero_delivery = spark.sql("""
    SELECT d.date, c.campaign_name, c.platform, c.status, d.impressions, d.clicks
    FROM adops_lab.delivery d
    JOIN adops_lab.campaigns c ON d.campaign_id = c.campaign_id
    WHERE d.impressions = 0 AND c.status = 'Active'
    ORDER BY d.date DESC
""")
display(zero_delivery)

# COMMAND ----------
# MAGIC %md
# MAGIC ### VAST Error Report

# COMMAND ----------

vast_errors = spark.sql("""
    SELECT c.campaign_name, c.platform, SUM(d.vast_errors) as total_vast_errors
    FROM adops_lab.delivery d
    JOIN adops_lab.campaigns c ON d.campaign_id = c.campaign_id
    WHERE d.vast_errors > 0
    GROUP BY c.campaign_name, c.platform
    ORDER BY total_vast_errors DESC
""")
display(vast_errors)

# COMMAND ----------
# MAGIC %md
# MAGIC ### SLA Analysis

# COMMAND ----------

sla_analysis = spark.sql("""
    SELECT ticket_id, title, request_type, urgency, due_date, assignee,
           datediff(to_date(due_date), current_date()) as days_to_due,
           CASE 
             WHEN to_date(due_date) < current_date() AND stage != 'Completed' THEN 'Overdue'
             ELSE 'On Track'
           END as sla_status
    FROM adops_lab.tickets
    ORDER BY days_to_due ASC
""")
display(sla_analysis)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Ticket Distribution & EVE Breakdown

# COMMAND ----------

tkt_dist = spark.sql("""
    SELECT request_type, routed_to_role, count(*) as num_tickets
    FROM adops_lab.tickets
    GROUP BY request_type, routed_to_role
    ORDER BY num_tickets DESC
""")
display(tkt_dist)

eve_eligible = spark.sql("""
    SELECT stage, count(*) as eligible_tickets
    FROM adops_lab.tickets
    WHERE eve_eligible = true
    GROUP BY stage
""")
display(eve_eligible)

