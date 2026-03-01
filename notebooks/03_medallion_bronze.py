# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3: Medallion Architecture — Bronze Layer
# MAGIC **Disney Ad Ops Lab — Raw Data Landing Zone**
# MAGIC
# MAGIC ## What is the Bronze Layer?
# MAGIC Bronze is the **raw landing zone** for all data entering the Lakehouse.
# MAGIC Data arrives here *exactly* as it came from the source system — no cleaning,
# MAGIC no transforms, no business logic applied.
# MAGIC
# MAGIC ### Rules of Bronze:
# MAGIC 1. **Append-only** — Never update or delete. Always add new rows.
# MAGIC 2. **Schema enforcement** — Use explicit types so bad data fails loudly.
# MAGIC 3. **Metadata stamps** — Every row tagged with ingestion time + source file.
# MAGIC 4. **Partitioned for speed** — Large tables partitioned by date.
# MAGIC
# MAGIC ### Why not just use the raw CSVs?
# MAGIC - CSVs don't support ACID transactions (half-written files corrupt data)
# MAGIC - No time travel (can't query "what did this data look like yesterday?")
# MAGIC - No schema enforcement (a string in a number column = silent errors)
# MAGIC - Delta format adds all of these on top of your data lake storage

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Setup: Create Bronze Database & Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Unity Catalog structure: catalog.schema.table
# MAGIC -- Using hive_metastore for simplicity; in production, use a named catalog
# MAGIC CREATE DATABASE IF NOT EXISTS adops_bronze
# MAGIC COMMENT 'Raw landing zone for Disney Ad Ops data. Append-only, no transforms.';
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS adops_silver
# MAGIC COMMENT 'Cleaned, deduplicated, and conformed data.';
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS adops_gold
# MAGIC COMMENT 'Business-ready aggregations and KPIs.';
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS adops_quarantine
# MAGIC COMMENT 'Data that failed quality checks. Reviewed by data engineers.';

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Define Explicit Schemas
# MAGIC
# MAGIC **Why explicit schemas?** At scale, `inferSchema=true` is:
# MAGIC - **Slow**: Spark reads the entire file twice (once to infer, once to load)
# MAGIC - **Unreliable**: "12345" could be a string OR a number — Spark guesses
# MAGIC - **Inconsistent**: Different files might infer different types for the same column
# MAGIC
# MAGIC Explicit schemas are a contract: "This column IS a DATE, period."

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, DateType, TimestampType
)
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit, to_date
)
from datetime import datetime

# ─── Campaign Schema ──────────────────────────────────────────────────────
campaign_schema = StructType([
    StructField("campaign_id", StringType(), False),      # NOT NULL — primary key
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
    StructField("audience_detailed", StringType(), True),
])

# ─── Delivery Schema ──────────────────────────────────────────────────────
delivery_schema = StructType([
    StructField("delivery_id", StringType(), False),
    StructField("campaign_id", StringType(), False),
    StructField("date", DateType(), True),
    StructField("impressions", IntegerType(), True),
    StructField("clicks", IntegerType(), True),
    StructField("ctr", DoubleType(), True),
    StructField("spend_usd", DoubleType(), True),
    StructField("vast_errors", IntegerType(), True),
    StructField("viewability_rate", DoubleType(), True),
])

# ─── Tickets Schema ───────────────────────────────────────────────────────
ticket_schema = StructType([
    StructField("ticket_id", StringType(), False),
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
    StructField("due_date", StringType(), True),
    StructField("assignee", StringType(), True),
    StructField("assignee_role", StringType(), True),
    StructField("sla_hours", IntegerType(), True),
    StructField("notes", StringType(), True),
])

# ─── QA Checks Schema ─────────────────────────────────────────────────────
qa_schema = StructType([
    StructField("qa_id", StringType(), False),
    StructField("ticket_id", StringType(), True),
    StructField("check_name", StringType(), True),
    StructField("check_details", StringType(), True),
    StructField("result", StringType(), True),
    StructField("checked_by", StringType(), True),
    StructField("checked_at", StringType(), True),
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Bronze Ingestion Function
# MAGIC
# MAGIC This function implements the core Bronze ingestion pattern:
# MAGIC 1. Read CSV with explicit schema
# MAGIC 2. Add metadata columns (_ingested_at, _source_file, _batch_id)
# MAGIC 3. Write as Delta table (append mode for facts, overwrite for dimensions)
# MAGIC
# MAGIC **APPEND vs OVERWRITE:**
# MAGIC - **Delivery data** → APPEND — new rows arrive daily, never modify old ones
# MAGIC - **Campaigns** → OVERWRITE — we get a full snapshot each time (small table)
# MAGIC - **Tickets** → OVERWRITE — same as campaigns

# COMMAND ----------

# Volume path where CSVs are uploaded
VOLUME_PATH = "/Volumes/workspace/default/adops_lab_data"
BATCH_ID = f"bronze_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def ingest_to_bronze(file_name, table_name, schema=None, mode="overwrite", partition_by=None):
    """
    Ingest a CSV file into a Bronze Delta table.
    
    Parameters:
    - file_name: Name of the CSV in the volume
    - table_name: Target Delta table name
    - schema: Explicit PySpark schema (StructType)
    - mode: 'overwrite' for dimensions, 'append' for facts
    - partition_by: Column to partition by (use for large tables like delivery)
    """
    try:
        # Read with explicit schema
        reader = spark.read.option("header", "true")
        if schema:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", "true")
        
        df = reader.csv(f"{VOLUME_PATH}/{file_name}")
        
        # Add metadata columns — these are CRITICAL for debugging and lineage
        df = (df
            .withColumn("_ingested_at", current_timestamp())           # When was this loaded?
            .withColumn("_source_file", lit(file_name))                # Where did it come from?
            .withColumn("_batch_id", lit(BATCH_ID))                    # Which batch is this?
        )
        
        # Write as Delta table
        writer = df.write.format("delta").mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        # Set Delta optimizations
        writer = writer.option("overwriteSchema", "true")  # Allow schema evolution
        writer.saveAsTable(f"adops_bronze.{table_name}")
        
        row_count = df.count()
        print(f"✅ {table_name}: {row_count:,} rows → adops_bronze.{table_name} ({mode})")
        return row_count
        
    except Exception as e:
        print(f"❌ {table_name}: Failed — {e}")
        print(f"   Ensure {file_name} is uploaded to: {VOLUME_PATH}/")
        return 0

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Execute Bronze Ingestion
# MAGIC
# MAGIC **Order matters**: Ingest reference tables first (titles, mappings),
# MAGIC then core tables (campaigns, tickets), then fact tables (delivery, QA).
# MAGIC This ensures foreign key relationships can be validated downstream.

# COMMAND ----------

print(f"🔄 Starting Bronze ingestion — Batch: {BATCH_ID}")
print("=" * 60)

# Reference tables (small, overwrite each time)
ingest_to_bronze("01_titles.csv", "titles")
ingest_to_bronze("06_brand_mapping.csv", "brand_mapping")
ingest_to_bronze("07_channel_mapping.csv", "channel_mapping")
ingest_to_bronze("08_markets.csv", "markets")
ingest_to_bronze("09_users.csv", "users")
ingest_to_bronze("10_ticket_types.csv", "ticket_types")
ingest_to_bronze("11_audiences.csv", "audiences")

print("─" * 60)

# Core dimension tables (overwrite — we get full snapshots)
ingest_to_bronze("02_campaigns.csv", "campaigns", campaign_schema, mode="overwrite")
ingest_to_bronze("04_tickets.csv", "tickets", ticket_schema, mode="overwrite")

print("─" * 60)

# Fact tables (append — new data arrives incrementally)
# NOTE: In production, you'd use Auto Loader here instead of batch read
ingest_to_bronze("03_delivery.csv", "delivery", delivery_schema, mode="overwrite", partition_by="date")
ingest_to_bronze("05_qa_checks.csv", "qa_checks", qa_schema, mode="overwrite")

print("=" * 60)
print(f"✅ Bronze ingestion complete!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Verify Bronze Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN adops_bronze;

# COMMAND ----------

# Quick row count check
for table in ["campaigns", "delivery", "tickets", "qa_checks", "titles", "markets", "users"]:
    try:
        count = spark.table(f"adops_bronze.{table}").count()
        print(f"  📊 adops_bronze.{table}: {count:,} rows")
    except Exception as e:
        print(f"  ❌ adops_bronze.{table}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Delta Lake Features Demo
# MAGIC
# MAGIC ### Time Travel
# MAGIC Delta Lake keeps a transaction log of every change. You can query any
# MAGIC previous version of a table — perfect for debugging "what changed?"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See the history of changes to the campaigns table
# MAGIC DESCRIBE HISTORY adops_bronze.campaigns;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the PREVIOUS version of campaigns (time travel!)
# MAGIC -- This is incredibly useful for debugging: "What did this table look like yesterday?"
# MAGIC -- SELECT * FROM adops_bronze.campaigns VERSION AS OF 0;

# COMMAND ----------
# MAGIC %md
# MAGIC ### Optimize & Z-Order
# MAGIC
# MAGIC **OPTIMIZE** compacts small files into larger ones (fixes "small file problem")
# MAGIC **Z-ORDER** co-locates related data for faster queries on filtered columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize the delivery table (it gets new data frequently)
# MAGIC OPTIMIZE adops_bronze.delivery
# MAGIC ZORDER BY (campaign_id);
# MAGIC
# MAGIC -- This makes queries like "WHERE campaign_id = 'CMP-0001'" much faster
# MAGIC -- because all rows for that campaign are stored in the same file blocks

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Auto Loader Template (Production Pattern)
# MAGIC
# MAGIC In production, you wouldn't manually run batch ingestion. You'd use
# MAGIC **Auto Loader** to automatically pick up new files as they arrive.
# MAGIC
# MAGIC Auto Loader uses `cloudFiles` format and maintains a checkpoint
# MAGIC so it only processes each file once.

# COMMAND ----------

# UNCOMMENT THIS FOR PRODUCTION USE:
# Auto Loader continuously watches for new delivery CSVs
#
# (spark.readStream
#     .format("cloudFiles")
#     .option("cloudFiles.format", "csv")
#     .option("cloudFiles.schemaLocation", "/mnt/checkpoints/delivery_schema")
#     .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#     .option("header", "true")
#     .schema(delivery_schema)
#     .load(f"{VOLUME_PATH}/delivery_*.csv")  # Glob pattern for daily files
#     .withColumn("_ingested_at", current_timestamp())
#     .withColumn("_source_file", input_file_name())
#     .withColumn("_batch_id", lit("autoloader"))
#     .writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", "/mnt/checkpoints/delivery")
#     .trigger(availableNow=True)  # Process available files then stop
#     .toTable("adops_bronze.delivery")
# )
