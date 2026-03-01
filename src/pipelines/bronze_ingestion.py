"""
Bronze Layer Ingestion — Disney Ad Ops Lab
============================================
PURPOSE:
  The Bronze layer is the "raw landing zone." Data arrives here EXACTLY as it came
  from the source system — no cleaning, no transforms, no business logic.
  
  Think of it like a warehouse receiving dock. Packages arrive, you stamp them
  with a received date, log where they came from, and store them untouched.

WHY THIS MATTERS IN AD OPS:
  - Campaign delivery data arrives from Meta, CM360, TikTok, etc. at different times
  - Each source has different schemas and naming conventions
  - You need to keep the raw data for auditing and debugging
  - If a transform goes wrong in Silver/Gold, you can always re-derive from Bronze

KEY CONCEPTS:
  1. Append-only: Never overwrite Bronze data, always append new rows
  2. Metadata stamping: Tag every row with ingestion timestamp + source
  3. Schema enforcement: Use explicit types, don't rely on inference at scale
  4. Auto Loader: Databricks feature that incrementally picks up new files
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


# ─── Schema Registry ────────────────────────────────────────────────────────
# In Databricks, you'd use StructType. Here we define them as dicts so this
# code runs locally AND in Databricks. The notebooks reference these.

BRONZE_SCHEMAS = {
    "campaigns": {
        "columns": [
            ("campaign_id", "STRING"),
            ("title_id", "STRING"),
            ("title_name", "STRING"),
            ("brand", "STRING"),
            ("brand_code", "STRING"),
            ("product", "STRING"),
            ("campaign_name", "STRING"),
            ("campaign_objective", "STRING"),
            ("targeting_geo", "STRING"),
            ("country", "STRING"),
            ("language", "STRING"),
            ("geo_cluster", "STRING"),
            ("region", "STRING"),
            ("channel", "STRING"),
            ("channel_mapped", "STRING"),
            ("platform", "STRING"),
            ("budget_usd", "DOUBLE"),
            ("start_date", "DATE"),
            ("end_date", "DATE"),
            ("status", "STRING"),
            ("impressions_goal", "INT"),
            ("flight_priority", "INT"),
            ("audience_tactic", "STRING"),
            ("audience_strategy", "STRING"),
            ("audience_detailed", "STRING"),
        ],
        "source_file": "02_campaigns.csv",
        "partition_by": None,  # Small dimension table — no partition needed
    },
    "delivery": {
        "columns": [
            ("delivery_id", "STRING"),
            ("campaign_id", "STRING"),
            ("date", "DATE"),
            ("impressions", "INT"),
            ("clicks", "INT"),
            ("ctr", "DOUBLE"),
            ("spend_usd", "DOUBLE"),
            ("vast_errors", "INT"),
            ("viewability_rate", "DOUBLE"),
        ],
        "source_file": "03_delivery.csv",
        "partition_by": "date",  # Partition by date — this is THE key optimization
    },
    "tickets": {
        "columns": [
            ("ticket_id", "STRING"),
            ("campaign_id", "STRING"),
            ("title", "STRING"),
            ("request_type", "STRING"),
            ("routed_to_role", "STRING"),
            ("eve_eligible", "BOOLEAN"),
            ("urgency", "STRING"),
            ("stage", "STRING"),
            ("platform", "STRING"),
            ("targeting_geo", "STRING"),
            ("brand", "STRING"),
            ("requested_by", "STRING"),
            ("created_date", "DATE"),
            ("due_date", "STRING"),
            ("assignee", "STRING"),
            ("assignee_role", "STRING"),
            ("sla_hours", "INT"),
            ("notes", "STRING"),
        ],
        "source_file": "04_tickets.csv",
        "partition_by": None,
    },
    "qa_checks": {
        "columns": [
            ("qa_id", "STRING"),
            ("ticket_id", "STRING"),
            ("check_name", "STRING"),
            ("check_details", "STRING"),
            ("result", "STRING"),
            ("checked_by", "STRING"),
            ("checked_at", "STRING"),
        ],
        "source_file": "05_qa_checks.csv",
        "partition_by": None,
    },
    "titles": {
        "columns": [
            ("title_id", "STRING"),
            ("title_name", "STRING"),
            ("franchise", "STRING"),
            ("release_window", "STRING"),
            ("content_type", "STRING"),
        ],
        "source_file": "01_titles.csv",
        "partition_by": None,
    },
}

# Lookup/reference tables with simple schemas
REFERENCE_TABLES = {
    "brand_mapping": "06_brand_mapping.csv",
    "channel_mapping": "07_channel_mapping.csv",
    "markets": "08_markets.csv",
    "users": "09_users.csv",
    "ticket_types": "10_ticket_types.csv",
    "audiences": "11_audiences.csv",
}


@dataclass
class IngestionRecord:
    """
    Metadata attached to every Bronze ingestion batch.
    This is how you track lineage — "where did this data come from?"
    
    In a real Disney pipeline, this would also include:
    - Source system ID (Meta API, CM360, etc.)
    - API version
    - Request ID for traceability
    """
    source_file: str
    table_name: str
    ingested_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    row_count: int = 0
    status: str = "pending"  # pending | success | failed
    error: Optional[str] = None

    def mark_success(self, row_count: int):
        self.status = "success"
        self.row_count = row_count

    def mark_failed(self, error: str):
        self.status = "failed"
        self.error = error


def generate_bronze_create_sql(table_name: str, catalog: str = "hive_metastore",
                                schema: str = "adops_bronze") -> str:
    """
    Generates CREATE TABLE SQL for a Bronze table.
    
    Bronze tables always include:
    - _ingested_at: When this row was loaded (important for debugging)
    - _source_file: Which file this row came from (lineage)
    - _batch_id: Groups rows from the same load (for rollback)
    
    WHY DELTA FORMAT:
    - ACID transactions: If ingestion fails halfway, no partial data
    - Time travel: Query data as it was yesterday (SELECT * FROM t VERSION AS OF 5)
    - Schema evolution: Add columns without breaking existing queries
    """
    if table_name not in BRONZE_SCHEMAS:
        raise ValueError(f"Unknown table: {table_name}. Available: {list(BRONZE_SCHEMAS.keys())}")
    
    config = BRONZE_SCHEMAS[table_name]
    columns = config["columns"]
    
    # Build column definitions
    col_defs = []
    for col_name, col_type in columns:
        col_defs.append(f"    {col_name} {col_type}")
    
    # Add metadata columns (these exist on EVERY Bronze table)
    col_defs.append("    _ingested_at TIMESTAMP")
    col_defs.append("    _source_file STRING")
    col_defs.append("    _batch_id STRING")
    
    columns_sql = ",\n".join(col_defs)
    
    partition_clause = ""
    if config["partition_by"]:
        partition_clause = f"\nPARTITIONED BY ({config['partition_by']})"
    
    return f"""
-- Bronze table: {table_name}
-- Source: {config['source_file']}
-- Pattern: Append-only, raw data with metadata stamps
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
{columns_sql}
)
USING DELTA{partition_clause}
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze'
);
"""


def generate_autoloader_sql(table_name: str, volume_path: str,
                             catalog: str = "hive_metastore",
                             schema: str = "adops_bronze") -> str:
    """
    Generates Auto Loader (cloudFiles) ingestion code.
    
    AUTO LOADER EXPLAINED:
    Auto Loader is Databricks' incremental file ingestion engine.
    Instead of "load all files from this folder", it keeps a checkpoint
    of which files it has already processed, so:
    
    - First run: Processes ALL files in the folder
    - Next run: Only processes NEW files added since last run
    - This is critical for production: you don't re-process 10TB of data daily
    
    In Ad Ops, new delivery/impression CSVs land every hour from platform APIs.
    Auto Loader picks them up automatically without you writing cron jobs.
    """
    if table_name not in BRONZE_SCHEMAS:
        raise ValueError(f"Unknown table: {table_name}")
    
    config = BRONZE_SCHEMAS[table_name]
    source_file = config["source_file"]
    
    return f"""
# Auto Loader ingestion for: {table_name}
# This runs as a STREAMING job — it continuously watches for new files
(spark.readStream
    .format("cloudFiles")                              # Auto Loader format
    .option("cloudFiles.format", "csv")                # Source file format
    .option("cloudFiles.schemaLocation",               # Where to store inferred schema
            "/mnt/checkpoints/{table_name}_schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Handle new columns gracefully
    .option("header", "true")
    .load("{volume_path}/{source_file}")
    # Add metadata columns for lineage tracking
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_batch_id", lit("{table_name}_" + datetime.now().strftime("%Y%m%d_%H%M%S")))
    .writeStream
    .format("delta")
    .outputMode("append")                              # Bronze = append only, NEVER overwrite
    .option("checkpointLocation",                      # Tracks which files are already processed
            "/mnt/checkpoints/{table_name}")
    .trigger(availableNow=True)                        # Process all available, then stop
    .toTable("{catalog}.{schema}.{table_name}")
)
"""


def generate_batch_ingest_sql(table_name: str, volume_path: str,
                               catalog: str = "hive_metastore",
                               schema: str = "adops_bronze") -> str:
    """
    Generates batch (non-streaming) ingestion SQL.
    Use this when Auto Loader isn't needed — e.g., initial bulk load or small reference tables.
    """
    if table_name not in BRONZE_SCHEMAS:
        raise ValueError(f"Unknown table: {table_name}")
    
    config = BRONZE_SCHEMAS[table_name]
    
    return f"""
-- Batch ingestion for: {table_name}
-- Use this for initial data load or small reference tables
COPY INTO {catalog}.{schema}.{table_name}
FROM '{volume_path}/{config["source_file"]}'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true',
    'mergeSchema' = 'true'
)
COPY_OPTIONS ('mergeSchema' = 'true');
"""
