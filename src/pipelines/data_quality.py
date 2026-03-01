"""
Data Quality Framework — Disney Ad Ops Lab
============================================
PURPOSE:
  Data quality checks that run between each layer of the Medallion Architecture.
  Bad data should NEVER silently reach Gold tables.

  This module implements the "expect, detect, quarantine" pattern:
  1. EXPECT: Define what good data looks like (rules)
  2. DETECT: Run checks against actual data
  3. QUARANTINE: Isolate bad rows instead of dropping them

GREAT EXPECTATIONS PATTERN:
  This follows the same philosophy as the "Great Expectations" library
  used in production data engineering. Each expectation is:
  - Named (for tracking)
  - Documented (why it matters)
  - Severity-tagged (error = block pipeline, warn = log only)
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional


class Severity(Enum):
    ERROR = "error"    # Blocks pipeline progression
    WARN = "warn"      # Logged but data passes through
    INFO = "info"      # Informational metric


@dataclass
class Expectation:
    """
    A single data quality check.
    
    Example:
        Expectation(
            name="positive_impressions",
            description="Impressions should never be negative",
            table="delivery",
            column="impressions",
            check_sql="impressions >= 0",
            severity=Severity.ERROR
        )
    """
    name: str
    description: str
    table: str
    column: str
    check_sql: str
    severity: Severity
    
    def to_dlt_expect(self) -> str:
        """
        Convert to Delta Live Tables expectation syntax.
        
        DLT EXPECTATIONS EXPLAINED:
        In Delta Live Tables, you declare quality rules inline with your pipeline:
        
        @dlt.expect("positive_impressions", "impressions >= 0")
        - Rows that PASS continue to the target table
        - Rows that FAIL get logged to the event log
        
        @dlt.expect_or_drop("positive_impressions", "impressions >= 0")
        - Rows that FAIL get silently dropped (use sparingly!)
        
        @dlt.expect_or_fail("positive_impressions", "impressions >= 0")  
        - If ANY row fails, the ENTIRE pipeline fails (for critical checks)
        """
        if self.severity == Severity.ERROR:
            return f'@dlt.expect_or_fail("{self.name}", "{self.check_sql}")'
        else:
            return f'@dlt.expect("{self.name}", "{self.check_sql}")'


@dataclass
class QualityReport:
    """Results from running quality checks against a table."""
    table: str
    checked_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    total_rows: int = 0
    passed_rows: int = 0
    failed_rows: int = 0
    warnings: int = 0
    check_results: List[Dict] = field(default_factory=list)
    
    @property
    def pass_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return round(self.passed_rows / self.total_rows * 100, 2)
    
    @property
    def is_healthy(self) -> bool:
        """Table is healthy if no ERROR-level checks failed."""
        return all(
            r["status"] == "passed" 
            for r in self.check_results 
            if r["severity"] == "error"
        )

    def summary(self) -> str:
        status = "✅ HEALTHY" if self.is_healthy else "🚨 UNHEALTHY"
        lines = [
            f"Quality Report: {self.table} — {status}",
            f"  Total rows: {self.total_rows:,}",
            f"  Pass rate:  {self.pass_rate}%",
            f"  Failed:     {self.failed_rows:,}",
            f"  Warnings:   {self.warnings}",
            "",
            "  Check Results:"
        ]
        for r in self.check_results:
            icon = "✅" if r["status"] == "passed" else ("🚨" if r["severity"] == "error" else "⚠️")
            lines.append(f"    {icon} {r['name']}: {r['status']} ({r.get('failing_rows', 0)} rows)")
        return "\n".join(lines)


# ─── Quality Suite Definition ────────────────────────────────────────────────

ADOPS_QUALITY_SUITE = [
    # Campaign expectations
    Expectation("campaign_has_id", "Every campaign row must have a campaign_id",
                "campaigns", "campaign_id", "campaign_id IS NOT NULL", Severity.ERROR),
    Expectation("campaign_positive_budget", "Budget must be greater than zero",
                "campaigns", "budget_usd", "budget_usd > 0", Severity.ERROR),
    Expectation("campaign_valid_dates", "End date must be on or after start date",
                "campaigns", "end_date", "end_date >= start_date", Severity.ERROR),
    Expectation("campaign_valid_status", "Status must be a recognized value",
                "campaigns", "status",
                "status IN ('Active','Paused','Completed','Cancelled','Draft')", Severity.WARN),
    Expectation("campaign_has_goal", "Active campaigns should have an impression goal",
                "campaigns", "impressions_goal", "impressions_goal > 0", Severity.WARN),
    
    # Delivery expectations
    Expectation("delivery_has_id", "Every delivery row must have a delivery_id",
                "delivery", "delivery_id", "delivery_id IS NOT NULL", Severity.ERROR),
    Expectation("delivery_non_negative_imps", "Impressions cannot be negative",
                "delivery", "impressions", "impressions >= 0", Severity.ERROR),
    Expectation("delivery_non_negative_clicks", "Clicks cannot be negative",
                "delivery", "clicks", "clicks >= 0", Severity.ERROR),
    Expectation("delivery_non_negative_spend", "Spend cannot be negative",
                "delivery", "spend_usd", "spend_usd >= 0", Severity.ERROR),
    Expectation("delivery_valid_ctr", "CTR should be between 0 and 1",
                "delivery", "ctr", "ctr >= 0 AND ctr <= 1.0", Severity.WARN),
    Expectation("delivery_clicks_leq_imps", "Clicks should not exceed impressions",
                "delivery", "clicks", "clicks <= impressions", Severity.WARN),
    Expectation("delivery_valid_viewability", "Viewability should be between 0 and 1",
                "delivery", "viewability_rate",
                "viewability_rate >= 0 AND viewability_rate <= 1.0", Severity.WARN),
    
    # Ticket expectations
    Expectation("ticket_has_id", "Every ticket must have a ticket_id",
                "tickets", "ticket_id", "ticket_id IS NOT NULL", Severity.ERROR),
    Expectation("ticket_positive_sla", "SLA hours must be positive",
                "tickets", "sla_hours", "sla_hours > 0", Severity.ERROR),
    Expectation("ticket_valid_urgency", "Urgency must be a known tier",
                "tickets", "urgency",
                "urgency IN ('Low','Medium','High','Critical')", Severity.WARN),
]


def generate_quality_check_sql(expectation: Expectation,
                                catalog: str = "hive_metastore",
                                schema: str = "adops_silver") -> str:
    """
    Generates SQL to count passing vs failing rows for a single expectation.
    
    This returns a result like:
    | check_name           | total_rows | passing | failing | pass_rate |
    | positive_impressions | 50000      | 49998   | 2       | 99.996%   |
    """
    return f"""
SELECT
    '{expectation.name}' as check_name,
    '{expectation.severity.value}' as severity,
    COUNT(*) as total_rows,
    SUM(CASE WHEN {expectation.check_sql} THEN 1 ELSE 0 END) as passing_rows,
    SUM(CASE WHEN NOT ({expectation.check_sql}) THEN 1 ELSE 0 END) as failing_rows,
    ROUND(
        SUM(CASE WHEN {expectation.check_sql} THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        4
    ) as pass_rate_pct
FROM {catalog}.{schema}.{expectation.table}
"""


def generate_full_quality_report_sql(table_name: str,
                                      catalog: str = "hive_metastore",
                                      schema: str = "adops_silver") -> str:
    """
    Generates a UNION ALL query that runs ALL quality checks for a table at once.
    This is efficient — one query instead of N separate queries.
    """
    table_checks = [e for e in ADOPS_QUALITY_SUITE if e.table == table_name]
    
    if not table_checks:
        return f"-- No quality checks defined for table: {table_name}"
    
    queries = []
    for exp in table_checks:
        queries.append(generate_quality_check_sql(exp, catalog, schema))
    
    return "\nUNION ALL\n".join(queries) + "\nORDER BY severity, pass_rate_pct ASC;"


def generate_quality_dashboard_view_sql(
    catalog: str = "hive_metastore",
    gold_schema: str = "adops_gold"
) -> str:
    """
    Creates a summary view for data quality monitoring dashboards.
    Runs all checks across all tables and stores the results.
    """
    return f"""
-- =====================================================================
-- Gold: Data Quality Scoreboard
-- Run this daily to track data quality trends over time
-- =====================================================================
CREATE OR REPLACE TABLE {catalog}.{gold_schema}.data_quality_scoreboard AS

SELECT
    current_timestamp() as check_run_at,
    'campaigns' as table_name,
    (SELECT COUNT(*) FROM {catalog}.adops_silver.campaigns) as total_rows,
    (SELECT COUNT(*) FROM {catalog}.adops_silver.campaigns WHERE budget_usd > 0) as budget_valid,
    (SELECT COUNT(*) FROM {catalog}.adops_silver.campaigns WHERE end_date >= start_date) as dates_valid,
    (SELECT COUNT(*) FROM {catalog}.adops_silver.campaigns WHERE impressions_goal > 0) as goals_set

UNION ALL

SELECT
    current_timestamp(),
    'delivery',
    (SELECT COUNT(*) FROM {catalog}.adops_silver.delivery),
    (SELECT COUNT(*) FROM {catalog}.adops_silver.delivery WHERE impressions >= 0),
    (SELECT COUNT(*) FROM {catalog}.adops_silver.delivery WHERE clicks <= impressions),
    (SELECT COUNT(*) FROM {catalog}.adops_silver.delivery WHERE spend_usd >= 0)

UNION ALL

SELECT
    current_timestamp(),
    'tickets',
    (SELECT COUNT(*) FROM {catalog}.adops_silver.tickets),
    (SELECT COUNT(*) FROM {catalog}.adops_silver.tickets WHERE sla_hours > 0),
    (SELECT COUNT(*) FROM {catalog}.adops_silver.tickets WHERE NOT is_breached),
    (SELECT COUNT(*) FROM {catalog}.adops_silver.tickets WHERE urgency IN ('Low','Medium','High','Critical'));
"""
