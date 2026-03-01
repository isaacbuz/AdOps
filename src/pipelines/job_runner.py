"""
Databricks Jobs API Runner — Disney Ad Ops Lab
================================================
PURPOSE:
  Programmatically trigger and monitor Databricks Workflows (Jobs) from Python.
  
  In production ad ops, pipelines don't run manually. They're triggered by:
  1. A schedule (every hour, daily at 6am)
  2. An event (new files arrive from platform APIs)
  3. A REST API call from your orchestrator (EVE)

  This module wraps the Databricks Jobs API to:
  - Create workflow definitions
  - Trigger pipeline runs
  - Monitor run status
  - Fetch run results

DATABRICKS WORKFLOWS EXPLAINED:
  A "Workflow" (formerly "Job") is a DAG of tasks:
  
  [Bronze Ingestion] → [Silver Transforms] → [Gold Aggregation] → [Quality Checks]
                                                                  ↓
                                                          [Alert on Failure]
  
  Each task runs a notebook. Tasks can have dependencies.
  If a task fails, downstream tasks are skipped and alerts fire.

AUTHENTICATION:
  Uses the same PAT (Personal Access Token) as the SQL Connector.
  In production, you'd use Service Principals or OAuth instead.
"""

import os
import json
import time
from datetime import datetime
from typing import Optional, Dict, List
from dataclasses import dataclass, field

try:
    import requests
except ImportError:
    requests = None

from dotenv import load_dotenv

load_dotenv()


@dataclass
class TaskConfig:
    """
    Defines a single task in a Databricks Workflow.
    
    KEY FIELDS:
    - task_key: Unique name for this task in the workflow
    - notebook_path: Path to the notebook in the Databricks workspace
    - depends_on: List of task_keys this task waits for
    - cluster_id: Which compute to use (existing cluster or new job cluster)
    """
    task_key: str
    notebook_path: str
    depends_on: List[str] = field(default_factory=list)
    timeout_seconds: int = 3600
    parameters: Dict[str, str] = field(default_factory=dict)


@dataclass
class WorkflowConfig:
    """
    Defines a complete Databricks Workflow (multi-task job).
    
    SCHEDULE FORMAT (Quartz Cron):
    - "0 0 6 * * ?" = Every day at 6:00 AM
    - "0 0 * * * ?" = Every hour
    - "0 0/15 * * * ?" = Every 15 minutes
    """
    name: str
    tasks: List[TaskConfig]
    schedule: Optional[str] = None  # Quartz cron expression
    timezone: str = "America/New_York"
    email_notifications: List[str] = field(default_factory=list)
    max_concurrent_runs: int = 1
    tags: Dict[str, str] = field(default_factory=dict)


class DatabricksJobRunner:
    """
    Client for the Databricks Jobs API (REST API 2.1).
    
    USAGE:
        runner = DatabricksJobRunner()
        
        # Define a pipeline workflow
        workflow = WorkflowConfig(
            name="AdOps Daily Pipeline",
            tasks=[
                TaskConfig("bronze", "/Workspace/adops_lab/03_medallion_bronze"),
                TaskConfig("silver", "/Workspace/adops_lab/04_medallion_silver", depends_on=["bronze"]),
                TaskConfig("gold", "/Workspace/adops_lab/05_medallion_gold", depends_on=["silver"]),
            ],
            schedule="0 0 6 * * ?",  # 6am daily
        )
        
        # Create the job
        job_id = runner.create_or_update_workflow(workflow)
        
        # Trigger it now
        run_id = runner.trigger_run(job_id)
        
        # Wait for completion
        result = runner.wait_for_run(run_id)
    """
    
    def __init__(self):
        self.host = os.getenv("DATABRICKS_SERVER_HOSTNAME", "")
        self.token = os.getenv("DATABRICKS_ACCESS_TOKEN", "")
        self.base_url = f"https://{self.host}" if self.host else ""
        
        if not self.host or not self.token:
            print("⚠️ Databricks credentials not configured. Set DATABRICKS_SERVER_HOSTNAME and DATABRICKS_ACCESS_TOKEN.")
    
    @property
    def _headers(self) -> Dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
    
    def _api_call(self, method: str, endpoint: str, payload: Optional[Dict] = None) -> Dict:
        """Make a REST API call to Databricks."""
        if not requests:
            return {"error": "requests library not installed. pip install requests"}
        
        url = f"{self.base_url}/api/2.1/jobs{endpoint}"
        
        try:
            if method == "GET":
                resp = requests.get(url, headers=self._headers, params=payload, timeout=30)
            elif method == "POST":
                resp = requests.post(url, headers=self._headers, json=payload, timeout=30)
            else:
                return {"error": f"Unsupported method: {method}"}
            
            resp.raise_for_status()
            return resp.json() if resp.text else {}
            
        except Exception as e:
            return {"error": str(e)}
    
    def list_jobs(self, limit: int = 25) -> List[Dict]:
        """
        Lists all workflows in the workspace.
        Useful for checking if a pipeline already exists before creating a duplicate.
        """
        result = self._api_call("GET", "/list", {"limit": limit})
        return result.get("jobs", [])
    
    def create_or_update_workflow(self, config: WorkflowConfig) -> Optional[int]:
        """
        Creates a new Databricks Workflow or updates an existing one.
        
        IDEMPOTENT: If a workflow with the same name already exists,
        it resets (replaces) it. This is important for CI/CD — you want
        your pipeline definition in code, deployed automatically.
        """
        # Check if workflow already exists
        existing = self.list_jobs()
        existing_job = next(
            (j for j in existing if j.get("settings", {}).get("name") == config.name),
            None
        )
        
        # Build task definitions
        tasks = []
        for task in config.tasks:
            task_def = {
                "task_key": task.task_key,
                "notebook_task": {
                    "notebook_path": task.notebook_path,
                    "base_parameters": task.parameters,
                },
                "timeout_seconds": task.timeout_seconds,
            }
            
            if task.depends_on:
                task_def["depends_on"] = [{"task_key": dep} for dep in task.depends_on]
            
            tasks.append(task_def)
        
        # Build job settings
        settings = {
            "name": config.name,
            "tasks": tasks,
            "max_concurrent_runs": config.max_concurrent_runs,
            "tags": config.tags,
        }
        
        # Add schedule if specified
        if config.schedule:
            settings["schedule"] = {
                "quartz_cron_expression": config.schedule,
                "timezone_id": config.timezone,
                "pause_status": "UNPAUSED",
            }
        
        # Add email notifications
        if config.email_notifications:
            settings["email_notifications"] = {
                "on_failure": config.email_notifications,
                "on_start": [],
                "on_success": [],
            }
        
        if existing_job:
            # Update existing
            job_id = existing_job["job_id"]
            payload = {"job_id": job_id, "new_settings": settings}
            result = self._api_call("POST", "/reset", payload)
            if "error" not in result:
                print(f"✅ Updated workflow '{config.name}' (Job ID: {job_id})")
                return job_id
        else:
            # Create new
            result = self._api_call("POST", "/create", settings)
            job_id = result.get("job_id")
            if job_id:
                print(f"✅ Created workflow '{config.name}' (Job ID: {job_id})")
                return job_id
        
        print(f"❌ Failed to create/update workflow: {result.get('error', 'Unknown error')}")
        return None
    
    def trigger_run(self, job_id: int, parameters: Optional[Dict] = None) -> Optional[int]:
        """
        Triggers a one-time run of a workflow.
        Returns the run_id for monitoring.
        """
        payload = {"job_id": job_id}
        if parameters:
            payload["notebook_params"] = parameters
        
        result = self._api_call("POST", "/run-now", payload)
        run_id = result.get("run_id")
        
        if run_id:
            print(f"🚀 Triggered run {run_id} for Job {job_id}")
            return run_id
        
        print(f"❌ Failed to trigger run: {result.get('error', 'Unknown error')}")
        return None
    
    def get_run_status(self, run_id: int) -> Dict:
        """
        Gets the current status of a workflow run.
        
        LIFECYCLE STATES:
        - PENDING: Queued, waiting for cluster
        - RUNNING: Actively executing
        - TERMINATING: Cleaning up
        - TERMINATED: Done (check result_state for success/failure)
        - SKIPPED: Skipped due to upstream failure
        - INTERNAL_ERROR: Platform error (retry)
        
        RESULT STATES (when TERMINATED):
        - SUCCESS: All tasks passed ✅
        - FAILED: At least one task failed ❌
        - TIMEDOUT: Exceeded timeout ⏰
        - CANCELED: Manually cancelled 🚫
        """
        result = self._api_call("GET", f"/runs/get", {"run_id": run_id})
        
        state = result.get("state", {})
        return {
            "run_id": run_id,
            "lifecycle_state": state.get("life_cycle_state", "UNKNOWN"),
            "result_state": state.get("result_state"),
            "state_message": state.get("state_message", ""),
            "start_time": result.get("start_time"),
            "end_time": result.get("end_time"),
            "tasks": [
                {
                    "task_key": t.get("task_key"),
                    "state": t.get("state", {}).get("life_cycle_state"),
                    "result": t.get("state", {}).get("result_state"),
                }
                for t in result.get("tasks", [])
            ],
        }
    
    def wait_for_run(self, run_id: int, poll_interval: int = 15,
                     timeout: int = 3600) -> Dict:
        """
        Polls a run until it completes, then returns the final status.
        This is a blocking call — useful for scripts, not for production.
        """
        print(f"⏳ Waiting for run {run_id} to complete...")
        start = time.time()
        
        while time.time() - start < timeout:
            status = self.get_run_status(run_id)
            state = status["lifecycle_state"]
            
            if state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                result = status.get("result_state", "UNKNOWN")
                icon = "✅" if result == "SUCCESS" else "❌"
                print(f"{icon} Run {run_id} finished: {result}")
                
                # Print task results
                for task in status.get("tasks", []):
                    t_icon = "✅" if task["result"] == "SUCCESS" else "❌"
                    print(f"  {t_icon} {task['task_key']}: {task['result']}")
                
                return status
            
            elapsed = int(time.time() - start)
            print(f"  ⏳ {state}... ({elapsed}s elapsed)")
            time.sleep(poll_interval)
        
        print(f"⏰ Timeout after {timeout}s. Run {run_id} is still {state}.")
        return self.get_run_status(run_id)


# ─── Pre-built Pipeline Definitions ─────────────────────────────────────────

def get_daily_pipeline_config(notebook_base_path: str = "/Workspace/Users/adops_lab") -> WorkflowConfig:
    """
    Returns the standard daily AdOps pipeline configuration.
    
    DAG STRUCTURE:
    
    [Bronze Ingestion] ──→ [Silver Transforms] ──→ [Gold Aggregation]
                                                         │
                                                         ├──→ [Quality Checks]
                                                         └──→ [Campaign Alerts]
    """
    return WorkflowConfig(
        name="Disney AdOps - Daily Pipeline",
        tasks=[
            TaskConfig(
                task_key="bronze_ingestion",
                notebook_path=f"{notebook_base_path}/03_medallion_bronze",
                timeout_seconds=1800,
            ),
            TaskConfig(
                task_key="silver_transforms",
                notebook_path=f"{notebook_base_path}/04_medallion_silver",
                depends_on=["bronze_ingestion"],
                timeout_seconds=1800,
            ),
            TaskConfig(
                task_key="gold_aggregation",
                notebook_path=f"{notebook_base_path}/05_medallion_gold",
                depends_on=["silver_transforms"],
                timeout_seconds=1800,
            ),
            TaskConfig(
                task_key="quality_checks",
                notebook_path=f"{notebook_base_path}/07_workflow_orchestration",
                depends_on=["gold_aggregation"],
                parameters={"run_mode": "quality_only"},
                timeout_seconds=600,
            ),
        ],
        schedule="0 0 6 * * ?",  # Every day at 6:00 AM ET
        timezone="America/New_York",
        tags={"team": "ad-ops", "env": "production", "pipeline": "daily"},
    )


def get_hourly_delivery_config(notebook_base_path: str = "/Workspace/Users/adops_lab") -> WorkflowConfig:
    """
    Hourly delivery ingestion for near-real-time pacing.
    Only refreshes Bronze + Silver delivery, and updates Gold pacing.
    """
    return WorkflowConfig(
        name="Disney AdOps - Hourly Delivery Refresh",
        tasks=[
            TaskConfig(
                task_key="delivery_bronze",
                notebook_path=f"{notebook_base_path}/03_medallion_bronze",
                parameters={"tables": "delivery"},  # Only ingest delivery data
                timeout_seconds=600,
            ),
            TaskConfig(
                task_key="delivery_silver",
                notebook_path=f"{notebook_base_path}/04_medallion_silver",
                depends_on=["delivery_bronze"],
                parameters={"tables": "delivery"},
                timeout_seconds=600,
            ),
            TaskConfig(
                task_key="pacing_refresh",
                notebook_path=f"{notebook_base_path}/05_medallion_gold",
                depends_on=["delivery_silver"],
                parameters={"tables": "campaign_performance"},
                timeout_seconds=600,
            ),
        ],
        schedule="0 0 * * * ?",  # Every hour on the hour
        timezone="America/New_York",
        max_concurrent_runs=1,
        tags={"team": "ad-ops", "env": "production", "pipeline": "hourly"},
    )


# Quick test
if __name__ == "__main__":
    runner = DatabricksJobRunner()
    
    # Print the daily pipeline config
    config = get_daily_pipeline_config()
    print(f"Pipeline: {config.name}")
    print(f"Schedule: {config.schedule}")
    print(f"Tasks: {' → '.join(t.task_key for t in config.tasks)}")
    
    # List existing jobs (if credentials are set)
    if runner.host and runner.token:
        jobs = runner.list_jobs()
        print(f"\nExisting Jobs: {len(jobs)}")
        for job in jobs:
            print(f"  - {job.get('settings', {}).get('name', 'Unnamed')} (ID: {job.get('job_id')})")
