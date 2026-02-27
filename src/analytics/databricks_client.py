import os
import pandas as pd
from dotenv import load_dotenv

try:
    from databricks import sql
except ImportError:
    # Handle environment where databricks-sql-connector isn't installed yet
    sql = None

load_dotenv()

class DatabricksLakehouseClient:
    """
    Client for connecting strictly to Databricks SQL Warehouses or REST APIs.
    Used for reading massive datasets (Campaign Delivery) or triggering Databricks Jobs.
    """
    def __init__(self):
        self.server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
        self.catalog = os.getenv("DATABRICKS_CATALOG", "hive_metastore")
        self.schema = os.getenv("DATABRICKS_SCHEMA", "adops_analytics")

    def _get_connection(self):
        if not all([self.server_hostname, self.http_path, self.access_token, sql]):
            print("⚠️ Skipping Databricks Execution: Credentials missing in .env or databricks-sql-connector not installed.")
            return None
        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token
        )

    def fetch_pacing_data(self, campaign_id: str) -> pd.DataFrame:
        """
        Queries the Databricks Delta Lake specifically for a Campaign's pacing data.
        Returns a Pandas DataFrame to be visualized in Streamlit or passed back to EVE.
        """
        conn = self._get_connection()
        if not conn:
            return None
            
        # Standard safety: parameterized queries to prevent SQL injection
        query = f"SELECT * FROM {self.catalog}.{self.schema}.daily_pacing WHERE campaign_id = %s"
        
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (campaign_id,))
                result = cursor.fetchall()
                # Create DataFrame from result
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(result, columns=columns)
                print(f"✅ Executed Databricks Query for Campaign: {campaign_id} (Returned {len(df)} rows)")
                return df
                
        except Exception as e:
            print(f"❌ Databricks Query Failed: {e}")
            return None
        finally:
            if conn:
                conn.close()

# Quick test stub
if __name__ == "__main__":
    dbx = DatabricksLakehouseClient()
    # df = dbx.fetch_pacing_data("CMP-12345")
    # if df is not None:
    #     print(df.head())
