import os
from dotenv import load_dotenv

try:
    from databricks import sql
except ImportError:
    print("❌ databricks-sql-connector is not installed. Run: pip install databricks-sql-connector")
    exit(1)

# Load environment variables from .env file
load_dotenv()

def test_connection():
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")

    if not all([server_hostname, http_path, access_token]):
        print("⚠️ Credentials missing in .env file.")
        print("Make sure you have set:")
        print(" - DATABRICKS_SERVER_HOSTNAME")
        print(" - DATABRICKS_HTTP_PATH")
        print(" - DATABRICKS_ACCESS_TOKEN")
        return

    print("🔄 Attempting to connect to Databricks...")
    
    try:
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
        
        cursor = connection.cursor()
        
        # Run a simple test query to verify connection
        print("✅ Connection successful! Running test query...")
        cursor.execute("SELECT 1 as test_val")
        result = cursor.fetchall()
        
        if result and result[0][0] == 1:
            print(f"🎉 Success! Databricks SQL Warehouse responded correctly: {result}")
        else:
            print("⚠️ Connected, but query returned unexpected results.")
            
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Connection failed. Error: {e}")

if __name__ == "__main__":
    test_connection()
