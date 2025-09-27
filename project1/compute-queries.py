import json
import requests
import time

"""
# --- Databricks Configuration ---
DATABRICKS_HOST = "https://adb-**************.9.azuredatabricks.net"  # Your Databricks workspace URL
DATABRICKS_API_TOKEN = "dapi6ab23aa55b0036981dad94"  # Replace with your access token
WAREHOUSE_ID = "f18b02ff43ec7dbf"  # Replace with your SQL Warehouse ID
"""

DATABRICKS_HOST = "https://dbc-*********.cloud.databricks.com/"  # Your Databricks workspace URL
DATABRICKS_API_TOKEN = "dapiddcdf67578cdb0cbe26fe89add"  # Replace with your access token
WAREHOUSE_ID = "384e930f7b74"  # Replace with your SQL Warehouse ID


# JSON file for queries
DUMMY_JSON_FILE = "queries.json"

# Step 1: Create Dummy JSON File
def create_dummy_json():

    """
    queries = {
        "queries_data": [
            {"id": 1, "query": "SELECT COUNT(*) FROM rsdata.actuarial_dwh.bridge_member_policy;"},
            {"id": 2, "query": "SELECT * FROM rsdata.actuarial_dwh.bridge_member_policy;"},
            {"id": 3, "query": "SELECT * FROM rsdata.actuarial_dwh.fact_utilization;"},
            {"id": 4, "query": "SELECT COUNT(*) FROM rsdata.actuarial_dwh.bridge_member_policy;"},
            {"id": 5, "query": "SELECT COUNT(*) FROM rsdata.actuarial_dwh.bridge_member_policy;"}
        ]
    }
    """

    queries = {
        "queries_data": [
            {"id": 1, "query": "select * from information_schema.catalogs;"}
        ]
    }


    with open(DUMMY_JSON_FILE, "w") as f:
        json.dump(queries, f, indent=4)
    print("Dummy JSON file created successfully!")

# Step 2: Parse JSON File to Extract Queries
def parse_json(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)
    return [{"id": item["id"], "query": item["query"]} for item in data["queries_data"]]

# Step 3: Submit Queries to Databricks and Fetch Results
def execute_queries_on_databricks(queries, databricks_host, databricks_api_token, warehouse_id):
    headers = {
        "Authorization": f"Bearer {databricks_api_token}",
        "Content-Type": "application/json"
    }
    
    results = []
    
    for query in queries:
        query_id = query["id"]
        query_text = query["query"]
        print(f"Submitting Query ID {query_id}: {query_text}")
        
        # Submit the query
        payload = {"statement": query_text, "warehouse_id": warehouse_id}
        response = requests.post(f"{databricks_host}/api/2.0/sql/statements", headers=headers, json=payload)
        
        if response.status_code != 200:
            print(f"Failed to submit query ID {query_id}: {response.text}")
            continue
        
        # Poll Query Status
        statement_id = response.json()["statement_id"]
        query_status = "PENDING"
        result_data = None
        while query_status not in ["SUCCEEDED", "FAILED", "CANCELED"]:
            time.sleep(2)  # Poll every 2 seconds
            status_response = requests.get(f"{databricks_host}/api/2.0/sql/statements/{statement_id}", headers=headers)
            if status_response.status_code != 200:
                print(f"Error checking status for Query ID {query_id}: {status_response.text}")
                break
            
            query_response = status_response.json()
            query_status = query_response["status"]["state"]
            print(f"Query ID {query_id} status: {query_status}")

            # Extract results if the query succeeded
            if query_status == "SUCCEEDED":
                result_data = query_response.get("result", {}).get("data_array", [])
                break

        if query_status == "SUCCEEDED" and result_data:
            results.append({"query_id": query_id, "result": result_data})
        else:
            print(f"Query ID {query_id} failed or returned no results.")

    return results

# Step 4: Main Execution Logic
if __name__ == "__main__":
    create_dummy_json()  # Step 1: Generate the dummy queries.json file
    queries = parse_json(DUMMY_JSON_FILE)  # Step 2: Parse queries from JSON
    print(f"Parsed Queries: {queries}")
    
    # Step 3: Execute queries remotely on Databricks SQL Warehouse
    results = execute_queries_on_databricks(queries, DATABRICKS_HOST, DATABRICKS_API_TOKEN, WAREHOUSE_ID)
    
    # Step 4: Print all results
    print("\nQuery Results:")
    for result in results:
        print(f"Query ID {result['query_id']} Result:", result["result"])

  
