#============================================================================================
# 4) Final code for running the "INSERT" queries at client's side (- POSTGRES connection)
#============================================================================================


import json
import requests
import time
import logging
from typing import List, Dict, Any, Optional

# --- 1. Configuration and Logger Setup ---

# Configure the logger
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# --- Databricks Configuration ---
DATABRICKS_HOST = "https://adb-28356628782***.9.azuredatabricks.net"  
DATABRICKS_API_TOKEN = "dapid2a2ede319166ca36eecc6bc559e"  
WAREHOUSE_ID = "f18b02ff43***7dbf"  
DUMMY_JSON_FILE = "queries.json"


# --- 2. Utility Functions ---

def create_json():

    """Generates the queries.json file"""
    queries = {
        "queries_data": [
            # DML query 1: INSERT into dim_employer_group
            {"id": 1, "target_table": "rsdata.actuarial_dwh.dim_employer_group", "query": "INSERT INTO actuarial_dwh.dim_employer_group ( group_number, group_name, sic_code, industry ) SELECT DISTINCT src.group_number, src.group_name, src.employer_sic_code, src.industry_desc FROM landingzone.stg_src_member_enrollment src WHERE src.group_number IS NOT NULL AND NOT EXISTS ( SELECT 1 FROM actuarial_dwh.dim_employer_group deg WHERE deg.group_number = src.group_number );"},
            # DML query 2: INSERT into dim_member
            {"id": 2, "target_table": "rsdata.actuarial_dwh.dim_member", "query": "INSERT INTO actuarial_dwh.dim_member ( member_id_nat, first_name, last_name, dob, gender, effective_date, termination_date ) SELECT DISTINCT src.member_id, src.first_name, src.last_name, src.date_of_birth, CASE WHEN UPPER(src.gender) IN ('F', 'FEMALE') THEN 'F' WHEN UPPER(src.gender) IN ('M', 'MALE') THEN 'M' WHEN UPPER(src.gender) IN ('O', 'OTHER') THEN 'O' ELSE NULL END AS gender, src.coverage_effective_date, src.coverage_termination_date FROM landingzone.stg_src_member_enrollment src WHERE src.member_id IS NOT NULL AND NOT EXISTS ( SELECT 1 FROM actuarial_dwh.dim_member dm WHERE dm.member_id_nat = src.member_id );"},
            # Non-DML query 3: SELECT (Used to test non-DML queries do not get counts)
            {"id": 3, "target_table": "rsdata.actuarial_dwh.dim_broker", "query": "INSERT INTO actuarial_dwh.dim_policy ( policy_number, policy_status, inception_date, expiration_date, product_key, employer_group_key, rating_plan_key ) SELECT DISTINCT src.policy_number, COALESCE(CAST(NULL AS STRING), 'Active') AS policy_status, src.coverage_effective_date AS inception_date, src.coverage_termination_date AS expiration_date, dp.product_key, deg.employer_group_key, drp.rating_plan_key FROM landingzone.stg_src_member_enrollment src LEFT JOIN actuarial_dwh.dim_product dp ON dp.product_code = src.product_code LEFT JOIN actuarial_dwh.dim_employer_group deg ON deg.group_number = src.group_number LEFT JOIN actuarial_dwh.dim_rating_plan drp ON drp.rating_plan_code = COALESCE(CAST(NULL AS STRING), 'COMMUNITY') WHERE src.policy_number IS NOT NULL AND NOT EXISTS ( SELECT 1 FROM actuarial_dwh.dim_policy dpol WHERE dpol.policy_number = src.policy_number );"},
            # DML query 4: INSERT (another example)
            {"id": 4, "target_table": "rsdata.actuarial_dwh.dim_broker_sankar_test_1", "query": "INSERT INTO rsdata.actuarial_dwh.dim_broker_sankar_test_1 (broker_key, broker_id_nat, broker_name, brokerage_firm) SELECT broker_key, broker_id_nat, broker_name, brokerage_firm FROM rsdata.actuarial_dwh.dim_broker;"},
        ]
    }

    try:
        with open(DUMMY_JSON_FILE, "w") as f:
            json.dump(queries, f, indent=4)
        logger.info(f"Dummy JSON file '{DUMMY_JSON_FILE}' created successfully with corrected queries.")
    except Exception as e:
        logger.error(f"Error creating dummy JSON file: {e}")
        raise

def parse_json(file_path: str) -> List[Dict[str, Any]]:
    """Parses JSON file to extract query ID, target_table, and query text."""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        logger.info(f"Successfully parsed queries from {file_path}.")
        return [{"id": item["id"], "target_table": item.get("target_table"), "query": item["query"]} for item in data["queries_data"]]
    except FileNotFoundError:
        logger.error(f"Error: JSON file not found at {file_path}")
        return []
    except json.JSONDecodeError:
        logger.error(f"Error: Failed to decode JSON from {file_path}. Check file structure.")
        return []
    except Exception as e:
        logger.error(f"Unexpected error during JSON parsing: {e}")
        return []


def submit_and_poll_statement(query_text: str, query_id: str, databricks_host: str, databricks_api_token: str, warehouse_id: str) -> Optional[Dict[str, Any]]:
    """
    Submits a single SQL statement to Databricks and polls for completion.
    Returns the final response JSON or None on failure.
    """
    headers = {
        "Authorization": f"Bearer {databricks_api_token}",
        "Content-Type": "application/json"
    }
    api_url = f"{databricks_host}/api/2.0/sql/statements"
    
    # 1. Submit the query
    payload = {"statement": query_text, "warehouse_id": warehouse_id}
    statement_id = None
    
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=10)
        response.raise_for_status() 
        response_json = response.json()
        statement_id = response_json.get("statement_id")
        
        if not statement_id:
            logger.error(f"Query {query_id} submission successful but no statement_id found: {response_json}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Network/API error submitting Query {query_id}: {e}")
        return None
    
    # 2. Poll Query Status
    query_status = "PENDING"
    
    while query_status not in ["SUCCEEDED", "FAILED", "CANCELED"]:
        try:
            time.sleep(2)  # Poll every 2 seconds
            status_url = f"{api_url}/{statement_id}"
            status_response = requests.get(status_url, headers=headers, timeout=10)
            status_response.raise_for_status()
            
            query_response = status_response.json()
            query_status = query_response["status"]["state"]
            
            if query_status in ["SUCCEEDED", "FAILED", "CANCELED"]:
                return query_response

        except requests.exceptions.RequestException as e:
            logger.error(f"Network/API error checking status for Query {query_id}: {e}")
            return None
    
    return None # Should be caught by the above loop, but as a fallback


def execute_count_query(table_name: str, databricks_host: str, databricks_api_token: str, warehouse_id: str, count_type: str) -> Optional[int]:
    """Runs a SELECT COUNT(*) query and extracts the integer result."""
    count_query_text = f"SELECT COUNT(*) FROM {table_name};"
    query_id_label = f"COUNT({count_type})"

    logger.info(f"  -> Running {count_type} count query on {table_name}...")
    
    response = submit_and_poll_statement(count_query_text, query_id_label, databricks_host, databricks_api_token, warehouse_id)

    if response and response["status"]["state"] == "SUCCEEDED":
        try:
            # Databricks API returns results in the data_array field for SELECT queries
            count_value = response["result"]["data_array"][0][0]
            logger.info(f"  -> {count_type} count on {table_name}: {count_value}")
            return int(count_value)
        except (KeyError, IndexError, TypeError, ValueError) as e:
            logger.error(f"Failed to parse count result for {table_name}: {e}")
            return None
    else:
        logger.error(f"Failed to execute {count_type} count query on {table_name}. Check permissions or table existence.")
        # Log the specific Databricks error if available
        if response and response.get("status", {}).get("error"):
            error_message = response["status"]["error"]["message"]
            logger.error(f"Databricks error: {error_message}")
        return None


def execute_queries_on_databricks(queries: List[Dict[str, Any]], databricks_host: str, databricks_api_token: str, warehouse_id: str) -> List[Dict[str, Any]]:
    """Submits and polls SQL queries on the Databricks SQL Warehouse."""
    
    results = []
    
    for query_item in queries:
        query_id = query_item["id"]
        target_table = query_item.get("target_table")
        query_text = query_item["query"]
        logger.info(f"\n--- Processing Query ID {query_id} ---")
        logger.debug(f"Query Text: {query_text}")
        
        pre_count = None
        
        # 1. Execute PRE-COUNT if target_table is provided and the query is ONLY INSERT query and not SELECT
        if target_table and query_text.strip().upper().startswith("INSERT"):
            logger.info(f"DML detected. Preparing to run pre- and post-counts on {target_table}.")
            pre_count = execute_count_query(target_table, databricks_host, databricks_api_token, warehouse_id, "PRE")
        
        # 2. Submit and poll the main query
        query_response = submit_and_poll_statement(query_text, str(query_id), databricks_host, databricks_api_token, warehouse_id)
        
        query_status = query_response["status"]["state"] if query_response else "FAILED"
        result_data = None
        
        if query_status == "SUCCEEDED":
            # Extract row count for DML statements from the API status field
            row_count_api = query_response.get("status", {}).get("row_count")
            result_data = f"SUCCEEDED. API Row Count: {row_count_api}" if row_count_api is not None else "SUCCEEDED. No row count provided by API."
            
            # 3. Execute POST-COUNT if pre-count was successful
            if pre_count is not None:
                post_count = execute_count_query(target_table, databricks_host, databricks_api_token, warehouse_id, "POST")
                
                if post_count is not None:
                    inserted_rows = post_count - pre_count
                    result_data += f" | Pre-Count: {pre_count}, Post-Count: {post_count}, Inserted Rows: {inserted_rows}"
                    logger.info(f"Query ID {query_id} - SUMMARY: {inserted_rows} row(s) inserted into {target_table}.")
                    
            logger.info(f"Query ID {query_id} completed. Result: {result_data}")

        elif query_status in ["FAILED", "CANCELED"]:
            error_message = query_response.get("status", {}).get("error", {}).get("message", "Unknown error") if query_response else "Failed during submission."
            result_data = f"{query_status}. Error: {error_message}"
            logger.error(f"Query ID {query_id} {query_status}. Error: {error_message}")
            
        else:
             # This handles cases where submit_and_poll_statement returned None
            result_data = "FAILED. Submission or polling failed."


        results.append({"query_id": query_id, "result": result_data})

    return results

# --- 3. Main Execution Logic ---
if __name__ == "__main__":
    try:
        # Step 1: Generate the dummy queries.json file
        create_json() 
        
        # Step 2: Parse queries from JSON
        # Updated parse_json to include target_table
        queries = parse_json(DUMMY_JSON_FILE)
        logger.info(f"Parsed {len(queries)} queries.")
        
        if not queries:
            logger.critical("No queries parsed. Exiting program.")
        else:
            # Step 3: Execute queries remotely on Databricks SQL Warehouse
            results = execute_queries_on_databricks(queries, DATABRICKS_HOST, DATABRICKS_API_TOKEN, WAREHOUSE_ID)
            
            # Step 4: Print final results summary
            logger.info("\n" + "="*40)
            logger.info("FINAL EXECUTION RESULTS SUMMARY")
            logger.info("="*40)
            for result in results:
                logger.info(f"Query ID {result['query_id']}: {result['result']}")

    except Exception as final_e:
        logger.critical(f"A critical error occurred in the main execution block: {final_e}")
        
