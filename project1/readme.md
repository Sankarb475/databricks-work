# Databricks SQL Query Execution POC

## 📌 Overview
This Proof of Concept (PoC) demonstrates how to:
1. Store SQL queries in a JSON file inside **our Databricks workspace**.
2. Extract these queries dynamically in a Databricks notebook.
3. Execute them on a **client's Databricks SQL Warehouse (compute)** located in a **different Azure account**.

All orchestration, storage, and control happen in **our Databricks environment**. Only the **execution compute** belongs to the client.

---

## ⚙️ Prerequisites
Before running this PoC, ensure you have the following:

- **Client’s Databricks Workspace details**
  - Workspace URL (e.g., `https://adb-<workspace-id>.<region>.azuredatabricks.net`)
  - SQL Warehouse (Compute) ID
  - Personal Access Token (PAT) generated from client’s workspace

- **Our Databricks environment**
  - JSON file containing SQL queries (sample structure below)
  - Notebook with connection logic
  - Secret scope created to securely store the client’s PAT

---

## 📂 Example SQL JSON File
```json
{
  "queries": [
    "SELECT current_date() as today;",
    "SELECT count(*) FROM catalog.schema.table_name;"
  ]
}
