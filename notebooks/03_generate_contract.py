# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Write the contract from the table
# MAGIC `generate_contract(table)` reads the schema, profiles the data and asks lineage who reads the table,
# MAGIC then writes a draft ODCS file to review. Faster than starting from an empty file.

# COMMAND ----------
import os, sys, yaml
from databricks.sdk import WorkspaceClient
ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(os.path.join(ROOT, "tools"))
from databricks_utilities.contracts import contract_from_lineage, save_contract, to_yaml
from databricks_utilities.lineage import find_readers

S = "workspace.vid11_databricks_contracts"
ws = WorkspaceClient()
os.makedirs(os.path.join(ROOT, "contracts", "generated"), exist_ok=True)

# COMMAND ----------
for table in ["orders_landing", "revenue_daily"]:
    full = f"{S}.{table}"
    print("=====", table)
    print("who reads it, and which columns:", find_readers(full, ws))
    contract = contract_from_lineage(full, ws, spark=spark, team="analytics")
    path = save_contract(contract, os.path.join(ROOT, "contracts", "generated", f"{table}.yaml"))
    for p in contract["schema"][0]["properties"]:
        print(f"  {p['name']:14} required={p['required']!s:5} {p['description']}")
    print("saved", path.split("/contracts/")[1])
