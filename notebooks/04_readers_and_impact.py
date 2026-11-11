# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Who reads this, and what do they expect
# MAGIC Lineage gives the readers of a table and the columns each one uses. The contract says who is supposed to read it.
# MAGIC `impact_of_change` takes a contract change and names the readers it would break.

# COMMAND ----------
# MAGIC %pip install datacontract-cli

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import os, sys, yaml
from databricks.sdk import WorkspaceClient
ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(os.path.join(ROOT, "tools"))
from lineage import readers_of, readers_with_columns, check_consumers, impact_of_change

S = "workspace.vid11_databricks_contracts"
ws = WorkspaceClient()
load = lambda rel: yaml.safe_load(open(os.path.join(ROOT, rel)))

# COMMAND ----------
# MAGIC %md
# MAGIC ## What each reader uses, and whether the contract knows about it

# COMMAND ----------
for table, contract_rel in [("orders_landing", "contracts/orders_api.yaml"), ("revenue_daily", "contracts/revenue_daily.yaml")]:
    full = f"{S}.{table}"
    cols = spark.table(full).columns
    print("=====", table)
    print("what each reader uses:", readers_with_columns(ws, full, cols))
    print("contract vs lineage:", check_consumers(load(contract_rel), readers_of(ws, full)))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Which readers would this change break

# COMMAND ----------
cases = [
    ("orders_landing", "scenarios/03b-drop-remove/orders_api.yaml", "Change 3: remove channel"),
    ("orders_landing", "scenarios/04b-rename-direct/orders_api.yaml", "Change 4: rename currency in place"),
    ("orders_landing", "scenarios/07-retire-amount/orders_api.yaml", "Change 6: remove amount"),
]
for table, new_rel, label in cases:
    res = impact_of_change(ws, f"{S}.{table}", os.path.join(ROOT, "contracts/orders_api.yaml"), os.path.join(ROOT, new_rel))
    print("=====", label, "| breaking:", res["breaking"])
    for e in res["errors"]:
        print("  ", e["change"], "| readers:", e["readers"] or "none, safe to release")
