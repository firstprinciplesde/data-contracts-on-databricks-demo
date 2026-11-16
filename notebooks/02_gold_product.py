# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver to gold, enforced when gold is written
# MAGIC The analytics team builds silver, then the gold table `revenue_daily` under its own contract.
# MAGIC The silver-to-gold logic is `pipeline/revenue_daily.py`.
# MAGIC Write, audit, publish: the result is written to a staging table, checked against the contract, and only then written to the gold table.

# COMMAND ----------
# MAGIC %pip install "databricks-labs-dqx[datacontract]"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import os, sys
from pyspark.sql import functions as F
S = "workspace.vid11_databricks_contracts"
ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(ROOT)
from pipeline.revenue_daily import build_revenue_daily

# silver: the four columns the gold job needs (the table already exists from 00_setup)
(spark.table(f"{S}.orders_landing")
      .select("order_id", "customer_id", "amount", "created_at")
      .write.mode("overwrite").saveAsTable(f"{S}.silver_orders"))

# COMMAND ----------
# write: the gold job's result goes into the staging table first
build_revenue_daily(spark.table(f"{S}.silver_orders")).write.mode("overwrite").saveAsTable(f"{S}.revenue_daily_staging")
staged = spark.table(f"{S}.revenue_daily_staging")

# audit: DQX builds the checks from the contract (strict schema first, then the value rules)
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
ws = WorkspaceClient()
rules = DQGenerator(workspace_client=ws, spark=spark).generate_rules_from_contract(
    contract_file=os.path.join(ROOT, "contracts", "revenue_daily.yaml"))
valid_df, invalid_df = DQEngine(ws).apply_checks_by_metadata_and_split(staged, rules)
n_valid, n_invalid = valid_df.count(), invalid_df.count()
print("valid", n_valid, "invalid", n_invalid)
if n_invalid:
    invalid_df.select("order_date", "orders", "revenue", F.explode("_errors").alias("e")) \
        .select("order_date", "orders", "revenue", "e.name", "e.message").show(truncate=False)
    raise ValueError("revenue_daily breaks its contract, not published")

# COMMAND ----------
# write gold: only after the audit passed
staged.write.mode("overwrite").saveAsTable(f"{S}.revenue_daily")
spark.table(f"{S}.revenue_daily").orderBy("order_date").show()
