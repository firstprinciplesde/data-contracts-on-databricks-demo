# Databricks notebook source
# MAGIC %md
# MAGIC # 01 The API producer
# MAGIC The producer calls the Orders API, checks the batch against its contract, and publishes only if it passes.
# MAGIC Widgets: `batch` picks what the API sends, `strict` picks strict or permissive schema validation,
# MAGIC `contract` picks the contract version to enforce (a file under `contracts/` or `scenarios/`).

# COMMAND ----------
# MAGIC %pip install "databricks-labs-dqx[datacontract]"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.dropdown("batch", "good", ["good", "cents", "extra", "null_currency", "renamed", "drift"])
dbutils.widgets.dropdown("strict", "true", ["true", "false"])
dbutils.widgets.text("contract", "contracts/orders_api.yaml")
dbutils.widgets.text("start_id", "1000")
batch = dbutils.widgets.get("batch")
strict = dbutils.widgets.get("strict") == "true"
contract_rel = dbutils.widgets.get("contract")
start_id = int(dbutils.widgets.get("start_id"))

# COMMAND ----------
import os, sys, yaml
from pyspark.sql import functions as F
ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(os.path.join(ROOT, "producer"))
from orders_api import get_orders

CONTRACT = os.path.join(ROOT, contract_rel)
TABLE = "workspace.vid11_databricks_contracts.orders_landing"
QUARANTINE = "workspace.vid11_databricks_contracts.quarantine_orders"
contract = yaml.safe_load(open(CONTRACT))
contract_cols = [p["name"] for p in contract["schema"][0]["properties"]]

resp = get_orders(cents=(batch == "cents"), extra_field=(batch == "extra"),
                  null_currency=(batch == "null_currency"), renamed=(batch == "renamed"),
                  drift=(batch == "drift"), start_id=start_id)
raw = spark.createDataFrame(resp["orders"])
present = [c for c in contract_cols if c in raw.columns]            # columns the contract expects, in contract order
extra_cols = [c for c in raw.columns if c not in contract_cols]      # fields the contract does not know
df = (raw.select(*present, *extra_cols)
      .withColumn("amount", F.col("amount").cast("decimal(10,2)"))
      .withColumn("created_at", F.to_timestamp("created_at")))
if "quantity" in df.columns:
    df = df.withColumn("quantity", F.col("quantity").cast("int"))     # the API sends JSON numbers; the contract says INT
print(batch, df.count(), "records from the API, columns:", df.columns)
print("contract", contract["version"], "from", contract_rel, "| strict schema:", strict)

# COMMAND ----------
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator

ws = WorkspaceClient()
rules = DQGenerator(workspace_client=ws, spark=spark).generate_rules_from_contract(
    contract_file=CONTRACT, strict_schema_validation=strict)
valid_df, invalid_df = DQEngine(ws).apply_checks_by_metadata_and_split(df, rules)
n_valid, n_invalid = valid_df.count(), invalid_df.count()
print(f"valid {n_valid}, invalid {n_invalid}")

# COMMAND ----------
if n_invalid:
    (invalid_df.withColumn("contract_version", F.lit(contract["version"]))
        .withColumn("batch", F.lit(batch)).withColumn("rejected_at", F.current_timestamp())
        .write.mode("append").option("mergeSchema", "true").saveAsTable(QUARANTINE))
    why = (invalid_df.select(F.explode("_errors").alias("e")).select("e.name")
           .groupBy("name").count().orderBy(F.desc("count")).collect())
    print("rejected because:", [(r["name"], r["count"]) for r in why][:5])
    raise ValueError(f"Batch rejected: {n_invalid} records break the orders_api contract {contract['version']} (see {QUARANTINE})")
# only columns the landing table has are published; fields outside the contract (permissive mode) stop here
table_cols = spark.table(TABLE).columns
publish = [c for c in valid_df.columns if c in table_cols]
valid_df.select(*publish).write.mode("append").saveAsTable(TABLE)
print(f"Published {n_valid} records to {TABLE}, columns {publish}")
