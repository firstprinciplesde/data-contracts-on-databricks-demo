# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup
# MAGIC Creates the schema, the landing Volume and every table the jobs write into, from the schemas declared in
# MAGIC `pipeline/schemas.py`. The same declarations are what CI compares with the contracts.

# COMMAND ----------
import os, sys
ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(ROOT)
from pipeline.schemas import TABLES

CATALOG, SCHEMA = "workspace", "vid11_databricks_contracts"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")

# COMMAND ----------
for name, ddl in TABLES.items():
    (spark.createDataFrame([], ddl.strip())
          .write.format("delta").mode("ignore").saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Reset (optional)
# MAGIC Uncomment to start the demo from empty tables.

# COMMAND ----------
# for t in [*TABLES, "quarantine_orders", "obs_stats"]:
#     spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{t}")
