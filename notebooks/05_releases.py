# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Releases
# MAGIC The changes that need more than one step, carried to the end on the real tables:
# MAGIC widen `quantity`, rename `currency`, drop `channel`, and the cents change (`amount` to `amount_cents`).
# MAGIC Before each removal, ask lineage who reads the column (tools/lineage.py).

# COMMAND ----------
import os, sys
from databricks.sdk import WorkspaceClient
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "tools")))
from lineage import readers_of, column_readers

S = "workspace.vid11_databricks_contracts"
ws = WorkspaceClient()

print("readers of silver_orders:", readers_of(ws, f"{S}.silver_orders"))
print("readers of revenue_daily:", readers_of(ws, f"{S}.revenue_daily"))
print("readers of landing.channel:", column_readers(ws, f"{S}.orders_landing", "channel"))
print("readers of landing.amount:", column_readers(ws, f"{S}.orders_landing", "amount"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 · Widen a type (minor): quantity INT to BIGINT

# COMMAND ----------
spark.sql(f"ALTER TABLE {S}.orders_landing SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
spark.sql(f"ALTER TABLE {S}.orders_landing ALTER COLUMN quantity TYPE BIGINT")
print(spark.table(f"{S}.orders_landing").schema["quantity"].dataType)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 · Rename (expand, copy, remove): currency to currency_code

# COMMAND ----------
for t in ["orders_landing"]:
    spark.sql(f"""ALTER TABLE {S}.{t} SET TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
spark.sql(f"ALTER TABLE {S}.orders_landing ADD COLUMNS (currency_code STRING)")
spark.sql(f"UPDATE {S}.orders_landing SET currency_code = currency WHERE currency_code IS NULL")
print("both columns filled:", spark.sql(f"SELECT COUNT(*) c, COUNT(currency) o, COUNT(currency_code) n FROM {S}.orders_landing").collect())
spark.sql(f"ALTER TABLE {S}.orders_landing DROP COLUMN currency")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 · Drop a column (deprecate, check readers, remove): channel

# COMMAND ----------
assert not column_readers(ws, f"{S}.orders_landing", "channel"), "someone still reads channel"
spark.sql(f"ALTER TABLE {S}.orders_landing DROP COLUMN channel")
print(spark.table(f"{S}.orders_landing").columns)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 · Change a meaning: 1.1.0 adds amount_cents next to amount (minor), the readers move, 2.0.0 drops amount (major)

# COMMAND ----------
for t in ["orders_landing", "silver_orders"]:
    spark.sql(f"ALTER TABLE {S}.{t} ADD COLUMNS (amount_cents BIGINT)")
    spark.sql(f"UPDATE {S}.{t} SET amount_cents = CAST(ROUND(amount * 100) AS BIGINT) WHERE amount_cents IS NULL")
print(spark.sql(f"SELECT COUNT(*) AS rows, COUNT(amount_cents) AS with_cents FROM {S}.silver_orders").collect())

# COMMAND ----------
# The analytics team moves its own table to amount_cents. The gold contract does not change,
# so nothing downstream of gold is touched.
before = spark.table(f"{S}.revenue_daily").schema.simpleString()
spark.sql(f"""CREATE OR REPLACE TABLE {S}.revenue_daily AS
SELECT CAST(created_at AS DATE) AS order_date, COUNT(*) AS orders, CAST(SUM(amount_cents) / 100 AS DECIMAL(18,2)) AS revenue
FROM {S}.silver_orders GROUP BY CAST(created_at AS DATE)""")
assert before == spark.table(f"{S}.revenue_daily").schema.simpleString(), "revenue_daily contract changed"
print("revenue_daily schema unchanged:", before)

# COMMAND ----------
spark.sql(f"""ALTER TABLE {S}.silver_orders SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
for t in ["orders_landing", "silver_orders"]:
    spark.sql(f"ALTER TABLE {S}.{t} DROP COLUMN amount")
print(spark.table(f"{S}.orders_landing").columns)
print(spark.table(f"{S}.silver_orders").columns)
spark.table(f"{S}.revenue_daily").orderBy("order_date").show()
