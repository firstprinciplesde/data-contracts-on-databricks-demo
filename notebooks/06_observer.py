# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Beyond the contract
# MAGIC The contract's own fields choose the tests: numbers get a mean-shift check, fields with allowed values get a
# MAGIC distribution check, required fields get a null-rate check, `rowCount` gives volume, the SLA gives freshness.
# MAGIC A batch can pass every contract rule and still be different.

# COMMAND ----------
dbutils.widgets.text("baseline_where", "CAST(order_id AS INT) BETWEEN 1001 AND 3000")
dbutils.widgets.text("control_where", "CAST(order_id AS INT) BETWEEN 3001 AND 5000")
dbutils.widgets.text("current_where", "CAST(order_id AS INT) BETWEEN 5001 AND 7000")

# COMMAND ----------
import os, sys, yaml, json
from datetime import datetime
from decimal import Decimal
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "tools")))
from generate_contract import generate_contract
from observer import derive_checks, compute_stats, compare, last_commit_age_hours

ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
S = "workspace.vid11_databricks_contracts"
TABLE = f"{S}.orders_landing"
drafted = generate_contract(spark, TABLE)                       # allowed values come from the data it saw
written = yaml.safe_load(open(os.path.join(ROOT, "contracts", "orders_api.yaml")))
checks = derive_checks(drafted) + [c for c in derive_checks(written) if c["test"] == "freshness"]
print("tests derived from the contract:")
for c in checks:
    print("  ", c["test"], c["column"] or "(table)")

# COMMAND ----------
base = compute_stats(spark, TABLE, drafted, checks, dbutils.widgets.get("baseline_where"))
control = compute_stats(spark, TABLE, drafted, checks, dbutils.widgets.get("control_where"))
current = compute_stats(spark, TABLE, drafted, checks, dbutils.widgets.get("current_where"))
age = last_commit_age_hours(spark, TABLE)

def show(label, cur):
    print("=====", label, f"({cur['rows']} rows)")
    for f in compare(base, cur, checks, freshness_hours=age):
        print(f"  {f['status']:4} {f['check']:11} {str(f['column'] or ''):12} {f['detail']}")
    return compare(base, cur, checks, freshness_hours=age)

show("control batch, same shape as the baseline", control)
findings = show("current batch, passes every contract rule", current)

# COMMAND ----------
spark.createDataFrame([(datetime.utcnow().isoformat(), json.dumps(base), json.dumps(current), json.dumps(findings))],
                      "run_at string, baseline string, current string, findings string") \
    .write.mode("append").saveAsTable(f"{S}.obs_stats")
print("saved to", f"{S}.obs_stats")
print("FLAGS:", sum(f["status"] == "FLAG" for f in findings))
