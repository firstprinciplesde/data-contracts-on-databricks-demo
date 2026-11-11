"""Checks beyond the contract: rules and statistical tests derived from the contract's own fields.

    checks = derive_checks(contract)                       # which test applies to which field, read from the contract
    base = compute_stats(spark, table, contract, where=...)
    now = compute_stats(spark, table, contract, where=...)
    findings = compare(base, now, checks)

The contract picks the tests:
  number or integer field   -> mean shift in standard errors (flag above 2), null rate
  field with allowed values -> distribution shift (PSI: above 0.1 moderate, above 0.25 significant)
  required field            -> null rate
  rowCount rule             -> volume against the baseline
  slaProperties frequency   -> freshness from the table's last commit
Thresholds are the common vendor defaults, not standards.
"""
import math
import re
from datetime import datetime, timezone

from pyspark.sql import functions as F

PSI_MODERATE, PSI_SIGNIFICANT, SE_LIMIT, NULL_JUMP, VOLUME_RATIO = 0.1, 0.25, 2.0, 0.05, 0.5


def _allowed_values(contract, column):
    for q in contract["schema"][0].get("quality", []):
        if q.get("metric") == "invalidValues" and q.get("arguments", {}).get("validValues") and q.get("description") == f"Column {column}":
            return q["arguments"]["validValues"]
    for p in contract["schema"][0]["properties"]:
        if p["name"] == column:
            for q in p.get("quality", []):
                if q.get("metric") == "invalidValues" and q.get("arguments", {}).get("validValues"):
                    return q["arguments"]["validValues"]
    return None


def derive_checks(contract, categorical_columns=()):
    """Which statistical test applies to which field, read from the contract."""
    checks = []
    for p in contract["schema"][0]["properties"]:
        n = p["name"]
        if p.get("deprecated"):
            continue
        if p["logicalType"] in ("number", "integer"):
            checks.append({"column": n, "test": "mean_shift"})
        if p["logicalType"] == "string" and (_allowed_values(contract, n) or n in categorical_columns):
            checks.append({"column": n, "test": "psi"})
        if p.get("required"):
            checks.append({"column": n, "test": "null_rate"})
    if any(q.get("metric") == "rowCount" for q in contract["schema"][0].get("quality", [])):
        checks.append({"column": None, "test": "volume"})
    for sla in contract.get("slaProperties", []):
        if sla.get("property") == "frequency":
            checks.append({"column": None, "test": "freshness", "max_hours": float(sla["value"]) * (24 if sla.get("unit", "d") == "d" else 1)})
    return checks


def compute_stats(spark, table, contract, checks, where="1=1"):
    df = spark.table(table).where(where)
    stats = {"rows": df.count(), "columns": {}}
    numeric = {c["column"] for c in checks if c["test"] == "mean_shift"}
    cats = {c["column"] for c in checks if c["test"] == "psi"}
    nulls = {c["column"] for c in checks if c["test"] == "null_rate"} | numeric
    aggs = []
    for n in numeric:
        aggs += [F.mean(n).alias(f"{n}__mean"), F.stddev(n).alias(f"{n}__std")]
    for n in nulls:
        aggs.append((F.sum(F.col(n).isNull().cast("int")) / F.count(F.lit(1))).alias(f"{n}__null_rate"))
    row = df.agg(*aggs).collect()[0].asDict() if aggs else {}
    for n in numeric | nulls:
        stats["columns"][n] = {k.split("__")[1]: (float(v) if v is not None else None)
                               for k, v in row.items() if k.startswith(n + "__")}
    for n in cats:
        dist = {r[0]: r[1] for r in df.groupBy(n).count().collect()}
        total = sum(dist.values()) or 1
        stats["columns"].setdefault(n, {})["dist"] = {str(k): v / total for k, v in dist.items()}
    return stats


def last_commit_age_hours(spark, table):
    ts = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]["timestamp"]
    now = datetime.now(timezone.utc)
    ts = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
    return (now - ts).total_seconds() / 3600


def psi(base, cur, eps=1e-4):
    keys = set(base) | set(cur)
    return sum((cur.get(k, 0) + eps - (base.get(k, 0) + eps)) * math.log((cur.get(k, 0) + eps) / (base.get(k, 0) + eps)) for k in keys)


def compare(base, cur, checks, freshness_hours=None):
    """Return [{check, column, status, detail}], status ok or FLAG."""
    out = []
    for c in checks:
        n, t = c["column"], c["test"]
        if t == "mean_shift":
            b, k = base["columns"][n], cur["columns"][n]
            se = (b["std"] or 0) / math.sqrt(max(cur["rows"], 1)) or 1e-9
            z = (k["mean"] - b["mean"]) / se
            out.append({"check": t, "column": n, "status": "FLAG" if abs(z) > SE_LIMIT else "ok",
                        "detail": f"mean {b['mean']:.2f} -> {k['mean']:.2f} ({z:+.1f} standard errors, limit {SE_LIMIT})"})
        elif t == "psi":
            v = psi(base["columns"][n]["dist"], cur["columns"][n]["dist"])
            level = "significant" if v > PSI_SIGNIFICANT else "moderate" if v > PSI_MODERATE else "minimal"
            out.append({"check": t, "column": n, "status": "FLAG" if v > PSI_MODERATE else "ok", "detail": f"PSI {v:.3f} ({level})"})
        elif t == "null_rate":
            b, k = base["columns"][n]["null_rate"] or 0.0, cur["columns"][n]["null_rate"] or 0.0
            out.append({"check": t, "column": n, "status": "FLAG" if k - b > NULL_JUMP else "ok", "detail": f"null rate {b:.3f} -> {k:.3f}"})
        elif t == "volume":
            r = cur["rows"] / max(base["rows"], 1)
            out.append({"check": t, "column": None, "status": "FLAG" if abs(r - 1) > VOLUME_RATIO else "ok",
                        "detail": f"{base['rows']} -> {cur['rows']} rows"})
        elif t == "freshness" and freshness_hours is not None:
            out.append({"check": t, "column": None, "status": "FLAG" if freshness_hours > c["max_hours"] else "ok",
                        "detail": f"last commit {freshness_hours:.1f} h ago, SLA {c['max_hours']:.0f} h"})
    return out
