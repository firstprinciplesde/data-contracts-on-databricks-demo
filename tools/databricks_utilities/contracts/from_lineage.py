"""Draft an ODCS data contract for a table from what its readers actually use.

    from databricks_utilities.contracts import contract_from_lineage, save_contract
    contract = contract_from_lineage("main.sales.orders", team="analytics")
    save_contract(contract, "contracts/orders.yaml")

How the draft is decided:
  * schema, types and descriptions come from Unity Catalog (no Spark needed)
  * a column is `required` when at least one reader uses it, or Unity Catalog says it is NOT NULL
  * a column no reader uses is optional and flagged as a removal candidate
  * the readers go into `customProperties` (`consumers`, and per column `readers`)
  * with a `spark` session it also profiles the data: min and max become a range, small string columns become allowed values

It is a draft to review. Lineage shows who reads a column today, not who will tomorrow.
"""
from __future__ import annotations

import math
import re

import yaml
from databricks.sdk import WorkspaceClient

from databricks_utilities.lineage.finder import Reader, find_readers

_INT = {"tinyint", "smallint", "int", "integer", "bigint", "long", "short", "byte"}
_NUM = {"float", "double", "real"}


def odcs_type(type_text: str) -> tuple[str, str]:
    """Unity Catalog type text to (ODCS logicalType, physicalType)."""
    t = type_text.strip().lower()
    base = re.sub(r"\(.*", "", t)
    if base in _INT:
        return "integer", {"integer": "INT", "long": "BIGINT", "short": "SMALLINT", "byte": "TINYINT"}.get(base, base.upper())
    if base in _NUM or base == "decimal":
        return "number", t.upper()
    if base == "boolean":
        return "boolean", "BOOLEAN"
    if base == "date":
        return "date", "DATE"
    if base.startswith("timestamp"):
        return "timestamp", t.upper()
    if base == "string":
        return "string", "STRING"
    return "string", t.upper()


def _profile(spark, table: str, columns, max_categories: int):
    from pyspark.sql import functions as F

    df = spark.table(table)
    aggs = [F.count(F.lit(1)).alias("__rows")]
    for name, logical in columns:
        c = F.col(f"`{name}`")
        aggs += [F.count(c).alias(f"{name}__nonnull"), F.countDistinct(c).alias(f"{name}__distinct")]
        if logical in ("integer", "number"):
            aggs += [F.min(c).alias(f"{name}__min"), F.max(c).alias(f"{name}__max")]
    row = df.agg(*aggs).collect()[0].asDict()
    out = {"rows": row["__rows"], "columns": {}}
    for name, logical in columns:
        col = {"nulls": out["rows"] - row[f"{name}__nonnull"], "distinct": row[f"{name}__distinct"],
               "min": row.get(f"{name}__min"), "max": row.get(f"{name}__max")}
        if logical == "string" and 0 < col["distinct"] <= max_categories:
            col["values"] = sorted(r[0] for r in df.select(f"`{name}`").where(F.col(f"`{name}`").isNotNull()).distinct().collect())
        out["columns"][name] = col
    return out


def contract_from_lineage(table: str, ws: WorkspaceClient | None = None, spark=None, *, team: str = "unknown",
                          version: str = "0.1.0", readers: list[Reader] | None = None, pad: float = 0.5,
                          max_categories: int = 10) -> dict:
    """Return an ODCS v3 contract (a dict) for `table` (catalog.schema.table).

    ws       WorkspaceClient (default: WorkspaceClient() from the environment or notebook)
    spark    optional SparkSession; adds ranges and allowed values profiled from the data
    readers  pass your own list of Reader to skip the lineage call
    pad      how far past the observed min and max the draft range reaches (0.5 = half the span)
    """
    ws = ws or WorkspaceClient()
    catalog, schema, name = table.split(".")
    info = ws.tables.get(full_name=table)
    columns = sorted(info.columns or [], key=lambda c: c.position or 0)
    readers = readers if readers is not None else find_readers(table, ws)
    readers_by_col: dict[str, list[str]] = {}
    for r in readers:
        for col in r.columns:
            readers_by_col.setdefault(col, []).append(r.table)

    typed = [(c.name, odcs_type(c.type_text)[0]) for c in columns]
    prof = _profile(spark, table, typed, max_categories) if spark is not None else None

    props, quality = [], []
    for c in columns:
        logical, physical = odcs_type(c.type_text)
        used_by = sorted(readers_by_col.get(c.name, []))
        p = {"name": c.name, "logicalType": logical, "physicalType": physical,
             "required": bool(used_by) or c.nullable is False}
        notes = [c.comment] if c.comment else []
        if used_by:
            notes.append(f"Read by {', '.join(used_by)}.")
            p["customProperties"] = [{"property": "readers", "value": ", ".join(used_by)}]
        else:
            notes.append("No reader found in lineage: a candidate to deprecate.")
        if prof:
            s = prof["columns"][c.name]
            if logical in ("integer", "number") and s["min"] is not None:
                lo, hi = float(s["min"]), float(s["max"])
                span = (hi - lo) or abs(hi) or 1.0
                lower = math.floor(lo - pad * span)
                p["logicalTypeOptions"] = {"minimum": float(max(lower, 0) if lo >= 0 else lower),
                                           "maximum": float(math.ceil(hi + pad * span))}
            if s["distinct"] == prof["rows"] and logical == "string" and prof["rows"] > 1:
                p["unique"] = True
            if s.get("values"):
                quality.append({"id": f"{c.name}_allowed_values", "metric": "invalidValues", "mustBe": 0,
                                "arguments": {"validValues": s["values"]}, "description": f"Column {c.name}"})
        p["description"] = " ".join(notes)
        props.append(p)

    obj = {"name": name, "logicalType": "object", "physicalType": "table", "properties": props,
           "quality": [{"id": f"{name}_not_empty", "metric": "rowCount", "mustBeGreaterThan": 0}] + quality}
    contract = {
        "apiVersion": "v3.2.0", "kind": "DataContract", "id": name.replace("_", "-"), "name": name,
        "version": version, "status": "draft",
        "description": {"purpose": f"Draft contract for {table}, generated from lineage. Review before publishing."},
        "schema": [obj],
        "team": {"name": team},
        "servers": [{"server": "databricks", "type": "databricks", "environment": "dev",
                     "catalog": catalog, "schema": schema}],
    }
    if readers:
        contract["customProperties"] = [{"property": "consumers", "value": ", ".join(sorted(r.table for r in readers))}]
    return contract


def to_yaml(contract: dict) -> str:
    return yaml.safe_dump(contract, sort_keys=False)


def save_contract(contract: dict, path: str) -> str:
    with open(path, "w") as fh:
        fh.write(to_yaml(contract))
    return path
