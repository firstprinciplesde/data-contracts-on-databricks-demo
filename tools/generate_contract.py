"""Draft an ODCS data contract from a Unity Catalog table: schema, types, nullability, ranges, allowed values, readers.

    contract = generate_contract(spark, "workspace.my_schema.silver_orders", team="analytics", readers={...})
    save_contract(contract, "contracts/generated/silver_orders.yaml")

It is a draft to review, not a finished contract: ranges come from the data it saw, so widen them to what the
business actually allows before publishing.
"""
import yaml
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _odcs_type(dt):
    simple = dt.simpleString()
    if isinstance(dt, T.StringType):
        return "string", "STRING"
    if isinstance(dt, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)):
        return "integer", simple.upper()
    if isinstance(dt, (T.FloatType, T.DoubleType, T.DecimalType)):
        return "number", simple.upper()
    if isinstance(dt, T.BooleanType):
        return "boolean", "BOOLEAN"
    if isinstance(dt, T.DateType):
        return "date", "DATE"
    if isinstance(dt, T.TimestampType):
        return "timestamp", "TIMESTAMP"
    return "string", simple.upper()


def _round(x, up):
    import math
    return float(math.ceil(x) if up else math.floor(x))


def profile_table(spark, table, max_categories=10):
    """One pass of numbers per column: nulls, distinct, min, max (numeric) and the values of small categorical columns."""
    df = spark.table(table)
    aggs = [F.count(F.lit(1)).alias("__rows")]
    for f in df.schema.fields:
        c = F.col(f.name)
        aggs += [F.count(c).alias(f"{f.name}__nonnull"), F.countDistinct(c).alias(f"{f.name}__distinct")]
        if isinstance(f.dataType, (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType)):
            aggs += [F.min(c).alias(f"{f.name}__min"), F.max(c).alias(f"{f.name}__max")]
    row = df.agg(*aggs).collect()[0].asDict()
    prof = {"rows": row["__rows"], "columns": {}}
    for f in df.schema.fields:
        n = f.name
        col = {"nulls": prof["rows"] - row[f"{n}__nonnull"], "distinct": row[f"{n}__distinct"],
               "min": row.get(f"{n}__min"), "max": row.get(f"{n}__max")}
        if isinstance(f.dataType, T.StringType) and 0 < col["distinct"] <= max_categories:
            col["values"] = sorted(r[0] for r in df.select(n).where(F.col(n).isNotNull()).distinct().collect())
        prof["columns"][n] = col
    return prof


def generate_contract(spark, table, team="unknown", readers=None, version="0.1.0", pad=0.5, max_categories=10):
    """readers: {reader_table: [columns of this table it uses]}, from tools.lineage.readers_with_columns."""
    catalog, schema, name = table.split(".")
    df = spark.table(table)
    prof = profile_table(spark, table, max_categories)
    props, quality = [], []
    for f in df.schema.fields:
        stats = prof["columns"][f.name]
        logical, physical = _odcs_type(f.dataType)
        p = {"name": f.name, "logicalType": logical, "physicalType": physical}
        p["required"] = bool((not f.nullable) or stats["nulls"] == 0)
        if stats["distinct"] == prof["rows"] and logical == "string" and prof["rows"] > 1:
            p["unique"] = True
        if logical in ("integer", "number") and stats["min"] is not None:
            lo, hi = float(stats["min"]), float(stats["max"])
            span = (hi - lo) or abs(hi) or 1.0
            lower = _round(lo - pad * span, False)
            if lo >= 0:
                lower = max(lower, 0.0)
            p["logicalTypeOptions"] = {"minimum": lower, "maximum": _round(hi + pad * span, True)}
        p["description"] = f"Draft. Seen in {prof['rows']} rows: {stats['nulls']} nulls, {stats['distinct']} distinct values."
        props.append(p)
        if stats.get("values"):
            quality.append({"id": f"{f.name}_allowed_values", "metric": "invalidValues", "mustBe": 0,
                            "arguments": {"validValues": stats["values"]}, "description": f"Column {f.name}"})
    obj = {"name": name, "logicalType": "object", "physicalType": "table", "properties": props}
    contract = {
        "apiVersion": "v3.2.0", "kind": "DataContract", "id": name.replace("_", "-"), "name": name,
        "version": version, "status": "draft",
        "description": {"purpose": f"Draft contract generated from {table}. Review every range and every required flag."},
        "schema": [obj],
        "team": {"name": team},
        "servers": [{"server": "databricks", "type": "databricks", "environment": "dev", "catalog": catalog, "schema": schema}],
    }
    contract["schema"][0]["quality"] = [{"id": f"{name}_not_empty", "metric": "rowCount", "mustBeGreaterThan": 0}] + quality
    if readers:
        contract["customProperties"] = [
            {"property": "consumers", "value": ", ".join(sorted(readers))},
            {"property": "consumer_columns", "value": {r: sorted(c) for r, c in sorted(readers.items())}},
        ]
    return contract


def save_contract(contract, path):
    with open(path, "w") as fh:
        yaml.safe_dump(contract, fh, sort_keys=False)
    return path
