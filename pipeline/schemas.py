"""What each job writes, declared in code (Spark DDL strings).

Three things use these and nothing else:
  - notebooks/00_setup creates every table from them,
  - the jobs write into those tables,
  - ci/check_output_schema.py compares them with the contracts on every pull request, before anything runs.
"""

ORDERS_LANDING = """
  order_id STRING NOT NULL, customer_id STRING NOT NULL, amount DECIMAL(10,2) NOT NULL,
  created_at TIMESTAMP NOT NULL, quantity INT, currency STRING, channel STRING"""

SILVER_ORDERS = """
  order_id STRING NOT NULL, customer_id STRING NOT NULL, amount DECIMAL(10,2) NOT NULL, created_at TIMESTAMP NOT NULL"""

REVENUE_DAILY = """
  order_date DATE NOT NULL, orders BIGINT NOT NULL, revenue DECIMAL(18,2) NOT NULL"""

# table name -> DDL, in the order setup creates them
TABLES = {
    "orders_landing": ORDERS_LANDING,
    "silver_orders": SILVER_ORDERS,
    "revenue_daily": REVENUE_DAILY,
    "revenue_daily_staging": REVENUE_DAILY,
}


def parse_ddl(ddl):
    """[(column, TYPE, nullable)] from a DDL string. No Spark needed, so CI can run it."""
    cols, depth, cur = [], 0, ""
    for ch in ddl.replace("\n", " "):
        depth += ch == "("
        depth -= ch == ")"
        if ch == "," and depth == 0:
            cols.append(cur)
            cur = ""
        else:
            cur += ch
    cols.append(cur)
    out = []
    for c in (x.strip() for x in cols if x.strip()):
        nullable = not c.upper().endswith("NOT NULL")
        c = c[: -len("NOT NULL")].strip() if not nullable else c
        name, typ = c.split(None, 1)
        out.append((name, typ.strip().upper().replace(" ", ""), nullable))
    return out
