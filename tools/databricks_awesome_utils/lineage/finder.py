"""Find who reads a table, and which columns each reader uses, from Unity Catalog lineage.

Uses the lineage REST API behind Catalog Explorer, so it needs no Spark and works in notebooks, jobs and plain scripts.
Lineage only knows about reads and writes Databricks has seen (notebooks, jobs, SQL), and it keeps direct readers.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient


@dataclass
class Reader:
    """One table that reads the source table, and the source columns it uses."""

    table: str
    columns: list[str] = field(default_factory=list)


def _client(ws: WorkspaceClient | None) -> WorkspaceClient:
    return ws or WorkspaceClient()


def _short(full_name: str) -> str:
    return full_name.split(".")[-1]


def column_names(table: str, ws: WorkspaceClient | None = None) -> list[str]:
    """Column names of a Unity Catalog table, in order."""
    info = _client(ws).tables.get(full_name=table)
    return [c.name for c in sorted(info.columns or [], key=lambda c: c.position or 0)]


def _column_readers(ws: WorkspaceClient, table: str, column: str) -> list[str]:
    r = ws.api_client.do("GET", "/api/2.0/lineage-tracking/column-lineage",
                         query={"table_name": table, "column_name": column})
    return sorted({c["table_name"] for c in r.get("downstream_cols", []) if c["table_name"] != _short(table)})


def find_readers(table: str, ws: WorkspaceClient | None = None) -> list[Reader]:
    """Readers of `table` (catalog.schema.table) with the columns each one uses.

    >>> find_readers("main.sales.orders")
    [Reader(table='finance_export', columns=['order_date', 'revenue'])]
    """
    ws = _client(ws)
    used: dict[str, set[str]] = {}
    for col in column_names(table, ws):
        for reader in _column_readers(ws, table, col):
            used.setdefault(reader, set()).add(col)
    return [Reader(table=t, columns=sorted(c)) for t, c in sorted(used.items())]
