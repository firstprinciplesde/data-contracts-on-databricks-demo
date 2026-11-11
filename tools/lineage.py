"""Readers and what they use, from Unity Catalog lineage, and the readers a contract change would break.

The lineage REST API is the one Catalog Explorer uses. (On Free Edition the system.access lineage tables stayed empty.)
"""
import re

from databricks.sdk import WorkspaceClient


def _short(table_full):
    return table_full.split(".")[-1]


def readers_of(ws: WorkspaceClient, table_full):
    r = ws.api_client.do("GET", "/api/2.0/lineage-tracking/table-lineage",
                         query={"table_name": table_full, "include_entity_lineage": True})
    names = {d["tableInfo"]["name"] for d in r.get("downstreams", []) if "tableInfo" in d}
    return sorted(names - {_short(table_full)})


def column_readers(ws: WorkspaceClient, table_full, column):
    r = ws.api_client.do("GET", "/api/2.0/lineage-tracking/column-lineage",
                         query={"table_name": table_full, "column_name": column})
    return sorted({f'{c["table_name"]}.{c["name"]}' for c in r.get("downstream_cols", [])
                   if c["table_name"] != _short(table_full)})


def readers_with_columns(ws: WorkspaceClient, table_full, columns):
    """{reader table: [columns of table_full it uses]}: what each reader expects from this table."""
    out = {}
    for col in columns:
        for reader_col in column_readers(ws, table_full, col):
            out.setdefault(reader_col.split(".")[0], set()).add(col)
    return {r: sorted(c) for r, c in out.items()}


def check_consumers(contract, found):
    """Compare the contract's consumers list with the readers lineage found."""
    declared = next((p["value"] for p in contract.get("customProperties", []) if p["property"] == "consumers"), "")
    declared = {x.strip() for x in str(declared).split(",") if x.strip()}
    found = set(found)
    return {"declared_not_seen": sorted(declared - found), "seen_not_declared": sorted(found - declared)}


def impact_of_change(ws: WorkspaceClient, table_full, old_contract_path, new_contract_path, perspective="consumer"):
    """Run the contract breaking check and name the readers each ERROR would hit."""
    from datacontract.data_contract import DataContract

    result = DataContract(data_contract_file=old_contract_path).breaking(
        DataContract(data_contract_file=new_contract_path), perspective=perspective)
    impacts = []
    for e in result.entries:
        if str(e.level.value).lower() != "error":
            continue
        m = re.search(r"properties\.([^.]+)", e.path)
        col = m.group(1) if m else None
        impacts.append({"change": e.message, "path": e.path, "column": col,
                        "readers": column_readers(ws, table_full, col) if col else []})
    return {"breaking": result.is_breaking, "errors": impacts}
