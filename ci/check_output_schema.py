"""CI step: what the jobs declare they write (pipeline/schemas.py) must match the contracts. No data, no workspace.

Compares, per contract: the columns and their order, the type of each column, and required vs NOT NULL.
Pattern taken from a pull-request gate that compares a contract's columns and types with a code-side schema
(github.com/gbholdings/straterai-gates); applying it to a Spark job's declared output is our own addition.
"""
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))
from pipeline.schemas import TABLES, parse_ddl  # noqa: E402

PAIRS = {"contracts/orders_api.yaml": "orders_landing", "contracts/revenue_daily.yaml": "revenue_daily"}

problems = []
for contract_file, table in PAIRS.items():
    contract = yaml.safe_load(open(contract_file))
    want = [(p["name"], p["physicalType"].upper().replace(" ", ""), not p.get("required", False))
            for p in contract["schema"][0]["properties"]]
    have = parse_ddl(TABLES[table])
    print(f"{contract_file} (v{contract['version']}) vs pipeline/schemas.py:{table}")
    for name, typ, nullable in want:
        got = next((h for h in have if h[0] == name), None)
        if got is None:
            problems.append(f"{table}: the contract has column {name}, the job does not write it")
        elif got[1] != typ:
            problems.append(f"{table}.{name}: the contract says {typ}, the job writes {got[1]}")
        elif got[2] != nullable:
            problems.append(f"{table}.{name}: the contract says {'optional' if nullable else 'required'}, "
                            f"the job writes it as {'nullable' if got[2] else 'NOT NULL'}")
    for name, _, _ in have:
        if name not in [w[0] for w in want]:
            problems.append(f"{table}: the job writes column {name}, the contract does not have it")
    if [w[0] for w in want if w[0] in [h[0] for h in have]] != [h[0] for h in have if h[0] in [w[0] for w in want]]:
        problems.append(f"{table}: the columns are in a different order than the contract")

if problems:
    print("\nNOT MATCHING:\n- " + "\n- ".join(problems))
    sys.exit(1)
print("ok: the declared output matches both contracts")
