"""CI step: every change scenario in scenarios/ is checked the way a real PR would be.
For each scenario folder: `datacontract breaking` (both views) against scenarios/00-baseline/orders_api.yaml (the frozen current contract).
This needs no data and no workspace. What happens to real data is shown on Databricks.
The expected levels are in each folder's expect.json (`--record` rewrites them from what actually happens)."""
import json
import re
import subprocess
import sys
from pathlib import Path

RECORD = "--record" in sys.argv
BASE = "scenarios/00-baseline/orders_api.yaml"  # frozen 1.0.0, so a PR that edits the live contract cannot move the baseline


def run(*cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, re.sub(r"\x1b\[[0-9;]*m", "", p.stdout + p.stderr)


def level(code, out):
    return "ERROR" if code != 0 else ("WARNING" if re.search(r"\[ \d+ Warning", out) else "INFO")


failures = []
print(f"{'scenario':24} {'release':18} {'consumer':9} {'producer':9}")
for folder in sorted(Path("scenarios").iterdir()):
    if not folder.is_dir() or not (folder / "expect.json").exists():
        continue
    exp = json.loads((folder / "expect.json").read_text())
    new = str(folder / "orders_api.yaml")
    got = {"breaking": {}}
    for view in ("consumer", "producer"):
        got["breaking"][view] = level(*run("datacontract", "breaking", BASE, new, "--perspective", view))
    print(f"{folder.name:24} {exp['release']:18} {got['breaking']['consumer']:9} {got['breaking']['producer']:9}")
    if RECORD:
        exp["breaking"] = got["breaking"]
        (folder / "expect.json").write_text(json.dumps(exp, indent=2) + "\n")
    elif got["breaking"] != exp["breaking"]:
        failures.append(f"{folder.name}: expected {exp['breaking']}, got {got['breaking']}")

if failures:
    print("\nMISMATCH:\n" + "\n".join(failures))
    sys.exit(1)
