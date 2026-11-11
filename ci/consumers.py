"""CI step: for each contract, print who owns it and who consumes it, so the PR shows who a change affects."""
import sys
from pathlib import Path

import yaml

lines = ["## Contracts in this PR", "", "| Contract | Version | Owner team | Consumers |", "|---|---|---|---|"]
for path in sorted(Path("contracts").glob("*.yaml")):
    c = yaml.safe_load(path.read_text())
    consumers = next((p["value"] for p in c.get("customProperties", []) if p["property"] == "consumers"), "none listed")
    lines.append(f"| `{path.name}` | {c['version']} | {c.get('team', {}).get('name', '?')} | {consumers} |")
text = "\n".join(lines)
print(text)
if len(sys.argv) > 1:
    Path(sys.argv[1]).open("a").write(text + "\n")
