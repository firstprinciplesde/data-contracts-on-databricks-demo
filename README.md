# Data Contracts on Databricks - Demo

Demo repo for the First Principles Data Engineering video "Data Contracts on Databricks".

Two data contracts (ODCS) for an Orders API and the gold table built from it, tested in CI, enforced and extended on Databricks, with the six kinds of change handled one by one.

## Layout

- `contracts/` the data contracts (`orders_api.yaml`, `revenue_daily.yaml`)
- `pipeline/` what the jobs write, declared in code (`schemas.py`), and the silver-to-gold logic
- `producer/` the simulated Orders API
- `notebooks/` Databricks notebooks, in order `00_setup` to `06_observer`
- `scenarios/` one changed contract per kind of change
- `ci/` the checks that run on pull requests; `.github/` the workflows, CODEOWNERS and the PR template
- `tools/` contract generator, lineage and impact check, observer
- `docs/change-playbook.md` the six kinds of change as releases

## Where each check runs

| When | Where | What |
|---|---|---|
| Pull request (no data, no secrets) | GitHub Actions, `contract-ci.yml` | lint, `breaking` against the base branch, `test --dry-run`, the job's declared output vs the contract, the playbook scenarios |
| Before publish | the notebooks (`01`, `02`) | DQX checks built from the contract: exact columns, order and types first, then the value rules; a bad batch goes to quarantine and nothing is published |
| After the data is loaded | GitHub Actions, `contract-data-tests.yml` | `datacontract ci` on the real tables (on merge to main, every morning, by hand) |
| What no rule covers | `06_observer` | statistical drift checks |

## Set up your copy

1. Click **Use this template** on GitHub and create your own repo (private is fine), then `git clone` it.
2. You need a Databricks workspace (Free Edition works) and a SQL warehouse. Run `notebooks/00_setup` first.
3. For the after-load workflow add three repository secrets under Settings, Secrets and variables, Actions:
   - `DATABRICKS_SERVER_HOSTNAME` e.g. `dbc-xxxx.cloud.databricks.com` (no `https://`)
   - `DATABRICKS_HTTP_PATH` your warehouse's HTTP path, e.g. `/sql/1.0/warehouses/<id>`
   - `DATABRICKS_TOKEN` a personal access token (give it a short expiry)

   Without them that workflow skips with a notice, and the pull request checks still run. They never need the secrets.
