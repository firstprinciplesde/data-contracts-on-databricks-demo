# Change playbook: the six kinds of change as contract releases

Levels are what `datacontract breaking old.yaml new.yaml` printed (Data Contract CLI 1.2.9). Default view is `consumer`; `ERROR` exits 1. Tested on a contract with an optional `quantity` (INT) and `currency` (STRING) next to the orders fields.

| Kind of change | Contract edit | Release | consumer view (default) | producer view | Roll-out |
|---|---|---|---|---|---|
| Add an optional column | add the property, `required: false` | minor | INFO | ERROR | none |
| Widen a type (INT to BIGINT) | change `physicalType` | minor by our rule | **ERROR** | **ERROR** | the CLI flags any type change; readers first, then release |
| Drop a column, step 1 | `deprecated: true` plus the removal date in the description | minor | INFO | INFO | wait until lineage shows no readers |
| Drop a column, step 2 | remove the property | major | **ERROR** | INFO | only after the readers moved |
| Rename, expand | add `currency_code`, deprecate `currency` | minor | INFO | ERROR | copy the values, move readers |
| Rename, direct | rename in place | major | **ERROR** | **ERROR** | do not: it is a remove plus an add, and lineage is lost on rename |
| Tighten (optional to required) | `required: true` | major | INFO | ERROR | backfill first |
| Tighten (narrower range) | lower `maximum` | major | INFO | ERROR | check existing data first |
| Change a meaning, new name | add `amount_cents`, deprecate `amount` | minor | INFO | ERROR | add the column, keep the old one, migrate |
| Change a meaning, in place (type) | `amount` DECIMAL to BIGINT | major | **ERROR** | **ERROR** | breaks readers |
| Change a meaning, in place (description only) | edit the description | none | INFO | INFO | **invisible to `breaking`**: needs a value rule |

## What the table says
- Under the consumer view a release that only adds, deprecates or tightens passes with INFO; anything that removes or retypes a property is ERROR. Removing a deprecated property is still ERROR: the check does not know you waited.
- The producer view flips additions and tightening to ERROR, because existing data would no longer satisfy the new contract.
- A change of meaning with the same name and type (`amount` from dollars to cents) is INFO in every view. Only a value rule catches it: the min and max on `amount` in DQX, or the SQL rule in `datacontract test`.
- ODCS defines no minor or major rule for `version`. This table is our convention.

## A field in the data that is not in the contract
- DQX strict schema validation (the default): the batch is rejected.
- DQX permissive (`strict_schema_validation=False`): extra columns are allowed.
- See `notebooks/01_producer.py`, widget `strict`.
