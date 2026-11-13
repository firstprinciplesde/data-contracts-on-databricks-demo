"""Small reusable utilities that make life easier in Databricks.

Buckets (more to come):
    odcs        generate an ODCS data contract for a table from what its readers use
    lineage     find the readers of a table and the columns each one uses
"""
from databricks_awesome_utils.odcs import odcs_contract_from_lineage  # noqa: F401
from databricks_awesome_utils.lineage import find_readers  # noqa: F401

__version__ = "0.1.0"
