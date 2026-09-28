"""Shared helpers for the wwi_12_inventory bundle (WWI_Inventory SSIS project).

`transforms` holds the pure DataFrame logic ported from the five INV_* packages
(unit-tested locally); `contracts` holds the legacy -> Delta object/column
bindings; `runtime` holds the Delta write / control-framework glue used by the
notebooks and is only exercised on Databricks.
"""
