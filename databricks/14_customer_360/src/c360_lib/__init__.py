"""Transformation library for the WWI_Customer360 SSIS project migrated to Databricks.

Each module mirrors one legacy package (``ssis/14_customer_360/C360_*.dtsx``) and exposes
pure DataFrame -> DataFrame functions so the business rules can be unit tested with local
PySpark. Notebooks under ``../notebooks`` only read/write Delta tables and call the shared
``dbx_etl_common`` control layer.
"""
