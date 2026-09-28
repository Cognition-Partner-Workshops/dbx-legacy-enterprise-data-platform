"""Shared helpers for the WWI_DataQuality (ssis/05_data_quality) migration.

Pure transformation logic lives here so it can be unit tested with local
PySpark. Notebooks under ../../notebooks import this package and the shared
control layer ``dbx_etl_common`` (owned by session 00); nothing in here
re-implements the control framework.
"""
