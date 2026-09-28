"""Databricks migration of the WWI_Ingest_Files SSIS project (ssis/03_file_ingestion).

Auto Loader (cloudFiles) picks the feed files up from UC Volumes; the modules
here replicate the flat-file connection managers, data flows and Foreach loop
control logic of the seven ING_FILE_* packages as Spark DataFrame operations.
"""

__all__ = ["feeds", "lines", "transforms", "control_totals", "runner"]
