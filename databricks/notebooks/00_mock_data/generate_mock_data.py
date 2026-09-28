# Databricks notebook source
# ruff: noqa: F821  (dbutils / spark / display are Databricks notebook globals)
# MAGIC %md
# MAGIC # Generate mock source data
# MAGIC Runs `sales_lakehouse.mock_data.generate` and writes the CONVENTIONS.md "Mock source data"
# MAGIC layout (`sqlserver/<Schema>/<Table>.csv`, `oracle/<SCHEMA>/<TABLE>.csv`, `manifest.json`)
# MAGIC to a Unity Catalog volume. Downstream bronze reads `mock_data_root`.

# COMMAND ----------

import os
import sys
from pathlib import Path

dbutils.widgets.text("catalog", "wwi_sales", "Unity Catalog catalog")
dbutils.widgets.text("mock_data_root", "", "Volume path for the generated CSVs (default: /Volumes/<catalog>/sales_bronze/mock_source)")
dbutils.widgets.dropdown("scale", "small", ["small", "medium"], "Scale")
dbutils.widgets.text("seed", "42", "Seed")
dbutils.widgets.dropdown("parquet", "false", ["false", "true"], "Also write Parquet for the 5 largest tables")

catalog = dbutils.widgets.get("catalog")
mockDataRoot = dbutils.widgets.get("mock_data_root") or f"/Volumes/{catalog}/sales_bronze/mock_source"
scale = dbutils.widgets.get("scale")
seed = int(dbutils.widgets.get("seed"))
parquet = dbutils.widgets.get("parquet") == "true"

# COMMAND ----------

# The repo's src/ directory is two levels up from this notebook when run from a Repo / bundle.
srcDir = os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src"))
if srcDir not in sys.path:
    sys.path.insert(0, srcDir)

from sales_lakehouse.mock_data.generate import generate

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.sales_bronze")
volumeParts = mockDataRoot.strip("/").split("/")
if volumeParts[0] == "Volumes" and len(volumeParts) >= 4:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {volumeParts[1]}.{volumeParts[2]}.{volumeParts[3]}")

manifest = generate(Path(mockDataRoot), seed=seed, scale=scale, parquet=parquet)
tables = manifest["tables"]
edgeCases = manifest["edgeCases"]
assert isinstance(tables, list) and isinstance(edgeCases, list)
print(f"wrote {len(tables)} tables and {len(edgeCases)} edge cases to {mockDataRoot}")
display(spark.createDataFrame([(t["path"], t["rowCount"]) for t in tables], ["path", "rowCount"]))

# COMMAND ----------

dbutils.notebook.exit(mockDataRoot)
