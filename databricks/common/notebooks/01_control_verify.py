# Databricks notebook source
# MAGIC %md
# MAGIC # Control bootstrap verification
# MAGIC Fails the job when a control table / view is missing or a seed table is empty.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
catalog = dbutils.widgets.get("catalog").strip()

from dbx_etl_common import naming, schema, seeds, views

missing = [t.name for t in schema.TABLES if not spark.catalog.tableExists(naming.controlTable(catalog, t.name))]
missing += [v for v in views.VIEW_NAMES if not spark.catalog.tableExists(naming.controlTable(catalog, v))]
empty = [s.table for s in seeds.SEEDS if spark.table(naming.controlTable(catalog, s.table)).limit(1).count() == 0]
if missing or empty:
    raise RuntimeError(f"control bootstrap incomplete: missing={missing} emptySeeds={empty}")
print(f"{len(schema.TABLES)} control tables, {len(views.VIEW_NAMES)} views and {len(seeds.SEEDS)} seed sets present in {catalog}.{naming.ETL}")
