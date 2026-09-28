# Databricks notebook source
# MAGIC %md
# MAGIC # wwi_00_control_bootstrap
# MAGIC Creates the `etl` control schema, every control Delta table (identity columns, defaults, CHECK constraints),
# MAGIC applies the idempotent seed data and (re)creates the `etl.v_*` operational views.
# MAGIC
# MAGIC Re-runnable: tables are created `IF NOT EXISTS`, seeds are Delta `MERGE`s, views are `CREATE OR REPLACE`.
# MAGIC The `dbx_etl_common` wheel is attached to the task as a library by the bundle
# MAGIC (`databricks/common/databricks.yml`).

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog catalog (bundle variable ${var.catalog})")
dbutils.widgets.text("createSchemas", "True", "Also create bronze / silver / gold schemas")

from dbx_etl_common import bootstrap, naming, params

catalog = dbutils.widgets.get("catalog").strip()
if not catalog:
    raise ValueError("Job parameter 'catalog' is required")
createSchemas = params.parseBool(dbutils.widgets.get("createSchemas"), default=True)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
if createSchemas:
    for schema in (naming.BRONZE, naming.SILVER, naming.GOLD):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {naming.table(catalog, schema, '').rstrip('.')}")

summary = bootstrap.bootstrap(spark, catalog)
print(f"created tables: {summary['createdTables']}")
print(f"seeded tables:  {summary['seededTables']}")
print(f"views:          {summary['views']}")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.{naming.ETL}"))
