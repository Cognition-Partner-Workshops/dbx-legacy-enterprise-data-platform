# Databricks notebook source
# MAGIC %md
# MAGIC # AGG_Refresh_Customer360
# MAGIC Migrated from `ssis/09_aggregates/AGG_Refresh_Customer360.dtsx` (generator `build_aggregate_packages.py`).
# MAGIC Target: `${catalog}.gold.agg_customer_360` (legacy `Aggregate.Customer 360`).
# MAGIC
# MAGIC **Strategy: full rebuild (atomic Delta overwrite of the whole table)**
# MAGIC
# MAGIC The legacy package TRUNCATEs `Aggregate.Customer 360` and rebuilds every current customer as at `AsAtDate`. A materialized view was **not** used because the package emits rejects (inactive customers when `IncludeInactiveCustomers = 0`), applies GDPR anonymisation and logs row counts / watermarks through the control framework; a single-commit `overwrite` gives the same all-or-nothing semantics as TRUNCATE+INSERT. Conditional split: erased and no-purchase customers insert; inactive customers reject unless included.
# MAGIC
# MAGIC Legacy control flow: Init As At Date -> Log Package Start -> Truncate Customer 360 -> Rebuild Customer 360 (data flow) -> Derive Profile Measures -> Assert Reconciliation -> Log Rejected Records -> Log Row Counts -> Log Package Success.
# MAGIC
# MAGIC `dbx_etl_common` is attached to the job task as a wheel library (see
# MAGIC `resources/wwi_09_aggregates.yml`, `../../common/dbx_etl_common/dist/*.whl`).

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, params, naming  # noqa: E402

import agg_common  # noqa: E402
import agg_sql  # noqa: E402
from agg_package import RefreshSpec, runRefresh  # noqa: E402

# COMMAND ----------

# Package-specific parameters ($Package::* in the .dtsx); job parameters override the widget defaults.
dbutils.widgets.text("AsAtDate", "")
dbutils.widgets.text("IncludeInactiveCustomers", "0")

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
businessDate = p["businessDate"]
reloadFullHistory = p["reloadFullHistory"]
t = agg_common.resolveTables(catalog, naming.table)

asAtDate = agg_common.getOptionalWidget(dbutils, "AsAtDate")
asAtDate = agg_common.parseDate("" if asAtDate.startswith("1900-") else asAtDate, businessDate)
includeInactiveCustomers = agg_common.parseBool(agg_common.getOptionalWidget(dbutils, "IncludeInactiveCustomers", "0"), False)

# COMMAND ----------

import datetime as dt  # noqa: E402

print(f"Building Customer 360 as at {asAtDate}; includeInactiveCustomers={includeInactiveCustomers}")

# COMMAND ----------

sql = agg_sql.customer360Sql(t, asAtDate)

spec = RefreshSpec(
    packageName="AGG_Refresh_Customer360",
    targetKey="agg_customer_360",
    sql=sql,
    replacePredicate=None,
    rejectCondition=(F.col("is_inactive") & F.lit(not includeInactiveCustomers)),
    rejectReasonCode="AGG_INACTIVE_CUSTOMER",
    rejectReason="Inactive customer: no order in 24 months",
    keyMeasures=("lifetime_net_revenue", "lifetime_gross_margin", "lifetime_order_count"),
    watermarkTo=dt.datetime.combine(asAtDate, dt.time.min),
    stepName="Aggregates",
)

# COMMAND ----------

result = runRefresh(spark, control, catalog, batchId, spec, t["agg_customer_360"])

dbutils.notebook.exit(json.dumps({
    "package": "AGG_Refresh_Customer360",
    "target": t["agg_customer_360"],
    "sourceRowCount": result.sourceRowCount,
    "targetRowCount": result.targetRowCount,
    "rejectRowCount": result.rejectRowCount,
    "measureTotals": result.measureTotals,
    "asAtDate": asAtDate.isoformat(),
}))
