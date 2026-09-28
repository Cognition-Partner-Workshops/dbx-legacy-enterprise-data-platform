# Databricks notebook source
# MAGIC %md
# MAGIC # SLS_Load_Commission (parameterised by RegionCode)
# MAGIC One implementation for the three legacy packages `SLS_NA_Load_Commission`,
# MAGIC `SLS_EU_Load_Commission` and `SLS_APAC_Load_Commission`. The region's own rules are
# MAGIC preserved (see `src/sales_commission.py`):
# MAGIC
# MAGIC | RegionCode | Commissionable amount | Period | Regional rule | Parameters |
# MAGIC |---|---|---|---|---|
# MAGIC | NA | ExtendedPrice + TaxAmount (USD only) | calendar month | accelerator above threshold, house accounts at HouseAccountRatePercent | CommissionMonth, HouseAccountRatePercent |
# MAGIC | EU | VAT-exclusive net, converted to EUR (AVERAGE, month end) | calendar month | statutory cap, cash-basis countries held until CLEARED payment | CommissionMonth, CashBasisCountries |
# MAGIC | APAC | GST-exclusive, converted to plan currency (AVERAGE) | 4-4-5 fiscal period | team split, missing calendar blocks, missing FX rejects | FiscalPeriod445, TeamSplitEnabled |
# MAGIC
# MAGIC Control flow (all regions): Log Package Start -> Truncate work table -> Calculate ->
# MAGIC (regional step) -> Post Commission (MERGE into gold.fact_sale_commission) -> Log Row Counts ->
# MAGIC Log Package Success; OnError -> Log Error + Mark Execution Failed.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, naming, params  # noqa: E402

import sales_common as sc  # noqa: E402
import sales_schemas as schemas  # noqa: E402
import sales_commission_run as runner  # noqa: E402

# COMMAND ----------

summary = runner.runCommission(spark, dbutils)
print(summary)

# COMMAND ----------

dbutils.notebook.exit(str(summary))
