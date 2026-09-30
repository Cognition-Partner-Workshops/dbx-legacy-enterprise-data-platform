# Databricks notebook source
# Standalone run of one owned DQ_* / ERR_* / MNT_* / ING_FILE_* package (job ssis_platform_control_<package>).
# MAGIC %run ./_bootstrap

# COMMAND ----------
import json

from platform_control.control import ControlFramework
from platform_control.runners import runStandalone
from platform_control.spark import getSpark
from platform_control.tables import ensureControlTables

bindDbutils(dbutils)  # noqa: F821
cfg = platformConfig()
spark = getSpark()
cf = ControlFramework(spark, cfg)
ensureControlTables(spark, cfg)

package = widget("package")
batchId = widget("batch_id")
parameters = parametersJson()
parameters.setdefault("job_run_id", widget("job_run_id"))
result = runStandalone(cf, package, parameters, batchId=int(batchId) if batchId else None)
print(json.dumps(result, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
