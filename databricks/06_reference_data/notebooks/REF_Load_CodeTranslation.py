# Databricks notebook source
# MAGIC %md
# MAGIC # REF_Load_CodeTranslation
# MAGIC Migrated from `ssis/06_reference_data/REF_Load_CodeTranslation.dtsx` (generator `build_reference_packages.py`, project WWI_ReferenceData).
# MAGIC
# MAGIC All status / reason seeds and the full steward crosswalk grid (`ref.usp_LoadCodeCrosswalk` without a domain) -> `silver.ref_code_crosswalk`; publishes one `etl.configuration` `CodeSetVersion.<DOMAIN>` row per domain and system (the legacy Dimension.Code Translation feed) and sweeps every raw extract for unmapped codes (`ref.vw_UnmappedSourceCode`, `ref.usp_ReportUnmappedCodes`).
# MAGIC
# MAGIC Control flow (legacy task -> here): `CTL Log Package Start` -> `control.logPackageStart`; `Truncate Dimension.Code Translation` -> idempotent
# MAGIC Delta MERGE (surrogate keys and reserved members are preserved); pre-tasks (`ref.usp_Load*`) -> `wwi_ref.ref_loads`;
# MAGIC data flow -> `wwi_ref.transforms` + `wwi_ref.delta_io`; error outputs -> `silver.err_*` + `control.logRejectedRecordSet`;
# MAGIC `CTL Log Row Count` -> `control.logRowCount`; `CTL Log Package Success` / `OnError` -> `control.logPackageEnd` / `control.logError`.
# MAGIC
# MAGIC Job parameters: `BatchId`, `BusinessDate`, `ReloadFullHistory`, `EnvironmentCode`, `RestartFromStep`, `catalog`.

# COMMAND ----------

import datetime
import os
import sys


def bundleSourcePath():
    """../src of this bundle (workspace files), so `wwi_ref` imports without a wheel build."""
    try:
        notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return os.path.join("/Workspace", os.path.dirname(os.path.dirname(notebookPath)).lstrip("/"), "src")
    except Exception:
        return os.path.abspath(os.path.join(os.getcwd(), "..", "src"))


if bundleSourcePath() not in sys.path:
    sys.path.insert(0, bundleSourcePath())

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, params, naming  # noqa: E402,F401  (dbx_etl_common wheel: task environment)

from wwi_ref import runtime, ref_loads, transforms, delta_io, schemas  # noqa: E402,F401

# COMMAND ----------

PACKAGE_NAME = "REF_Load_CodeTranslation"
p = params.getJobParams(dbutils)
print("catalog=%s batchId=%s businessDate=%s environment=%s restartFromStep=%s"
      % (p["catalog"], p["batchId"], p["businessDate"], p["environmentCode"], p["restartFromStep"]))

if runtime.isSkippedByRestart(p, PACKAGE_NAME):
    dbutils.notebook.exit("SKIPPED: RestartFromStep=%s resumes after %s" % (p["restartFromStep"], PACKAGE_NAME))

# COMMAND ----------

with runtime.packageRun(spark, p, PACKAGE_NAME) as ctx:
    # -- pre-tasks: ref.usp_LoadStatusCode, ref.usp_LoadReasonCode, ref.usp_LoadCodeCrosswalk (all domains)
    ref_loads.loadStatusCode(ctx)
    ref_loads.loadReasonCode(ctx)
    ref_loads.loadCodeCrosswalk(ctx, None, maintainedByName=PACKAGE_NAME)

    # -- DFT Load Code Translation: REF Code Crosswalk Active -> Shape Configuration Row -> Summarize Mapping Coverage -> etl.Configuration
    ctx.step("DFT Load Code Translation")
    crosswalk = spark.table(ctx.table("ref.CodeCrosswalk"))
    configuration = transforms.codeSetConfiguration(crosswalk).cache()
    rowsRead = configuration.count()
    configTarget = ctx.etlTable("configuration")
    configRows = (configuration
                  .withColumn("ConfigurationKey", F.concat_ws(".", F.col("ConfigurationKey"), F.col("SourceSystemCode")))
                  .withColumn("EnvironmentCode", F.lit(p["environmentCode"] or "ALL"))
                  .withColumn("ConfigurationValue", F.concat_ws("|", F.col("ConfigurationValue"), F.col("MappingCount").cast("string")))
                  .withColumn("ValueDataType", F.lit("string"))
                  .withColumn("Description", F.concat(F.lit("Code set version for "), F.col("CodeDomainCode"), F.lit(" / "),
                                                      F.col("SourceSystemCode"), F.lit(" (REF_Load_CodeTranslation)")))
                  .withColumn("IsSensitive", F.lit(False))
                  .withColumn("ModifiedAtUtc", F.current_timestamp())
                  .select("ConfigurationKey", "EnvironmentCode", "ConfigurationValue", "ValueDataType", "Description",
                          "IsSensitive", "ModifiedAtUtc"))
    configColumns = [c for c in configRows.columns if c in spark.table(configTarget).columns]
    if "ConfigurationId" in spark.table(configTarget).columns:
        configRows = delta_io.withSequence(spark, configTarget, configRows.withColumn("ConfigurationId", F.lit(None).cast("bigint"))
                                           .join(spark.table(configTarget).select("ConfigurationKey", "EnvironmentCode",
                                                                                  F.col("ConfigurationId").alias("_existingId")),
                                                 ["ConfigurationKey", "EnvironmentCode"], "left")
                                           .withColumn("ConfigurationId", F.col("_existingId")).drop("_existingId"),
                                           "ConfigurationId", ["ConfigurationKey"])
        configColumns = ["ConfigurationId"] + configColumns
    counts = delta_io.mergeReference(spark, configTarget, configRows.select(*configColumns), ["ConfigurationKey", "EnvironmentCode"],
                                     updateCols=[c for c in configColumns if c not in ("ConfigurationId", "ConfigurationKey", "EnvironmentCode")])
    ctx.addCounts("etl.Configuration", sourceRowCount=rowsRead,
                  targetRowCount=spark.table(configTarget).where(F.col("ConfigurationKey").startswith("CodeSetVersion.")).count(),
                  insertRowCount=counts.inserted, updateRowCount=counts.updated)
    configuration.unpersist()

    # -- DFT Scan For Unmapped Codes: SRC Unmapped Codes (ref.vw_UnmappedSourceCode) -> Tag Unmapped -> Screen -> ERR destination
    ctx.step("DFT Scan For Unmapped Codes")
    unmapped = transforms.tagUnmappedCodes(ref_loads.unmappedSourceCodes(ctx)).cache()
    scanned = unmapped.count()
    reportable, blank = transforms.screenUnmappedCode(unmapped)
    reported = ctx.rejectLookupFailures(
        reportable, "ref.vw_UnmappedSourceCode", "ref.CodeCrosswalk", "SourceCodeValue", "SourceCodeValue", "SourceCodeValue",
        "REF_UNMAPPED_CODE_REPORT", "source code observed without an active crosswalk mapping",
        sourceSystemCodeColumn="SourceSystemCode", routedToUnknownMember=True, occurrenceColumn="TotalOccurrenceCount",
        payloadColumns=["CodeDomainCode", "SourceSystemCode", "SourceCodeValue", "CoverageStatusCode", "SeverityCode", "ReviewedFlag"])
    reported += ctx.rejectConstraintViolations(
        blank, "ref.CodeCrosswalk", "CodeDomainCode", "SourceCodeValue", "SourceCodeValue", "REF_CODE_BLANK",
        "unmapped source code is blank", constraintName="CK_CodeCrosswalk_SourceCodeValue")
    ctx.addCounts("ref.vw_UnmappedSourceCode", sourceRowCount=scanned, targetRowCount=reported, rejectRowCount=reported)
    unmapped.unpersist()

    # -- post-task: ref.usp_ReportUnmappedCodes (all domains)
    ref_loads.reportUnmappedCodes(ctx)

    print(ctx.summary())
