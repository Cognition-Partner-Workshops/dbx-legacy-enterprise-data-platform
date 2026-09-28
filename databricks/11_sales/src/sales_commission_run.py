"""End-to-end flow of the parameterised commission notebook (SLS_Load_Commission),
one function per legacy package step, selected by RegionCode."""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import sales_commission as com
import sales_common as sc
import sales_schemas as schemas

COMMISSION_WIDGETS = (
    ("RegionCode", "NA"), ("CommissionMonth", ""), ("HouseAccountRatePercent", "50"),
    ("CashBasisCountries", "DE,AT"), ("FiscalPeriod445", ""), ("TeamSplitEnabled", "True"),
)

WORK_TABLES = {
    "NA": ("work_commission_na", schemas.WORK_COMMISSION_NA),
    "EU": ("work_commission_eu", schemas.WORK_COMMISSION_EU),
    "APAC": ("work_commission_apac", schemas.WORK_COMMISSION_APAC),
}


def packageNameFor(regionCode: str) -> str:
    return "SLS_%s_Load_Commission" % regionCode


def runCommission(spark: SparkSession, dbutils, regionCode: str | None = None) -> dict:
    regionCode = (regionCode or sc.getWidget(dbutils, "RegionCode", "NA")).strip().upper()
    if regionCode not in com.REGION_CODES:
        raise ValueError("RegionCode must be one of %s, got %r" % (com.REGION_CODES, regionCode))
    packageName = packageNameFor(regionCode)
    ctx = sc.resolveContext(spark, dbutils, params, control, packageName, COMMISSION_WIDGETS)
    if sc.shouldSkipForRestart(ctx.restartFromStep, packageName):
        return {"package": packageName, "status": "Skipped", "reason": "RestartFromStep=" + ctx.restartFromStep}

    def t(schema, table):
        return naming.table(ctx.catalog, schema, table)

    summary = {"package": packageName, "batchId": ctx.batchId}
    with sc.legacyPackageRun(spark, control, ctx, packageName) as run:
        pid = run.packageExecutionId
        workTable, workSchema = WORK_TABLES[regionCode]
        workName = t("silver", workTable)
        factName = t("gold", "fact_sale_commission")
        sc.ensureTable(spark, workName, workSchema)
        sc.ensureTable(spark, factName, schemas.FACT_SALE_COMMISSION, partitionBy=("RegionCode",))

        run.currentTask = "Read staged sale lines"
        allLines = com.legacySaleLines(spark.table(t("silver", "stg_sale_line")), regionCode)
        lines = sc.batchFilter(allLines, "LoadBatchId", ctx.batchId, ctx.reloadFullHistory)
        plans = com.legacyCommissionPlans(spark.table(t("silver", "stg_commission_plan")))
        regionLines = com.regionSaleLines(lines, regionCode)
        planned = com.joinCommissionPlans(regionLines, plans, regionCode)

        if regionCode == "NA":
            postedPeriod = sc.parseOptional(sc.getWidget(dbutils, "CommissionMonth")) or sc.monthPeriod(ctx.businessDate)
            houseRate = int(float(sc.getWidget(dbutils, "HouseAccountRatePercent", "50") or 50))
            run.currentTask = "Find Reps Without A Plan"
            summary["unplannedRepCount"] = com.countUnplannedReps(lines, plans, "NA")
            run.currentTask = "Calculate NA Commission"
            houseAccounts = com.currentHouseAccountFlags(spark.table(t("gold", "dim_customer")))
            if houseAccounts is None:
                control.logError(spark, ctx.catalog, packageExecutionId=pid, batchId=ctx.batchId,
                                 errorSeverity="Warning", errorCode="HOUSE_ACCOUNT_FLAG_MISSING",
                                 sourceName=packageName, sourceComponent="Lookup House Account Flag",
                                 errorDescription="gold.dim_customer has no IsHouseAccount column; "
                                                  "house account factor defaulted to 1.")
            work = com.computeNaCommission(planned, houseAccounts, houseRate)
            work = work.withColumn("BatchId", F.lit(ctx.batchId).cast("long"))
            run.currentTask = "Truncate work_CommissionNa"
            run.rowsRead = sc.overwriteTable(work, workName, workSchema)
            run.rowsInserted = run.rowsRead

        elif regionCode == "EU":
            postedPeriod = sc.parseOptional(sc.getWidget(dbutils, "CommissionMonth")) or sc.monthPeriod(ctx.businessDate)
            cashBasis = sc.parseCsvList(sc.getWidget(dbutils, "CashBasisCountries", "DE,AT"))
            heldName = t("silver", "work_commission_eu_held")
            sc.ensureTable(spark, heldName, schemas.WORK_COMMISSION_EU_HELD)
            run.currentTask = "Calculate EU Commission"
            fx = com.legacyFxRates(spark.table(t("silver", "stg_fx_rate")))
            eu = com.computeEuCommission(planned, fx, cashBasis).withColumn("BatchId", F.lit(ctx.batchId).cast("long"))
            accrual, held = com.splitCashBasis(eu)
            run.currentTask = "Truncate work_CommissionEu"
            accrualRows = sc.overwriteTable(accrual, workName, workSchema)
            heldRows = sc.overwriteTable(held, heldName, schemas.WORK_COMMISSION_EU_HELD)
            run.rowsRead = accrualRows + heldRows
            run.currentTask = "Release Cash Basis Lines With Payment"
            payments = com.legacyCustomerPayments(spark.table(t("silver", "stg_customer_payment")))
            released = com.releaseHeldEuLines(spark.table(heldName), payments)
            released = released.withColumn("BatchId", F.lit(ctx.batchId).cast("long"))
            releasedDf = sc.conformToSchema(released, workSchema).cache()
            releasedRows = releasedDf.count()
            releasedDf.write.format("delta").mode("append").saveAsTable(workName)
            releasedDf.unpersist()
            run.rowsInserted = accrualRows + releasedRows
            run.currentTask = "Count Capped Representatives"
            summary["heldRowCount"] = heldRows
            summary["releasedRowCount"] = releasedRows
            summary["cappedRepCount"] = com.countCappedReps(spark.table(workName))

        else:  # APAC
            teamSplit = sc.parseBool(sc.getWidget(dbutils, "TeamSplitEnabled", "True"), True)
            rejectName = t("silver", "err_commission_apac_reject")
            sc.ensureTable(spark, rejectName, schemas.ERR_COMMISSION_APAC_REJECT)
            cal = com.legacyFiscalCalendar(spark.table(t("silver", "stg_fiscal_calendar445")))
            postedPeriod = sc.parseOptional(sc.getWidget(dbutils, "FiscalPeriod445"))
            if postedPeriod is None:
                periodRow = (cal.where(F.col("CalendarDate").cast("date") == F.lit(ctx.businessDate))
                                .select("FiscalPeriod445").limit(1).collect())
                if not periodRow:
                    raise ValueError("FiscalPeriod445 not supplied and BusinessDate %s is not in stg.FiscalCalendar445"
                                     % ctx.businessDate)
                postedPeriod = periodRow[0][0]
            run.currentTask = "Check 445 Calendar Coverage"
            missingDays = com.countMissingCalendarDays(regionLines, cal)
            summary["missingCalendarCount"] = missingDays
            if missingDays > 0:
                # Legacy precedence constraint (MissingCalendarCount == 0) silently skips the data flow
                # and the package still succeeds; surfaced here as a Warning in etl.error_log.
                control.logError(spark, ctx.catalog, packageExecutionId=pid, batchId=ctx.batchId,
                                 errorSeverity="Warning", errorCode="FISCAL_CALENDAR_GAP",
                                 sourceName=packageName, sourceComponent="Check 445 Calendar Coverage",
                                 errorDescription="%d invoice date(s) missing from stg.FiscalCalendar445; "
                                                  "APAC commission not calculated for batch %d." % (missingDays, ctx.batchId))
                summary["status"] = "SucceededWithWarnings"
                control.logRowCount(spark, ctx.catalog, pid, "Fact.Sale", sourceRowCount=0, targetRowCount=0, rejectRowCount=0)
                return summary
            run.currentTask = "Calculate APAC Commission"
            fx = com.legacyFxRates(spark.table(t("silver", "stg_fx_rate")))
            matched, rejected = com.computeApacCommission(planned, cal, fx, teamSplit)
            matched = matched.withColumn("BatchId", F.lit(ctx.batchId).cast("long"))
            rejected = rejected.withColumn("BatchId", F.lit(ctx.batchId).cast("long"))
            run.currentTask = "Truncate work_CommissionApac"
            run.rowsInserted = sc.overwriteTable(matched, workName, workSchema)
            rejectedRows = sc.overwriteTable(rejected, rejectName, schemas.ERR_COMMISSION_APAC_REJECT)
            run.rowsRead = run.rowsInserted + rejectedRows
            if rejectedRows:
                run.rowsRejected = control.logRejectedRecordSet(
                    spark, ctx.catalog, "stg.SaleLine", spark.table(rejectName),
                    batchId=ctx.batchId, packageExecutionId=pid, sourceSystemCode="WWI",
                    rejectStage="Mart", rejectReasonCode="FX_RATE_MISSING", businessKeyColumn="SaleLineId")
            run.currentTask = "Count Period Boundary Lines"
            summary["periodBoundaryCount"] = com.countPeriodBoundaryLines(spark.table(workName))

        run.currentTask = "Post %s Commission" % regionCode
        posting = com.postingRows(spark.table(workName), regionCode, postedPeriod, ctx.batchId, pid)
        metrics = sc.mergeInto(spark, posting, factName, schemas.FACT_SALE_COMMISSION,
                               ("RegionCode", "SaleLineId", "CommissionPeriod"))
        run.rowsUpdated = int(metrics["num_affected_rows"] if metrics["num_affected_rows"] is not None
                              else spark.table(workName).count())
        summary["postedCommissionPeriod"] = postedPeriod

        run.currentTask = "Log Row Counts"
        control.logRowCount(spark, ctx.catalog, pid, "Fact.Sale", sourceRowCount=run.rowsRead,
                            targetRowCount=run.rowsInserted, rejectRowCount=run.rowsRejected)
        control.logRowCount(spark, ctx.catalog, pid, "gold.fact_sale_commission",
                            sourceRowCount=run.rowsInserted, targetRowCount=run.rowsUpdated,
                            insertRowCount=metrics["num_inserted_rows"], updateRowCount=metrics["num_updated_rows"])
        control.logRowCount(spark, ctx.catalog, pid, "silver." + workTable,
                            sourceRowCount=run.rowsRead, targetRowCount=run.rowsInserted, rejectRowCount=run.rowsRejected)
        run.currentTask = "Log Package Success"
        summary.update({"status": "Succeeded", "rowsRead": run.rowsRead, "rowsInserted": run.rowsInserted,
                        "rowsUpdated": run.rowsUpdated, "rowsRejected": run.rowsRejected})
    return summary
