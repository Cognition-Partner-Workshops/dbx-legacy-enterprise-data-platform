"""Reference-table lookups shared by several staging packages, and the Spark
ports of the two cross-cutting staging procedures every load calls:
stg.usp_TranslateSourceCodes and stg.usp_ConvertCurrencyAmounts."""

from pyspark.sql import Window
from pyspark.sql import functions as F

from stg_common import expressions as X

ROW_ID = "_stgRowId"


def withRowId(df):
    return df.withColumn(ROW_ID, F.monotonically_increasing_id()) if ROW_ID not in df.columns else df


def dropRowId(df):
    return df.drop(ROW_ID) if ROW_ID in df.columns else df


def region(run):
    return run.silver("ref_region")


def country(run, activeOnly=True):
    df = run.silver("ref_country")
    return df.where(F.col("IsActive") == F.lit(True)) if activeOnly else df


def fxRateDaily(run):
    return run.silver("ref_fx_rate_daily")


def codeCrosswalk(run, domain):
    return run.silver("ref_code_crosswalk").where(
        (F.col("CodeDomainCode") == domain) & F.col("EffectiveToDate").isNull()
    )


def uomConversion(run):
    return run.silver("ref_uom_conversion")


def latestRateOnOrBefore(
    df,
    fxDf,
    fromCurrencyCol,
    rateDateCol,
    rateTypeCol,
    outputCol="FxRate",
    outputDateCol=None,
    toCurrency="USD",
    minDateCol=None,
):
    """`OUTER APPLY (SELECT TOP 1 ConversionRate ... WHERE RateDate <= x ORDER BY
    RateDate DESC)`: latest rate on or before the row's date, treasury overrides
    winning on a tie. `rateTypeCol` may be a literal string or a Column."""
    df = withRowId(df)
    rateType = F.lit(rateTypeCol) if isinstance(rateTypeCol, str) else rateTypeCol
    left = df.withColumn("_rateType", rateType)
    fx = fxDf.select(
        F.col("FromCurrencyCode").alias("_fxFrom"),
        F.col("ToCurrencyCode").alias("_fxTo"),
        F.col("RateDate").alias("_fxDate"),
        F.col("RateTypeCode").alias("_fxType"),
        F.col("ConversionRate").alias("_fxRate"),
        F.coalesce(F.col("IsTreasuryOverride").cast("int"), F.lit(0)).alias("_fxOverride"),
    ).where(F.col("_fxTo") == toCurrency)
    cond = (
        (F.col(fromCurrencyCol) == F.col("_fxFrom"))
        & (F.col("_rateType") == F.col("_fxType"))
        & (F.col("_fxDate") <= F.col(rateDateCol).cast("date"))
    )
    if minDateCol is not None:
        cond = cond & (F.col("_fxDate") >= minDateCol)
    joined = left.join(fx, cond, "left")
    w = Window.partitionBy(ROW_ID).orderBy(F.col("_fxDate").desc_nulls_last(), F.col("_fxOverride").desc())
    ranked = joined.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1)
    out = ranked.withColumn(outputCol, F.col("_fxRate"))
    if outputDateCol:
        out = out.withColumn(outputDateCol, F.col("_fxDate"))
    return out.drop("_rateType", "_fxFrom", "_fxTo", "_fxDate", "_fxType", "_fxRate", "_fxOverride", "_rn")


def translateSourceCodes(run, df, codeDomain, sourceCodeCol, targetCol, keyCol, unmatchedAction="LEAVE", defaultValue=None, sourceSystemCol="SourceSystemCode"):
    """Port of stg.usp_TranslateSourceCodes for the DataFrame in flight.

    Matched rows take ConformedCodeValue. Unmatched rows are registered in
    err.RejectedLookupFailure and then, per @UnmatchedAction: LEAVE keeps the
    source value, NULL blanks it, DEFAULT applies @DefaultValue; NULL/DEFAULT
    also flag DqStatusCode = 'WARN'."""
    if unmatchedAction not in ("LEAVE", "NULL", "DEFAULT"):
        raise ValueError("UnmatchedAction must be LEAVE, NULL or DEFAULT")
    if unmatchedAction == "DEFAULT" and defaultValue is None:
        raise ValueError("DefaultValue is required when UnmatchedAction = DEFAULT")
    cx = codeCrosswalk(run, codeDomain).select(
        F.col("SourceSystemCode").alias("_cxSystem"),
        F.upper(F.trim(F.col("SourceCodeValue"))).alias("_cxSource"),
        F.col("ConformedCodeValue").alias("_cxConformed"),
    ).dropDuplicates(["_cxSystem", "_cxSource"])
    src = F.upper(F.trim(F.col(sourceCodeCol).cast("string")))
    systemCode = F.col(sourceSystemCol) if sourceSystemCol in df.columns else F.lit(run.sourceSystemCode)
    joined = df.join(
        cx,
        (systemCode == F.col("_cxSystem")) & (src == F.col("_cxSource")),
        "left",
    )
    unmatched = joined.where(F.col("_cxConformed").isNull() & F.col(sourceCodeCol).isNotNull())
    run.rejectLookupFailures(
        unmatched,
        lookupName="ref.CodeCrosswalk/%s" % codeDomain,
        lookupColumn=sourceCodeCol,
        sourceBusinessKey=keyCol,
        reason="%s has no active ref.CodeCrosswalk row for domain %s" % (sourceCodeCol, codeDomain),
        rejectStage="Transform",
    )
    matched = F.col("_cxConformed").isNotNull()
    if unmatchedAction == "LEAVE":
        value = F.when(matched, F.col("_cxConformed")).otherwise(F.col(targetCol) if targetCol in df.columns else F.col(sourceCodeCol))
    elif unmatchedAction == "NULL":
        value = F.when(matched, F.col("_cxConformed"))
    else:
        value = F.when(matched, F.col("_cxConformed")).otherwise(F.lit(defaultValue))
    out = joined.withColumn(targetCol, value)
    if unmatchedAction != "LEAVE":
        dq = F.col("DqStatusCode") if "DqStatusCode" in df.columns else F.lit("PASS")
        out = out.withColumn("DqStatusCode", F.when(~matched & F.col(sourceCodeCol).isNotNull(), F.lit("WARN")).otherwise(dq))
    return out.drop("_cxSystem", "_cxSource", "_cxConformed")


def rateTypeForRegion(regionCol):
    """usp_ConvertCurrencyAmounts: EU books at PERIOD_END, APAC at CORPORATE, else SPOT."""
    return X.regionCase(regionCol, {"EU": "PERIOD_END", "APAC": "CORPORATE"}, "SPOT")


def convertCurrencyAmounts(run, df, amountCol, currencyCol, rateDateCol, regionCol, outputCol, keyCol, fallbackDays=7, toCurrency="USD", scratchObjectName=None):
    """Port of stg.usp_ConvertCurrencyAmounts for the DataFrame in flight.

    Resolves the exact or the most recent prior rate inside the fallback window
    (treasury overrides first), applies the Euro legacy fixed rate for retired
    currencies, leaves same-currency rows at their source amount, and records
    every resolution in work.CurrencyConversionScratch. Adds `outputCol`,
    `outputCol + 'Rate'` and `outputCol + 'RateResolutionCode'`."""
    df = withRowId(df)
    rateType = rateTypeForRegion(regionCol)
    minDate = F.date_sub(F.col(rateDateCol).cast("date"), fallbackDays)
    withRate = latestRateOnOrBefore(
        df.withColumn("_rateType", rateType),
        fxRateDaily(run),
        currencyCol,
        rateDateCol,
        F.col("_rateType"),
        outputCol="_rate",
        outputDateCol="_rateDate",
        toCurrency=toCurrency,
        minDateCol=minDate,
    )
    cur = run.silver("ref_currency").select(
        F.col("CurrencyCode").alias("_ccy"),
        F.col("IsEuroLegacy").alias("_euroLegacy"),
        F.col("EuroFixedRate").alias("_euroFixed"),
    )
    withRate = withRate.join(cur, F.col(currencyCol) == F.col("_ccy"), "left")
    eurRate = latestRateOnOrBefore(
        withRate.select(ROW_ID, rateDateCol, rateType.alias("_rateType")).withColumn("_eur", F.lit("EUR")),
        fxRateDaily(run),
        "_eur",
        rateDateCol,
        F.col("_rateType"),
        outputCol="_eurRate",
        toCurrency=toCurrency,
        minDateCol=minDate,
    ).select(ROW_ID, "_eurRate")
    withRate = withRate.join(eurRate, ROW_ID, "left")
    sameCurrency = F.col(currencyCol) == toCurrency
    legacyRate = F.when(
        (F.col("_euroLegacy") == F.lit(True)) & F.col("_euroFixed").isNotNull() & F.col("_eurRate").isNotNull(),
        F.col("_eurRate") / F.col("_euroFixed"),
    )
    appliedRate = F.when(sameCurrency, F.lit(1.0)).otherwise(F.coalesce(F.col("_rate"), legacyRate))
    resolution = (
        F.when(sameCurrency, F.lit("SAME_CURRENCY"))
        .when(F.col("_rate").isNotNull() & (F.col("_rateDate") == F.col(rateDateCol).cast("date")), F.lit("EXACT"))
        .when(F.col("_rate").isNotNull(), F.lit("PRIOR_DAY"))
        .when(legacyRate.isNotNull(), F.lit("EURO_LEGACY"))
        .otherwise(F.lit("MISSING"))
    )
    out = withRate.withColumns(
        {
            outputCol + "Rate": appliedRate.cast("decimal(19,8)"),
            outputCol + "RateResolutionCode": resolution,
            outputCol: F.when(appliedRate.isNotNull(), (F.col(amountCol) * appliedRate).cast("decimal(19,4)")),
        }
    )
    scratch = out.select(
        F.lit(scratchObjectName or run.objectName).alias("TargetObjectName"),
        F.col(keyCol).cast("string").alias("TargetBusinessKey"),
        F.lit(amountCol).alias("AmountColumnName"),
        F.col(currencyCol).alias("FromCurrencyCode"),
        F.lit(toCurrency).alias("ToCurrencyCode"),
        rateType.alias("RateTypeCode"),
        F.col(rateDateCol).cast("date").alias("RequestedRateDate"),
        F.col("_rateDate").alias("AppliedRateDate"),
        F.col(outputCol + "Rate").alias("ConversionRate"),
        F.col(amountCol).cast("decimal(19,4)").alias("SourceAmount"),
        F.col(outputCol).alias("ConvertedAmount"),
        F.col(outputCol + "RateResolutionCode").alias("RateResolutionCode"),
        F.when(F.col("_rateDate").isNotNull(), F.datediff(F.col(rateDateCol).cast("date"), F.col("_rateDate"))).cast("smallint").alias("FallbackDaysUsed"),
        F.current_timestamp().alias("CreatedAtUtc"),
    )
    _appendScratch(run, scratch)
    return out.drop("_rateType", "_rate", "_rateDate", "_ccy", "_euroLegacy", "_euroFixed", "_eurRate")


def _appendScratch(run, scratch):
    """work.CurrencyConversionScratch: the procedure deletes its own object/column
    slice for the batch before re-inserting, so a re-run never doubles up."""
    fqn = run.silverTable("work_currency_conversion_scratch")
    stamped = scratch.withColumns(
        {"BatchId": F.lit(run.batchId).cast("long"), "PackageExecutionId": F.lit(run.packageExecutionId).cast("long")}
    )
    if run.spark.catalog.tableExists(fqn):
        slices = scratch.select("TargetObjectName", "AmountColumnName").distinct().collect()
        for row in slices:
            run.spark.sql(
                "DELETE FROM %s WHERE BatchId = %d AND TargetObjectName = '%s' AND AmountColumnName = '%s'"
                % (fqn, run.batchId, row["TargetObjectName"].replace("'", "''"), row["AmountColumnName"].replace("'", "''"))
            )
        stamped.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fqn)
    else:
        stamped.write.format("delta").mode("overwrite").saveAsTable(fqn)
