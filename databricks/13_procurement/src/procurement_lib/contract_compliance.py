"""PRC_Load_ContractCompliance: off-contract spend and contract leakage detection."""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .common import FAR_FUTURE, money, zeroMoney

STATUS_NON_PREFERRED = "NON_PREFERRED"
STATUS_NO_CONTRACT = "NO_CONTRACT"
STATUS_EXPIRED_CONTRACT = "EXPIRED_CONTRACT"
STATUS_PRICE_LEAKAGE = "PRICE_LEAKAGE"
STATUS_COMPLIANT = "COMPLIANT"

REJECT_OBJECT_NAME = "stg.VendorContract"
REJECT_REASON = "Purchase line failed contract compliance"


def evaluateContractCompliance(poLineDf: DataFrame, poHeaderDf: DataFrame, contractDf: DataFrame,
                               businessDate: date, complianceWindowDays: int,
                               priceLeakageTolerancePercent: int) -> DataFrame:
    """Execute SQL `Evaluate Contract Compliance` -> work.ContractCompliance rows.

    The contract join is supplier + category only (no date test) so an expired contract is
    still found and classified EXPIRED_CONTRACT. NON_PREFERRED is tested first: no contract for
    this supplier but a preferred contract covering the category on the order date exists.
    """
    windowStart = businessDate - timedelta(days=int(complianceWindowDays))
    pol = poLineDf.alias("pol")
    poh = poHeaderDf.alias("poh")
    vc = contractDf.alias("vc")

    base = (
        pol.join(poh, F.col("poh.PurchaseOrderNumber") == F.col("pol.PurchaseOrderNumber"), "inner")
        .join(vc, (F.col("vc.SupplierId") == F.col("poh.SupplierId"))
              & (F.col("vc.CategoryCode") == F.col("pol.CategoryCode")), "left")
        .where(F.col("poh.OrderDate") >= F.lit(windowStart))
        .select(
            F.col("pol.PurchaseOrderNumber").alias("PurchaseOrderNumber"),
            F.col("pol.LineNumber").alias("LineNumber"),
            F.col("poh.SupplierId").alias("SupplierId"),
            F.col("pol.CategoryCode").alias("CategoryCode"),
            F.col("poh.OrderDate").alias("OrderDate"),
            money(F.col("pol.OrderedOuters") * F.col("pol.ExpectedUnitPricePerOuter")).alias("OrderedAmount"),
            F.col("pol.OrderedOuters").alias("OrderedOuters"),
            money(F.col("pol.ExpectedUnitPricePerOuter")).alias("ExpectedUnitPricePerOuter"),
            F.col("vc.ContractNumber").alias("ContractNumber"),
            money(F.col("vc.ContractPricePerOuter")).alias("ContractPricePerOuter"),
            F.col("vc.ContractEndDate").alias("ContractEndDate"),
        )
    )

    # EXISTS (preferred contract for the category valid on the order date), as a semi-join flag.
    preferred = (
        contractDf.where(F.col("IsPreferred").cast("boolean"))
        .select(
            F.col("CategoryCode").alias("PrefCategoryCode"),
            F.col("ContractStartDate").alias("PrefStart"),
            F.coalesce(F.col("ContractEndDate"), F.to_date(F.lit(FAR_FUTURE))).alias("PrefEnd"),
        )
    )
    preferredKeys = (
        base.select("PurchaseOrderNumber", "LineNumber", "CategoryCode", "OrderDate").dropDuplicates()
        .join(preferred, (F.col("PrefCategoryCode") == F.col("CategoryCode"))
              & (F.col("OrderDate") >= F.col("PrefStart")) & (F.col("OrderDate") <= F.col("PrefEnd")), "leftsemi")
        .withColumn("PreferredContractExists", F.lit(True))
    )
    withPreferred = base.join(preferredKeys, ["PurchaseOrderNumber", "LineNumber", "CategoryCode", "OrderDate"], "left") \
        .withColumn("PreferredContractExists", F.coalesce(F.col("PreferredContractExists"), F.lit(False)))

    contractEnd = F.coalesce(F.col("ContractEndDate"), F.to_date(F.lit(FAR_FUTURE)))
    toleranceFactor = F.lit(1) + F.lit(int(priceLeakageTolerancePercent)) / F.lit(100.0)
    status = (
        F.when(F.col("ContractNumber").isNull() & F.col("PreferredContractExists"), F.lit(STATUS_NON_PREFERRED))
        .when(F.col("ContractNumber").isNull(), F.lit(STATUS_NO_CONTRACT))
        .when(F.col("OrderDate") > contractEnd, F.lit(STATUS_EXPIRED_CONTRACT))
        .when(F.col("ExpectedUnitPricePerOuter") > F.col("ContractPricePerOuter") * toleranceFactor, F.lit(STATUS_PRICE_LEAKAGE))
        .otherwise(F.lit(STATUS_COMPLIANT))
    )
    leakage = (
        F.when(F.col("ContractPricePerOuter").isNull(), zeroMoney())
        .when(F.col("ExpectedUnitPricePerOuter") > F.col("ContractPricePerOuter"),
              money((F.col("ExpectedUnitPricePerOuter") - F.col("ContractPricePerOuter")) * F.col("OrderedOuters")))
        .otherwise(zeroMoney())
    )
    return withPreferred.withColumn("ComplianceStatusCode", status).withColumn("LeakageAmount", leakage).select(
        "PurchaseOrderNumber", "LineNumber", "SupplierId", "CategoryCode", "OrderDate", "OrderedAmount",
        "ContractNumber", "ContractPricePerOuter", "ComplianceStatusCode", "LeakageAmount",
    )


def measureLeakage(workDf: DataFrame) -> tuple[object, int]:
    """Execute SQL `Measure Leakage` -> (LeakageAmount, ExpiredContractCount)."""
    row = workDf.agg(
        F.coalesce(F.sum("LeakageAmount"), zeroMoney()).alias("Leakage"),
        F.sum(F.when(F.col("ComplianceStatusCode") == STATUS_EXPIRED_CONTRACT, 1).otherwise(0)).alias("ExpiredContracts"),
    ).collect()[0]
    return row["Leakage"], int(row["ExpiredContracts"] or 0)


def summariseComplianceBySupplier(workDf: DataFrame) -> DataFrame:
    """The derived table in `Publish Compliance To Scorecard`, one row per supplier."""
    summary = workDf.groupBy("SupplierId").agg(
        F.sum("OrderedAmount").alias("TotalAmount"),
        F.sum(F.when(F.col("ComplianceStatusCode") != STATUS_COMPLIANT, F.col("OrderedAmount")).otherwise(zeroMoney())).alias("OffContractAmount"),
        F.sum("LeakageAmount").alias("LeakageAmount"),
    )
    compliancePct = F.when(F.col("TotalAmount") == 0, F.lit(100)).otherwise(
        (F.col("TotalAmount") - F.col("OffContractAmount")) * 100 / F.col("TotalAmount")
    )
    return summary.withColumn("CompliancePercent", compliancePct.cast("decimal(9,4)")) \
        .withColumn("OffContractAmount", money(F.col("OffContractAmount"))) \
        .withColumn("LeakageAmount", money(F.col("LeakageAmount")))


def nonCompliantLines(workDf: DataFrame) -> DataFrame:
    """Execute SQL `Raise Non Compliant Lines`: the rows that go to etl.RejectedRecord."""
    return workDf.where(F.col("ComplianceStatusCode") != STATUS_COMPLIANT).withColumn(
        "BusinessKey", F.concat_ws("|", F.col("PurchaseOrderNumber"), F.col("LineNumber").cast("string"))
    )
