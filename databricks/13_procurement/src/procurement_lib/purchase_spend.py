"""PRC_Load_PurchaseSpend: purchase order spend with contract linkage and spend classification."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .common import FAR_FUTURE, MONEY, money, nullSafeDiv, withBatchColumns, zeroMoney

SPEND_CLASS_ONCONTRACT = "ONCONTRACT"
SPEND_CLASS_OFFCONTRACT = "OFFCONTRACT"
SPEND_CLASS_MAVERICK = "MAVERICK"

EXCLUDED_ORDER_STATUSES = ("CANC", "DRAFT")

WORK_COLUMNS = [
    "PurchaseOrderLineId", "PurchaseOrderNumber", "LineNumber", "SupplierId", "OrderDate",
    "BuyerPersonId", "RegionCode", "CurrencyCode", "StockItemId", "OrderedOuters",
    "ExpectedUnitPricePerOuter", "OrderedAmount", "ReceivedOuters", "IsOrderLineFinalized",
    "CategoryCode", "ContractNumber", "ContractPricePerOuter", "ContractStartDate",
    "ContractEndDate", "ResolvedContractNumber", "PriceVariancePercent", "SpendClassCode",
    "ContractedAmount", "SavingsAmount",
]


def resolveContractNumber(contractNumber: Column, legacyContractRef: Column) -> Column:
    """The ERP contract key changed format in 2013 but the old numbers were never migrated.

    Mirrors the CASE in SPEND_SQL: a matched contract wins, then a modern `C-` reference,
    then a five-digit legacy reference is re-shaped as `C-#####`; anything else is unresolved.
    """
    return (
        F.when(contractNumber.isNotNull(), contractNumber)
        .when(legacyContractRef.like("C-%"), legacyContractRef)
        .when(legacyContractRef.rlike(r"^[0-9]{5}$"), F.concat(F.lit("C-"), legacyContractRef))
        .otherwise(F.lit(None).cast("string"))
    )


def buildPurchaseSpendLines(poLineDf: DataFrame, poHeaderDf: DataFrame, contractDf: DataFrame,
                            batchId: int, categoryScope: str = "ALL") -> DataFrame:
    """OLE DB source `stg PurchaseOrderLine` (SPEND_SQL) as a DataFrame.

    Header join is inner; contract join is left on supplier + category with the order date
    inside the contract validity window (open-ended contracts run to 9999-12-31). Cancelled
    and draft orders are excluded; the load is scoped to the current batch.
    """
    pol = poLineDf.alias("pol")
    poh = poHeaderDf.alias("poh")
    vc = contractDf.alias("vc")

    contractEnd = F.coalesce(F.col("vc.ContractEndDate"), F.to_date(F.lit(FAR_FUTURE)))
    joined = (
        pol.join(poh, F.col("poh.PurchaseOrderNumber") == F.col("pol.PurchaseOrderNumber"), "inner")
        .join(
            vc,
            (F.col("vc.SupplierId") == F.col("poh.SupplierId"))
            & (F.col("vc.CategoryCode") == F.col("pol.CategoryCode"))
            & (F.col("poh.OrderDate") >= F.col("vc.ContractStartDate"))
            & (F.col("poh.OrderDate") <= contractEnd),
            "left",
        )
        .where(F.col("pol.LoadBatchId") == F.lit(int(batchId)))
        .where(~F.col("poh.OrderStatusCode").isin(*EXCLUDED_ORDER_STATUSES))
    )
    if categoryScope and categoryScope.upper() != "ALL":
        joined = joined.where(F.col("pol.CategoryCode") == F.lit(categoryScope))

    return joined.select(
        F.col("pol.PurchaseOrderLineId").alias("PurchaseOrderLineId"),
        F.col("pol.PurchaseOrderNumber").alias("PurchaseOrderNumber"),
        F.col("pol.LineNumber").alias("LineNumber"),
        F.col("poh.SupplierId").alias("SupplierId"),
        F.col("poh.OrderDate").alias("OrderDate"),
        F.col("poh.BuyerPersonId").alias("BuyerPersonId"),
        F.col("poh.RegionCode").alias("RegionCode"),
        F.col("poh.CurrencyCode").alias("CurrencyCode"),
        F.col("pol.StockItemId").alias("StockItemId"),
        F.col("pol.OrderedOuters").alias("OrderedOuters"),
        money(F.col("pol.ExpectedUnitPricePerOuter")).alias("ExpectedUnitPricePerOuter"),
        money(F.col("pol.OrderedOuters") * F.col("pol.ExpectedUnitPricePerOuter")).alias("OrderedAmount"),
        F.col("pol.ReceivedOuters").alias("ReceivedOuters"),
        F.col("pol.IsOrderLineFinalized").alias("IsOrderLineFinalized"),
        F.col("pol.CategoryCode").alias("CategoryCode"),
        F.col("vc.ContractNumber").alias("ContractNumber"),
        money(F.col("vc.ContractPricePerOuter")).alias("ContractPricePerOuter"),
        F.col("vc.ContractStartDate").alias("ContractStartDate"),
        F.col("vc.ContractEndDate").alias("ContractEndDate"),
        resolveContractNumber(F.col("vc.ContractNumber"), F.col("pol.LegacyContractRef")).alias("ResolvedContractNumber"),
    )


def classifySpend(df: DataFrame) -> DataFrame:
    """Derived column `Classify Spend`: price variance vs contract and the spend class split."""
    contractPrice = F.col("ContractPricePerOuter")
    priceVariance = F.when(contractPrice.isNull() | (contractPrice == 0), zeroMoney()).otherwise(
        money((F.col("ExpectedUnitPricePerOuter") - contractPrice) * 100 / contractPrice)
    )
    spendClass = (
        F.when(F.col("ResolvedContractNumber").isNull(), F.lit(SPEND_CLASS_MAVERICK))
        .when(contractPrice.isNull(), F.lit(SPEND_CLASS_OFFCONTRACT))
        .otherwise(F.lit(SPEND_CLASS_ONCONTRACT))
    )
    return df.withColumn("PriceVariancePercent", priceVariance).withColumn("SpendClassCode", spendClass)


def deriveSavings(df: DataFrame) -> DataFrame:
    """Derived column `Derive Savings`: contracted amount and savings against the contract price."""
    contractPrice = F.col("ContractPricePerOuter")
    contractedAmount = F.when(contractPrice.isNull(), F.col("OrderedAmount")).otherwise(
        money(F.col("OrderedOuters") * contractPrice)
    )
    savings = F.when(contractPrice.isNull(), zeroMoney()).otherwise(
        money(F.col("OrderedOuters") * contractPrice - F.col("OrderedAmount"))
    )
    return df.withColumn("ContractedAmount", contractedAmount).withColumn("SavingsAmount", savings)


def currentSupplierKeys(dimSupplierDf: DataFrame, asOf: Column | None = None) -> DataFrame:
    """Lookup `Lookup Supplier Key`: current supplier rows keyed by the WWI supplier id."""
    asOf = asOf if asOf is not None else F.current_timestamp()
    return (
        dimSupplierDf.where(F.col("valid_to") > asOf)
        .select(F.col("wwi_supplier_id").alias("SupplierId"), F.col("supplier_key").alias("SupplierKey"))
        .dropDuplicates(["SupplierId"])
    )


def lookupSupplierKey(df: DataFrame, supplierKeysDf: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Lookup with redirect-on-no-match: returns (matched, rejected)."""
    joined = df.join(supplierKeysDf, "SupplierId", "left")
    matched = joined.where(F.col("SupplierKey").isNotNull())
    rejected = joined.where(F.col("SupplierKey").isNull()).drop("SupplierKey").withColumn(
        "RejectReasonCode", F.lit("UNKNOWN_SUPPLIER")
    )
    return matched, rejected


def measureMaverickSpend(workDf: DataFrame) -> tuple[int, object]:
    """Execute SQL `Measure Maverick Spend` -> (OffContractCount, MaverickSpendAmount)."""
    row = workDf.agg(
        F.sum(F.when(F.col("SpendClassCode") != SPEND_CLASS_ONCONTRACT, 1).otherwise(0)).alias("OffContractLines"),
        F.coalesce(
            F.sum(F.when(F.col("SpendClassCode") == SPEND_CLASS_MAVERICK, F.col("OrderedAmount")).otherwise(zeroMoney())),
            zeroMoney(),
        ).alias("MaverickAmount"),
    ).collect()[0]
    return int(row["OffContractLines"] or 0), row["MaverickAmount"]


def toFactPurchase(workDf: DataFrame, batchId: int, loadedAtUtc: datetime | None = None) -> DataFrame:
    """Shape work.PurchaseSpendLine rows for the MERGE into gold.fact_purchase.

    Stands in for Integration.usp_PostPurchaseSpend, which is called by the package but is
    not present in sqlserver/; see the mapping doc.
    """
    df = withBatchColumns(workDf, batchId, loadedAtUtc)
    return df.select(
        F.col("OrderDate").alias("date_key"),
        F.col("SupplierKey").alias("supplier_key"),
        F.col("StockItemId").alias("wwi_stock_item_id"),
        F.col("PurchaseOrderLineId").alias("wwi_purchase_order_line_id"),
        F.col("PurchaseOrderNumber").alias("purchase_order_number"),
        F.col("LineNumber").alias("purchase_order_line_number"),
        F.col("BuyerPersonId").cast("string").alias("buyer_code"),
        F.col("RegionCode").alias("region_code"),
        F.col("CurrencyCode").alias("transaction_currency_code"),
        F.col("CategoryCode").alias("category_code"),
        F.col("OrderedOuters").alias("ordered_outers"),
        F.col("ReceivedOuters").alias("received_outers"),
        F.col("IsOrderLineFinalized").alias("is_order_finalized"),
        F.col("ExpectedUnitPricePerOuter").alias("unit_cost"),
        F.col("OrderedAmount").alias("extended_cost"),
        F.col("ResolvedContractNumber").alias("contract_number"),
        F.col("ContractPricePerOuter").alias("contract_price_per_outer"),
        F.col("PriceVariancePercent").alias("price_variance_percent"),
        F.col("SpendClassCode").alias("spend_class_code"),
        F.col("ContractedAmount").alias("contracted_amount"),
        F.col("SavingsAmount").alias("savings_amount"),
        F.col("BatchId").alias("batch_id"),
        F.col("LoadedAtUtc").alias("load_datetime"),
    )
