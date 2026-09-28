"""PRC_Export_SupplierStatement: monthly supplier statement extract in the legacy fixed layout."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .common import money

STATEMENT_FILE_PREFIX = "supplier_statement_"
STATEMENT_FILE_SUFFIX = ".csv"

# Column order of the extract; StatementLineText is the legacy fixed-layout key.
STATEMENT_FILE_COLUMNS = [
    "StatementLineText", "SupplierId", "SupplierName", "RegionCode", "StatementPeriod",
    "TransactionTypeCode", "TransactionDate", "TransactionReference", "TransactionAmount",
    "CurrencyCode", "VatAmount", "RunningBalance", "IncludesVatBlock",
]


def statementFileName(statementPeriod: str) -> str:
    """Expression task `Build Statement File Name`."""
    return "%s%s%s" % (STATEMENT_FILE_PREFIX, statementPeriod, STATEMENT_FILE_SUFFIX)


def buildStatementLines(factDf: DataFrame, dimSupplierDf: DataFrame, statementPeriod: str,
                        excludeSelfBilling: bool) -> DataFrame:
    """Execute SQL `Build Statement Lines` -> work.SupplierStatementLine rows.

    Period filter is the yyyy-MM of the transaction date; self-billing suppliers drop out when
    the flag is set; VAT amount is carried for EU suppliers only.
    """
    t = factDf.alias("t")
    s = dimSupplierDf.alias("s")
    df = t.join(s, F.col("s.supplier_key") == F.col("t.supplier_key"), "inner").where(
        F.date_format(F.col("t.transaction_date"), "yyyy-MM") == F.lit(statementPeriod)
    )
    if excludeSelfBilling:
        df = df.where(F.coalesce(F.col("s.is_self_billing").cast("boolean"), F.lit(False)) == F.lit(False))
    return df.select(
        F.col("s.wwi_supplier_id").alias("SupplierId"),
        F.col("s.supplier").alias("SupplierName"),
        F.col("s.region_code").alias("RegionCode"),
        F.lit(statementPeriod).alias("StatementPeriod"),
        F.col("t.transaction_type_code").alias("TransactionTypeCode"),
        F.col("t.transaction_date").alias("TransactionDate"),
        F.col("t.transaction_reference").alias("TransactionReference"),
        money(F.col("t.transaction_amount")).alias("TransactionAmount"),
        F.col("t.currency_code").alias("CurrencyCode"),
        F.when(F.col("s.region_code") == "EU", money(F.col("t.tax_amount"))).otherwise(F.lit(None).cast("decimal(18,2)")).alias("VatAmount"),
        F.col("t.supplier_transaction_key").alias("SupplierStatementLineId"),
    )


def computeRunningBalances(linesDf: DataFrame) -> DataFrame:
    """Execute SQL `Compute Statement Balances`: per-supplier running total by date, then line id."""
    w = Window.partitionBy("SupplierId").orderBy("TransactionDate", "SupplierStatementLineId").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    return linesDf.withColumn("RunningBalance", money(F.sum("TransactionAmount").over(w)))


def formatStatementRows(linesDf: DataFrame) -> DataFrame:
    """Derived column `Format Statement Row`.

    RIGHT("0000000000" + SupplierId, 10) + (DT_WSTR,10)TransactionTypeCode + (DT_WSTR,30)TransactionReference.
    DT_WSTR casts truncate but do not pad, which is preserved here.
    """
    lineText = F.concat(
        F.lpad(F.col("SupplierId").cast("string"), 10, "0"),
        F.coalesce(F.substring(F.col("TransactionTypeCode"), 1, 10), F.lit("")),
        F.coalesce(F.substring(F.col("TransactionReference"), 1, 30), F.lit("")),
    )
    return linesDf.withColumn("StatementLineText", lineText).withColumn(
        "IncludesVatBlock", F.col("RegionCode") == F.lit("EU")
    )


def orderedStatementRows(df: DataFrame) -> DataFrame:
    """The source ORDER BY plus the tie-break the running balance already used."""
    return df.orderBy("SupplierId", "TransactionDate", "SupplierStatementLineId").select(*STATEMENT_FILE_COLUMNS)


def countStatements(linesDf: DataFrame) -> int:
    """Execute SQL `Count Statements`."""
    return linesDf.select("SupplierId").distinct().count()
