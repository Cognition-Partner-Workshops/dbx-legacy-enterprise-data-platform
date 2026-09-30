"""Package name -> implementation dispatch (one entry per SSIS package in the group)."""
from pyspark.sql import SparkSession

from sales_o2c.config import RunContext
from sales_o2c.dq import runDqInvoiceLineScreen, runDqOrderLineScreen
from sales_o2c.extract import runExtract
from sales_o2c.fact_order import runFactLoadOrder
from sales_o2c.fact_sale import runFactDedupSale, runFactSaleRegion
from sales_o2c.fact_transaction import runFactLoadCustomerTransaction, runFactLoadTransaction
from sales_o2c.runtime import runPackage
from sales_o2c.snapshot_agg import runAggRefreshDailySalesSummary, runFactLoadDailySalesSnapshot
from sales_o2c.staging import runStgLoadOrder, runStgLoadSale

IMPLEMENTATIONS = {
    "EXT_SQL_Orders": lambda s, c: runExtract(s, c, "EXT_SQL_Orders"),
    "EXT_SQL_OrderLines": lambda s, c: runExtract(s, c, "EXT_SQL_OrderLines"),
    "EXT_SQL_Invoices": lambda s, c: runExtract(s, c, "EXT_SQL_Invoices"),
    "EXT_SQL_InvoiceLines": lambda s, c: runExtract(s, c, "EXT_SQL_InvoiceLines"),
    "EXT_SQL_CustomerTransactions": lambda s, c: runExtract(s, c, "EXT_SQL_CustomerTransactions"),
    "STG_Load_Order": runStgLoadOrder,
    "STG_Load_Sale": runStgLoadSale,
    "DQ_OrderLine_Screen": runDqOrderLineScreen,
    "DQ_InvoiceLine_Screen": runDqInvoiceLineScreen,
    "FACT_NA_Load_Sale": lambda s, c: runFactSaleRegion(s, c, "NA"),
    "FACT_EU_Load_Sale": lambda s, c: runFactSaleRegion(s, c, "EU"),
    "FACT_APAC_Load_Sale": lambda s, c: runFactSaleRegion(s, c, "APAC"),
    "FACT_Dedup_Sale": runFactDedupSale,
    "FACT_Load_Order": runFactLoadOrder,
    "FACT_Load_CustomerTransaction": runFactLoadCustomerTransaction,
    "FACT_Load_Transaction": runFactLoadTransaction,
    "FACT_Load_DailySalesSnapshot": runFactLoadDailySalesSnapshot,
    "AGG_Refresh_DailySalesSummary": runAggRefreshDailySalesSummary,
}


def executePackage(spark: SparkSession, ctx: RunContext, packageName: str) -> dict:
    impl = IMPLEMENTATIONS[packageName]
    return runPackage(spark, ctx, packageName, lambda: impl(spark, ctx))
