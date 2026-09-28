"""Delta schemas of the tables owned by the WWI_Sales notebooks.

Legacy object -> Delta table (naming contract: work.X -> silver.work_x, err.X ->
silver.err_x, Aggregate.X -> gold.agg_x). Column names keep the legacy PascalCase
(spaces removed) so validation/runtime/*.sql ports with minimal edits.
"""
from pyspark.sql import types as T

from sales_common import MONEY, RATE

S = T.StringType()
I = T.IntegerType()
L = T.LongType()
D = T.DateType()
B = T.BooleanType()
TS = T.TimestampType()


def _f(name, dtype, nullable=True):
    return T.StructField(name, dtype, nullable)


_COMMISSION_BASE = [
    _f("SaleLineId", S), _f("InvoiceNumber", S), _f("InvoiceDate", D),
    _f("SalespersonPersonId", S), _f("CustomerId", S), _f("TerritoryCode", S),
    _f("PlanCode", S), _f("BaseRatePercent", MONEY), _f("CommissionPeriod", S),
    _f("CommissionAmount", MONEY), _f("RegionCode", S), _f("BatchId", L),
]

# work.CommissionNa
WORK_COMMISSION_NA = T.StructType(_COMMISSION_BASE + [
    _f("StockItemId", S), _f("QuantitySold", T.DecimalType(18, 4)), _f("ExtendedPrice", MONEY),
    _f("TaxAmount", MONEY), _f("LineProfit", MONEY), _f("CommissionableAmount", MONEY),
    _f("AcceleratorRatePercent", MONEY), _f("AcceleratorThresholdAmount", MONEY),
    _f("PlanCurrencyCode", S), _f("IsHouseAccount", B), _f("BaseCommissionAmount", MONEY),
    _f("AcceleratorCommissionAmount", MONEY), _f("HouseAccountFactor", MONEY),
])

# work.CommissionEu and work.CommissionEuHeld share the flow's column set
WORK_COMMISSION_EU = T.StructType(_COMMISSION_BASE + [
    _f("CountryCode", S), _f("CurrencyCode", S), _f("ExtendedPrice", MONEY), _f("VatAmount", MONEY),
    _f("VatRatePercent", MONEY), _f("NetCommissionableAmount", MONEY), _f("StatutoryCapAmount", MONEY),
    _f("EurConversionRate", RATE), _f("NetAmountEur", MONEY), _f("IsCashBasisCountry", B),
    _f("RawCommissionAmount", MONEY),
])
WORK_COMMISSION_EU_HELD = WORK_COMMISSION_EU

# work.CommissionApac
WORK_COMMISSION_APAC = T.StructType(_COMMISSION_BASE + [
    _f("CountryCode", S), _f("CurrencyCode", S), _f("ExtendedPrice", MONEY), _f("GstAmount", MONEY),
    _f("GstExclusiveAmount", MONEY), _f("PlanCurrencyCode", S), _f("TeamSplitPercent", MONEY),
    _f("CommissionFiscalYear", I), _f("CommissionFiscalWeek", I), _f("ConversionRate", RATE),
    _f("PlanCurrencyAmount", MONEY), _f("SplitFactor", MONEY), _f("IsPeriodBoundaryLine", B),
])

# err.CommissionApacReject (lookup no-match output of "Lookup Period Average Rate")
ERR_COMMISSION_APAC_REJECT = T.StructType([
    _f("SaleLineId", S), _f("InvoiceNumber", S), _f("InvoiceDate", D), _f("SalespersonPersonId", S),
    _f("CustomerId", S), _f("CountryCode", S), _f("TerritoryCode", S), _f("CurrencyCode", S),
    _f("ExtendedPrice", MONEY), _f("GstAmount", MONEY), _f("GstExclusiveAmount", MONEY),
    _f("PlanCode", S), _f("BaseRatePercent", MONEY), _f("PlanCurrencyCode", S),
    _f("TeamSplitPercent", MONEY), _f("CommissionPeriod", S), _f("CommissionFiscalYear", I),
    _f("CommissionFiscalWeek", I), _f("RejectReasonCode", S), _f("BatchId", L),
])

# Posting target of Integration.usp_PostCommission (procedure not in the repo; see mapping doc).
FACT_SALE_COMMISSION = T.StructType([
    _f("RegionCode", S), _f("SaleLineId", S), _f("CommissionPeriod", S), _f("InvoiceNumber", S),
    _f("InvoiceDate", D), _f("SalespersonPersonId", S), _f("CustomerId", S), _f("TerritoryCode", S),
    _f("CountryCode", S), _f("PlanCode", S), _f("PlanCurrencyCode", S), _f("CommissionableAmount", MONEY),
    _f("CommissionAmount", MONEY), _f("PostedCommissionPeriod", S), _f("BatchId", L),
    _f("PackageExecutionId", L), _f("PostedAtUtc", TS),
])

# work.QuotaAttainment
WORK_QUOTA_ATTAINMENT = T.StructType([
    _f("TerritoryCode", S), _f("RegionCode", S), _f("QuotaPeriod", S), _f("QuotaAmount", MONEY),
    _f("ActualAmount", MONEY), _f("MeasureBasisCode", S), _f("BatchId", L),
])

# Aggregate.Regional Sales Performance (columns the package actually publishes + the
# DDL's period / refresh columns that can be derived from QuotaPeriod)
AGG_REGIONAL_SALES_PERFORMANCE = T.StructType([
    _f("FiscalYear", I), _f("FiscalPeriod", I), _f("CalendarMonth", D), _f("RegionCode", S),
    _f("TerritoryCode", S), _f("QuotaPeriod", S), _f("QuotaAmount", MONEY), _f("ActualAmount", MONEY),
    _f("MeasureBasisCode", S), _f("AttainmentPercent", MONEY), _f("AttainmentBandCode", S),
    _f("RefreshBatchId", L), _f("RefreshedDatetime", TS),
])

# work.PromotionSpill (redemptions outside the attribution window)
WORK_PROMOTION_SPILL = T.StructType([
    _f("PromotionId", S), _f("PromotionCode", S), _f("PromotionName", S), _f("RegionCode", S),
    _f("StartDate", D), _f("EndDate", D), _f("DiscountTypeCode", S), _f("DiscountValue", MONEY),
    _f("BudgetAmount", MONEY), _f("SaleLineId", S), _f("InvoiceDate", D), _f("CustomerId", S),
    _f("StockItemId", S), _f("RedeemedAmount", MONEY), _f("RedemptionChannelCode", S),
    _f("AttributionEndDate", D), _f("IsInWindow", B), _f("DiscountCostAmount", MONEY), _f("BatchId", L),
])

# Aggregate.Promotion Effectiveness (columns the package publishes)
AGG_PROMOTION_EFFECTIVENESS = T.StructType([
    _f("PromotionId", S), _f("PromotionCode", S), _f("RegionCode", S), _f("RedeemedAmount", MONEY),
    _f("DiscountCostAmount", MONEY), _f("RedemptionCount", L), _f("RedeemingCustomerCount", L),
    _f("BudgetAmount", MONEY), _f("BudgetStatus", S), _f("RefreshBatchId", L), _f("RefreshedDatetime", TS),
])

# work.PartnerFeedRow / work.PartnerFeedArchive
PARTNER_FEED_COLUMNS = [
    "PartnerCode", "InvoiceNumber", "InvoiceDate", "CustomerReference", "StockItemCode",
    "Quantity", "NetAmount", "SettlementCurrencyCode", "RegionCode",
]
WORK_PARTNER_FEED_ROW = T.StructType([
    _f("PartnerCode", S), _f("InvoiceNumber", S), _f("InvoiceDate", D), _f("CustomerReference", S),
    _f("StockItemCode", S), _f("Quantity", T.DecimalType(18, 4)), _f("NetAmount", MONEY),
    _f("SettlementCurrencyCode", S), _f("RegionCode", S), _f("BatchId", L),
])
WORK_PARTNER_FEED_ARCHIVE = T.StructType(WORK_PARTNER_FEED_ROW.fields + [
    _f("ExportFileName", S), _f("ExportedAtUtc", TS), _f("PackageExecutionId", L),
])

# Reconciliation baseline supplied from SQL Server (validation/SLS_Reconcile_SalesMart.py)
RECONCILIATION_BASELINE = T.StructType([
    _f("ObjectName", S), _f("PartitionKey", S), _f("BaselineRowCount", L), _f("BaselineHash", L),
])
