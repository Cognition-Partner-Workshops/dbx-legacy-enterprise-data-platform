"""Idempotent seed data (``03_seed_control_data.sql`` and ``05_seed_data_quality_rules.sql``).

Each seed is applied as a Delta ``MERGE`` keyed on the legacy unique constraint, so the bootstrap
job can be re-run at any time. Data-quality ``RuleExpression`` values are the Spark SQL
translation of the legacy T-SQL WHERE clauses (``N'..'`` -> ``'..'``, ``LTRIM(RTRIM())`` ->
``trim()``, ``COUNT_BIG`` -> ``count``, ``CAST(SYSUTCDATETIME() AS DATE)`` -> ``current_date()``,
``LEN`` -> ``length``, BIT comparisons -> ``CAST(x AS INT) = 0/1``). Legacy object references
(``stg.Customer`` ...) are kept and resolved at evaluation time by
:func:`dbx_etl_common.naming.translateLegacyReferences`; the outer object is aliased with
:func:`dbx_etl_common.naming.aliasFor` (``stg.OrderLine`` -> ``OrderLine``).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence, Tuple

from .naming import controlTable


@dataclass(frozen=True)
class Seed:
    table: str
    columns: Tuple[str, ...]
    keyColumns: Tuple[str, ...]
    rows: Tuple[Tuple[Any, ...], ...]
    updateColumns: Tuple[str, ...]
    insertOnlyColumns: Tuple[Tuple[str, str], ...] = ()   # (column, SQL literal) applied on insert only
    touchColumn: str = ""                                  # timestamp column set to current_timestamp() on update


SOURCE_SYSTEMS = Seed(
    table="source_system",
    columns=("SourceSystemCode", "SourceSystemName", "Platform", "RegionCode", "ConnectionParameter", "DefaultTimeZone"),
    keyColumns=("SourceSystemCode",),
    updateColumns=("SourceSystemName", "Platform", "RegionCode", "ConnectionParameter", "DefaultTimeZone"),
    rows=(
        ("ORA_ERP", "Oracle ERP - master data, procurement and finance", "Oracle", "GLOBAL", "OracleHost", "UTC"),
        ("ORA_ERP_NA", "Oracle ERP - North America ledger", "Oracle", "NA", "OracleHost", "America/Chicago"),
        ("ORA_ERP_EU", "Oracle ERP - Europe ledger", "Oracle", "EU", "OracleHost", "Europe/London"),
        ("ORA_ERP_AP", "Oracle ERP - APAC ledger", "Oracle", "APAC", "OracleHost", "Asia/Singapore"),
        ("WWI_OLTP", "WideWorldImporters OLTP", "SQL Server", "GLOBAL", "SqlServerOltpDb", "UTC"),
        ("WWI_WEB", "WideWorldImporters ecommerce platform", "SQL Server", "GLOBAL", "SqlServerOltpDb", "UTC"),
        ("PARTNER_FL", "Partner sales flat-file feed", "File", "GLOBAL", "InboundFileRoot", "UTC"),
        ("CARRIER_FL", "Carrier delivery confirmation feed", "File", "GLOBAL", "InboundFileRoot", "UTC"),
        ("BANK_FL", "Bank payment clearing feed", "File", "GLOBAL", "InboundFileRoot", "UTC"),
        ("FX_FEED", "Daily FX rate feed", "File", "GLOBAL", "InboundFileRoot", "UTC"),
        ("MANUAL", "Manual adjustments and corrections", "Manual", "GLOBAL", None, "UTC"),
    ),
)

CONFIGURATION = Seed(
    table="configuration",
    columns=("ConfigurationKey", "EnvironmentCode", "ConfigurationValue", "ValueDataType", "Description", "IsSensitive"),
    keyColumns=("ConfigurationKey", "EnvironmentCode"),
    updateColumns=("ConfigurationValue", "ValueDataType", "Description", "IsSensitive"),
    touchColumn="ModifiedAtUtc",
    rows=(
        ("EnvironmentCode", "ALL", "DEV", "String", "Deployment environment code (DEV, TEST, PROD).", False),
        ("WatermarkEpoch", "ALL", "1900-01-01T00:00:00", "Date", "Lower bound used the first time an object is extracted.", False),
        ("MaxRejectPercent", "ALL", "5", "Decimal", "Reject share of rows read above which a package execution is failed.", False),
        ("ReconAbsoluteTolerance", "ALL", "0", "Int", "Absolute row-count variance tolerated by the reconciliation gate.", False),
        ("ReconPercentTolerance", "ALL", "0.0", "Decimal", "Percentage row-count variance tolerated by the reconciliation gate.", False),
        ("DefaultBatchSize", "ALL", "100000", "Int", "Fast-load commit size for OLE DB destinations (Delta: target rows per write batch).", False),
        ("SourceQueryTimeoutSeconds", "ALL", "3600", "Int", "Command timeout applied to source extracts.", False),
        ("OracleFetchArraySize", "ALL", "10000", "Int", "Array fetch size for the Oracle provider (JDBC fetchsize).", False),
        ("LateArrivingDimensionDays", "ALL", "7", "Int", "Window in which a late-arriving dimension member is re-keyed into facts.", False),
        ("EarlyArrivingFactHoldDays", "ALL", "3", "Int", "How long an early-arriving fact waits on the unknown member before escalation.", False),
        ("SnapshotRetentionMonths", "ALL", "36", "Int", "Retention for periodic snapshot facts.", False),
        ("ControlTableRetentionDays", "ALL", "400", "Int", "Retention for etl.package_execution, etl.row_count_audit and etl.error_log.", False),
        ("RejectRetentionDays", "ALL", "180", "Int", "Retention for etl.rejected_record.", False),
        ("InboundFileRoot", "ALL", "/Volumes/wwi/landing/inbound", "String", "Root of the inbound file drop (Unity Catalog volume). Overridden per environment.", False),
        ("ArchiveFileRoot", "ALL", "/Volumes/wwi/landing/archive", "String", "Root of the processed-file archive (Unity Catalog volume).", False),
        ("RejectFileRoot", "ALL", "/Volumes/wwi/landing/reject", "String", "Root of the malformed-record reject drop (Unity Catalog volume).", False),
        ("DefaultReportingCurrency", "ALL", "USD", "String", "Currency all warehouse monetary measures are converted to.", False),
        ("FxRateTolerancePercent", "ALL", "2.0", "Decimal", "Day-over-day FX move above which the rate refresh warns.", False),
        ("UnknownMemberKey", "ALL", "0", "Int", "Surrogate key of the unknown member in every dimension.", False),
        ("IntradayCycleMinutes", "ALL", "30", "Int", "Interval of the intraday sales cycle.", False),
        ("OraclePasswordSecretName", "ALL", "ORACLE_PASSWORD", "String", "Name of the secret (scope wwi) holding the Oracle password.", True),
        ("SqlServerPasswordSecretName", "ALL", "SQLSERVER_PASSWORD", "String", "Name of the secret (scope wwi) holding the SQL Server password.", True),
        ("EnvironmentCode", "PROD", "PROD", "String", "Production environment code.", False),
        ("MaxRejectPercent", "PROD", "1", "Decimal", "Production tolerates far fewer rejects than lower environments.", False),
        ("DefaultBatchSize", "PROD", "250000", "Int", "Larger commit size on production hardware.", False),
    ),
)

RECONCILIATION_EXEMPTIONS = Seed(
    table="reconciliation_exemption",
    columns=("ObjectName", "Reason"),
    keyColumns=("ObjectName",),
    updateColumns=("Reason",),
    rows=(
        ("work.CustomerDedup", "Deduplication collapses rows by design."),
        ("Dimension.Customer", "SCD Type 2 expands one source row into multiple versions."),
        ("Dimension.Stock Item", "SCD Type 2 expands one source row into multiple versions."),
        ("Dimension.Supplier", "SCD Type 2 expands one source row into multiple versions."),
        ("Aggregate.Sales Daily", "Aggregation reduces the row count by design."),
        ("Aggregate.Sales Monthly", "Aggregation reduces the row count by design."),
        ("Aggregate.Daily Inventory Health", "Aggregation reduces the row count by design."),
        ("Aggregate.Customer 360", "Aggregation reduces the row count by design."),
        ("Fact.Stock Holding", "Periodic snapshot generates rows independent of the source count."),
        ("Fact.Order Fulfilment", "Accumulating snapshot updates existing rows rather than inserting."),
    ),
)

_DQ_COLUMNS = ("RuleCode", "RuleGroupCode", "ObjectName", "RuleName", "RuleExpression", "DimensionCode",
               "SeverityCode", "ThresholdValue", "RegionCode", "SourceSystemCode")

DATA_QUALITY_RULES = Seed(
    table="data_quality_rule",
    columns=_DQ_COLUMNS,
    keyColumns=("RuleCode",),
    updateColumns=("RuleGroupCode", "ObjectName", "RuleName", "RuleExpression", "DimensionCode",
                   "SeverityCode", "ThresholdValue", "RegionCode", "SourceSystemCode"),
    insertOnlyColumns=(("OwnerName", "'Data Stewardship'"),),
    touchColumn="UpdatedAtUtc",
    rows=(
        ("CUST_NULL_NAME", "CUSTOMER", "stg.Customer", "Customer name must be present",
         "CustomerName IS NULL OR trim(CustomerName) = ''", "Completeness", "FAIL", 0, None, None),
        ("CUST_DUP_BKEY", "CUSTOMER", "stg.Customer", "Business key must be unique in the batch",
         "CustomerBusinessKey IN (SELECT CustomerBusinessKey FROM stg.Customer GROUP BY CustomerBusinessKey HAVING count(*) > 1)",
         "Uniqueness", "FAIL", 0, None, None),
        ("CUST_NO_CATEGORY", "CUSTOMER", "stg.Customer", "Category must resolve to the conformed set",
         "CustomerCategoryCode IS NULL", "Validity", "WARN", 250, None, None),
        ("CUST_BAD_POSTCODE_NA", "CUSTOMER", "stg.Customer", "US and CA postal codes must match the country format",
         "CountryCode IN ('US', 'CA') AND CAST(PostalCodeIsValid AS INT) = 0", "Validity", "WARN", 50, "NA", None),
        ("CUST_BAD_POSTCODE_EU", "CUSTOMER", "stg.Customer", "EU postal codes must match the country format",
         "RegionCode = 'EU' AND CAST(PostalCodeIsValid AS INT) = 0", "Validity", "WARN", 400, "EU", None),
        ("CUST_BAD_POSTCODE_AP", "CUSTOMER", "stg.Customer", "APAC postal codes must match the country format",
         "RegionCode = 'APAC' AND CAST(PostalCodeIsValid AS INT) = 0", "Validity", "WARN", 2000, "APAC", None),
        ("CUST_FUTURE_OPENED", "CUSTOMER", "stg.Customer", "Account opened date cannot be in the future",
         "AccountOpenedDate > current_date()", "Validity", "FAIL", 0, None, "ORA_ERP"),
        ("SUPP_NULL_NAME", "SUPPLIER", "stg.Supplier", "Supplier name must be present",
         "SupplierName IS NULL OR trim(SupplierName) = ''", "Completeness", "FAIL", 0, None, None),
        ("SUPP_NO_TAXID_EU", "SUPPLIER", "stg.Supplier", "EU suppliers must carry a VAT registration",
         "RegionCode = 'EU' AND (TaxRegistrationNumber IS NULL OR length(TaxRegistrationNumber) < 8)",
         "Completeness", "FAIL", 5, "EU", None),
        ("SUPP_NO_PAYMENT_TERM", "SUPPLIER", "stg.Supplier", "Payment terms must resolve",
         "PaymentTermsCode IS NULL", "Validity", "WARN", 25, None, None),
        ("OL_NEG_QTY", "ORDERLINE", "stg.OrderLine", "Ordered quantity must be positive",
         "Quantity <= 0", "Validity", "FAIL", 0, None, None),
        ("OL_NEG_PRICE", "ORDERLINE", "stg.OrderLine", "Unit price must not be negative",
         "UnitPrice < 0", "Validity", "FAIL", 0, None, None),
        ("OL_ORPHAN_ORDER", "ORDERLINE", "stg.OrderLine", "Every line must have a header",
         "NOT EXISTS (SELECT 1 FROM stg.Order AS o WHERE o.OrderBusinessKey = OrderLine.OrderBusinessKey)",
         "Integrity", "FAIL", 0, None, None),
        ("OL_DISCOUNT_RANGE", "ORDERLINE", "stg.OrderLine", "Discount percentage must be between 0 and 90",
         "DiscountPercentage < 0 OR DiscountPercentage > 90", "Validity", "WARN", 10, None, None),
        ("OL_EXTENDED_MISMATCH", "ORDERLINE", "stg.OrderLine", "Extended amount must agree with quantity times net price",
         "abs(ExtendedAmount - (Quantity * UnitPrice * (1 - DiscountPercentage / 100.0))) > 0.01",
         "Accuracy", "WARN", 100, None, None),
        ("IL_NO_TAX_RATE", "INVOICE", "stg.InvoiceLine", "Tax rate must resolve from the jurisdiction",
         "TaxRate IS NULL", "Completeness", "FAIL", 0, None, None),
        ("IL_TAX_MISMATCH_NA", "INVOICE", "stg.InvoiceLine", "NA sales tax must agree with the rate applied",
         "RegionCode = 'NA' AND abs(TaxAmount - (ExtendedAmount * TaxRate / 100.0)) > 0.02",
         "Accuracy", "FAIL", 0, "NA", None),
        ("IL_TAX_MISMATCH_EU", "INVOICE", "stg.InvoiceLine", "EU VAT must agree unless reverse charged",
         "RegionCode = 'EU' AND CAST(ReverseChargeFlag AS INT) = 0 AND abs(TaxAmount - (ExtendedAmount * TaxRate / 100.0)) > 0.02",
         "Accuracy", "WARN", 75, "EU", None),
        ("IL_MARGIN_NEGATIVE", "INVOICE", "stg.InvoiceLine", "Margin below cost needs review",
         "ExtendedAmount < CostAmount", "Plausibility", "WARN", 500, None, None),
        ("PAY_UNAPPLIED", "PAYMENT", "stg.SupplierPayment", "Payments must be applied to an invoice",
         "InvoiceBusinessKey IS NULL", "Integrity", "WARN", 40, None, None),
        ("PAY_FX_MISSING", "PAYMENT", "stg.SupplierPayment", "Non-USD payments need an FX rate for the value date",
         "CurrencyCode <> 'USD' AND FxRateToUsd IS NULL", "Completeness", "FAIL", 0, None, None),
        ("PAY_FUTURE_DATE", "PAYMENT", "stg.SupplierPayment", "Payment date cannot be in the future",
         "PaymentDate > current_date()", "Validity", "FAIL", 0, None, "ORA_ERP"),
        ("REF_UNMAPPED_CODE", "REFERENCE", "ref.CodeCrosswalk", "Source codes must map to the conformed set",
         "ConformedCode IS NULL", "Integrity", "WARN", 30, None, None),
        ("REF_FX_GAP", "REFERENCE", "ref.FxRateDaily", "FX rates must exist for every trading day in the window",
         "RateToUsd IS NULL", "Completeness", "FAIL", 0, None, None),
        ("REF_TAX_EXPIRED", "REFERENCE", "ref.TaxJurisdiction", "An active jurisdiction must not be expired",
         "CAST(IsActive AS INT) = 1 AND EffectiveTo < current_date()", "Validity", "WARN", 5, None, None),
        ("INV_NEG_ONHAND", "INVENTORY", "stg.StockHolding", "On hand quantity must not be negative",
         "QuantityOnHand < 0", "Plausibility", "WARN", 20, None, None),
        ("MOV_NO_REASON", "INVENTORY", "stg.Movement", "Adjustments must carry a reason code",
         "MovementTypeCode = 'ADJ' AND ReasonCode IS NULL", "Completeness", "WARN", 15, None, None),
        ("FILE_SHORT_ROW", "FILE", "err.RejectedFileRow", "Delimited rows must have the expected column count",
         "RejectReasonCode = 'COLUMN_COUNT'", "Validity", "WARN", 100, None, None),
        ("FILE_BAD_ENCODING", "FILE", "err.RejectedFileRow", "Inbound files must decode cleanly",
         "RejectReasonCode = 'ENCODING'", "Validity", "FAIL", 0, None, None),
    ),
)

SEEDS: Tuple[Seed, ...] = (SOURCE_SYSTEMS, CONFIGURATION, RECONCILIATION_EXEMPTIONS, DATA_QUALITY_RULES)


def sqlLiteral(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def renderMerge(seed: Seed, catalog: str = "${catalog}") -> str:
    """Render the seed as a standalone Delta ``MERGE`` statement."""
    target = controlTable(catalog, seed.table)
    values = ",\n".join("    (" + ", ".join(sqlLiteral(v) for v in row) + ")" for row in seed.rows)
    on = " AND ".join(f"tgt.{c} = src.{c}" for c in seed.keyColumns)
    sets = [f"tgt.{c} = src.{c}" for c in seed.updateColumns]
    if seed.touchColumn:
        sets.append(f"tgt.{seed.touchColumn} = current_timestamp()")
    insertCols = list(seed.columns) + [c for c, _ in seed.insertOnlyColumns]
    insertVals = [f"src.{c}" for c in seed.columns] + [lit for _, lit in seed.insertOnlyColumns]
    return (
        f"MERGE INTO {target} AS tgt\n"
        f"USING (SELECT * FROM VALUES\n{values}\n  AS v ({', '.join(seed.columns)})) AS src\n"
        f"ON {on}\n"
        f"WHEN MATCHED THEN UPDATE SET {', '.join(sets)}\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(insertCols)}) VALUES ({', '.join(insertVals)});"
    )


def applySeed(spark: Any, catalog: str, seed: Seed) -> None:
    spark.sql(renderMerge(seed, catalog))


def applySeeds(spark: Any, catalog: str, seeds: Sequence[Seed] = SEEDS) -> None:
    """Apply every seed (idempotent)."""
    for seed in seeds:
        applySeed(spark, catalog, seed)
