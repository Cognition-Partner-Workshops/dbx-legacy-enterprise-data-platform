"""Delta tables owned by the finance project.

Only objects that no other session mirrors are created here: the finance
work/err tables (legacy work.*/err.* objects declared by the packages but
absent from sqlserver/staging), the finance reference inputs the packages read
(stg.CostAllocationRule etc., likewise absent from the staging DDL), the
reconciliation table (sqlserver/control/06_tables_reconciliation.sql) and the
new period-lock table. Gold targets are created only if they do not exist yet;
the FACT_*/AGG_* sessions own their full schema and finance columns are merged
in with schema evolution.
"""
from __future__ import annotations

from dbx_etl_common import naming

FINANCE_OWNED_TABLES = {
    # ----- silver work / err (legacy work.* / err.* of WWI_Finance) -----
    ("silver", "work_ap_aging_staging"): """
        ApInvoiceKey BIGINT, SupplierId STRING, SupplierSiteCode STRING, InvoiceNumber STRING,
        InvoiceDate DATE, DueDate DATE, CurrencyCode STRING, LedgerCode STRING, RegionCode STRING,
        InvoiceAmount DECIMAL(19,4), PaidAmount DECIMAL(19,4), OpenAmount DECIMAL(19,4),
        DaysPastDue INT, AgingBucketCode STRING, ReportableAmount DECIMAL(19,4),
        EarlyPaymentDiscountPercent DECIMAL(9,4), IsPastDue BOOLEAN, AgingBucketSort INT,
        DiscountAtRisk DECIMAL(19,4), SupplierKey BIGINT, AsOfDate DATE, BatchId BIGINT""",
    ("silver", "err_ap_aging_reject"): """
        ApInvoiceKey BIGINT, SupplierId STRING, InvoiceNumber STRING, LedgerCode STRING,
        RegionCode STRING, OpenAmount DECIMAL(19,4), RejectReasonCode STRING, BatchId BIGINT,
        RejectedAtUtc TIMESTAMP""",
    ("silver", "work_gl_held_line"): """
        GlJournalLineId BIGINT, JournalNumber STRING, JournalLineNumber INT, LedgerCode STRING,
        CostCentreCode STRING, AccountCode STRING, PostingDate DATE, AccountingPeriod STRING,
        CurrencyCode STRING, EnteredDebitAmount DECIMAL(19,4), EnteredCreditAmount DECIMAL(19,4),
        SourceSubledgerCode STRING, SourceDocumentNumber STRING,
        FunctionalDebitAmount DECIMAL(19,4), FunctionalCreditAmount DECIMAL(19,4),
        TaxRegimeCode STRING, NetAmount DECIMAL(19,4), PostingSide STRING,
        SubledgerSourceKey STRING, RequestedAccountingPeriod STRING, BatchId BIGINT,
        HeldAtUtc TIMESTAMP""",
    ("silver", "work_cost_allocation_result"): """
        AllocationRuleId INT, RuleSequence INT, SourceCostCentreCode STRING,
        TargetCostCentreCode STRING, DriverCode STRING, DriverValue DECIMAL(19,4),
        PoolAmount DECIMAL(19,4), AllocatedAmount DECIMAL(19,4), AccountingPeriod STRING,
        RuleSetCode STRING, BatchId BIGINT""",
    ("silver", "work_fx_revaluation_rate"): """
        CurrencyCode STRING, QuoteCurrencyCode STRING, RateDate DATE, RateTypeCode STRING,
        ConversionRate DECIMAL(19,8), RateSourceCode STRING, InverseRate DECIMAL(19,8),
        IsTriangulated BOOLEAN, BatchId BIGINT""",
    ("silver", "work_withholding_tax_line"): """
        ApInvoiceLineId BIGINT, ApInvoiceKey BIGINT, SupplierId STRING,
        SupplierTaxRegistrationNumber STRING, JurisdictionCode STRING, RegionCode STRING,
        LineAmount DECIMAL(19,4), TaxCode STRING, ServiceCategoryCode STRING,
        WithholdingRatePercent DECIMAL(9,4), WithholdingThresholdAmount DECIMAL(19,4),
        WithholdingAmount DECIMAL(19,4), NetPayableAmount DECIMAL(19,4), IsWithheld BOOLEAN,
        WithholdingCertificateRequired BOOLEAN, LedgerCode STRING, AccountingPeriod STRING,
        BatchId BIGINT""",
    ("silver", "err_withholding_tax_reject"): """
        ApInvoiceLineId BIGINT, ApInvoiceKey BIGINT, SupplierId STRING, JurisdictionCode STRING,
        RegionCode STRING, ServiceCategoryCode STRING, LineAmount DECIMAL(19,4),
        WithholdingAmount DECIMAL(19,4), RejectReasonCode STRING, BatchId BIGINT,
        RejectedAtUtc TIMESTAMP""",
    ("silver", "work_withholding_certificate_queue"): """
        SupplierId STRING, JurisdictionCode STRING, WithholdingAmount DECIMAL(19,4),
        QueuedAtUtc TIMESTAMP, BatchId BIGINT""",
    # ----- finance reference inputs the packages read but the staging DDL never defined -----
    ("silver", "stg_withholding_tax_rate"): """
        JurisdictionCode STRING, ServiceCategoryCode STRING, WithholdingRatePercent DECIMAL(9,4),
        TreatyRatePercent DECIMAL(9,4), WithholdingThresholdAmount DECIMAL(19,4),
        EffectiveFrom DATE, EffectiveTo DATE, BatchId BIGINT""",
    ("silver", "stg_cost_allocation_rule"): """
        AllocationRuleId INT, RuleSetCode STRING, RuleSequence INT, SourceCostCentreCode STRING,
        DriverCode STRING, AllocationBasisCode STRING, IsActive BOOLEAN, BatchId BIGINT""",
    ("silver", "stg_cost_allocation_target"): """
        AllocationRuleId INT, TargetCostCentreCode STRING, DriverValue DECIMAL(19,4),
        BatchId BIGINT""",
    ("silver", "stg_cost_centre_balance"): """
        CostCentreCode STRING, AccountingPeriod STRING, Amount DECIMAL(19,4), LedgerCode STRING,
        BatchId BIGINT""",
    # ----- finance mart (legacy Integration.usp_RefreshApAgingSummary output) -----
    ("gold", "agg_ap_aging_summary"): """
        LedgerCode STRING, AgingBucketCode STRING, AgingBucketSort INT, OpenItemCount BIGINT,
        OpenAmount DECIMAL(19,4), ReportableAmount DECIMAL(19,4), DiscountAtRisk DECIMAL(19,4),
        AsOfDate DATE, AccountingPeriod STRING, BatchId BIGINT""",
    # ----- control-side finance tables -----
    ("etl", "reconciliation_result"): """
        ReconciliationResultId BIGINT GENERATED ALWAYS AS IDENTITY, BatchId BIGINT NOT NULL,
        PackageExecutionId BIGINT, ReconciliationName STRING NOT NULL, ObjectName STRING,
        SourceKey STRING, LedgerCode STRING, AccountingPeriod STRING, AccountCode STRING,
        RegionCode STRING, SourceAmount DECIMAL(19,4), TargetAmount DECIMAL(19,4),
        VarianceAmount DECIMAL(19,4), VarianceStatus STRING NOT NULL, ExplanationCode STRING,
        EvaluatedAtUtc TIMESTAMP NOT NULL""",
    ("etl", "period_lock"): """
        PeriodLockId BIGINT GENERATED ALWAYS AS IDENTITY, LedgerCode STRING NOT NULL,
        AccountingPeriod STRING NOT NULL, LockStatusCode STRING NOT NULL, LockedByBatchId BIGINT,
        LockedByPackage STRING, LockedAtUtc TIMESTAMP, UnlockedByBatchId BIGINT,
        UnlockedAtUtc TIMESTAMP, Notes STRING""",
}

# Gold targets: created only when missing (FACT_/AGG_ sessions own the full
# schema). Finance-specific columns are added via MERGE schema evolution.
GOLD_MINIMUM_SCHEMAS = {
    ("gold", "fact_payment"): """
        PaymentBusinessKey STRING NOT NULL, PaymentSourceCode STRING, SupplierKey BIGINT,
        SupplierId STRING, InvoiceNumber STRING, InvoiceDate DATE, DueDate DATE, RegionCode STRING,
        LedgerCode STRING, AccountingPeriod STRING, ControlAccount STRING, AccountClass STRING,
        TransactionCurrencyCode STRING, EntityCurrencyCode STRING, TransactionAmount DECIMAL(19,4),
        FunctionalAmount DECIMAL(19,4), OpenAmount DECIMAL(19,4), PaymentAmount DECIMAL(19,4),
        WithholdingTaxAmount DECIMAL(19,4), AgingBucketCode STRING, AgingBucketSort INT,
        DaysPastDue INT, IsPastDue BOOLEAN, ReportableAmount DECIMAL(19,4),
        DiscountAtRisk DECIMAL(19,4), RevaluedFunctionalAmount DECIMAL(19,4),
        UnrealisedGainLossAmount DECIMAL(19,4), RevaluationRateDate DATE,
        RevaluationRateType STRING, FxRateToReporting DECIMAL(19,8), FxRateSourceCode STRING,
        AsOfDate DATE, BatchId BIGINT, LoadDatetime TIMESTAMP""",
    ("gold", "fact_gl_posting"): """
        GlJournalLineId BIGINT NOT NULL, JournalNumber STRING, JournalLineNumber INT,
        LedgerCode STRING, CostCentreCode STRING, AccountCode STRING, PostingDate DATE,
        AccountingPeriod STRING, CurrencyCode STRING, EnteredDebitAmount DECIMAL(19,4),
        EnteredCreditAmount DECIMAL(19,4), FunctionalDebitAmount DECIMAL(19,4),
        FunctionalCreditAmount DECIMAL(19,4), NetAmount DECIMAL(19,4), PostingSide STRING,
        SourceSubledgerCode STRING, SourceDocumentNumber STRING, SubledgerSourceKey STRING,
        TaxRegimeCode STRING, PeriodStatusCode STRING, BatchId BIGINT, LoadDatetime TIMESTAMP""",
    ("gold", "agg_finance_close_summary"): """
        CostCentreCode STRING NOT NULL, AccountingPeriod STRING NOT NULL, LedgerCode STRING,
        RegionCode STRING, AllocatedCostAmount DECIMAL(19,4), AllocationRuleCount INT,
        AllocationRuleSetCode STRING, UnallocatedResidualAmount DECIMAL(19,4),
        RefreshBatchId BIGINT, RefreshedDatetime TIMESTAMP""",
}


def ensureFinanceTables(spark, catalog: str) -> None:
    for (schema, table), columns in FINANCE_OWNED_TABLES.items():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {naming.table(catalog, schema, table)} ({columns}) USING DELTA"
        )


def ensureGoldTargets(spark, catalog: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
    for (schema, table), columns in GOLD_MINIMUM_SCHEMAS.items():
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {naming.table(catalog, schema, table)} ({columns}) USING DELTA"
        )
