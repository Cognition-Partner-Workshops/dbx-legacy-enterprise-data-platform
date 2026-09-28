"""Source adapters: legacy package column contract -> silver staging columns.

The FIN_* packages select columns (ApInvoiceKey, JournalLineNumber,
FunctionalDebitAmount, ...) that sqlserver/staging/tables/21_stg_tables_finance.sql
spells differently (StagingApInvoiceId, LineNumber, AccountedDebitAmount, ...).
Each adapter below is a Spark SQL SELECT that renames the silver column to the
name the package logic expects, so finance_rules.py can stay a literal port of
the package SQL/expressions. Columns marked UNMAPPED have no counterpart in the
staging DDL and are surfaced as NULL until the owning session confirms a source
(see docs/migration/10_finance-package-mapping.md, "needs decision").
"""
from __future__ import annotations

from dbx_etl_common import naming

UNMAPPED = "UNMAPPED"

AP_INVOICE_COLUMNS = {
    "ApInvoiceKey": "StagingApInvoiceId",
    "ApInvoiceBusinessKey": "ApInvoiceBusinessKey",
    "SupplierId": "SupplierBusinessKey",
    "SupplierSiteCode": UNMAPPED,
    "InvoiceNumber": "InvoiceNumber",
    "InvoiceDate": "InvoiceDate",
    "DueDate": "DueDate",
    "CurrencyCode": "TransactionCurrencyCode",
    "LedgerCode": "LedgerCode",
    "RegionCode": "RegionCode",
    "InvoiceAmount": "InvoiceAmount",
    "PaidAmount": "AmountPaid",
    "RecoverableVatAmount": "VatRecoverableAmount",
    "GstInputCreditAmount": "GstInputCreditAmount",
    "PaymentTermsCode": "PaymentTermsCode",
    "InvoiceStatusCode": "InvoiceStatusCode",
    "IsOnHold": "IsOnHold",
    "HoldReasonCode": "HoldReasonCode",
    "FiscalPeriodLabel": "FiscalPeriodLabel",
    "LoadBatchId": "BatchId",
}

GL_JOURNAL_LINE_COLUMNS = {
    "GlJournalLineId": "StagingGlJournalLineId",
    "JournalNumber": "JournalName",
    "JournalLineNumber": "LineNumber",
    "LedgerCode": "LedgerCode",
    "CostCentreCode": "CostCenterCode",
    "AccountCode": "GlAccountCode",
    "PostingDate": "EffectiveDate",
    "AccountingPeriod": "FiscalPeriodLabel",
    "CurrencyCode": "TransactionCurrencyCode",
    "EnteredDebitAmount": "EnteredDebitAmount",
    "EnteredCreditAmount": "EnteredCreditAmount",
    "FunctionalDebitAmount": "AccountedDebitAmount",
    "FunctionalCreditAmount": "AccountedCreditAmount",
    "SourceSubledgerCode": "JournalSourceCode",
    "SourceDocumentNumber": "GlJournalLineBusinessKey",
    "JournalStatusCode": "CASE WHEN IsPosted THEN 'POSTED' ELSE 'UNPOSTED' END",
    "LoadBatchId": "BatchId",
}

FX_RATE_COLUMNS = {
    "CurrencyCode": "FromCurrencyCode",
    "QuoteCurrencyCode": "ToCurrencyCode",
    "RateDate": "RateDate",
    "RateTypeCode": "UPPER(RateTypeCode)",
    "ConversionRate": "ConversionRate",
    "RateSourceCode": "RateSourceCode",
    "LoadBatchId": "BatchId",
}

PAYMENT_TERMS_COLUMNS = {
    "PaymentTermsCode": "PaymentTermsCode",
    "DiscountPercent": "DiscountPercent",
    "RegionCode": "RegionCode",
    "IsActive": "IsActive",
}

AP_INVOICE_LINE_COLUMNS = {
    "ApInvoiceLineId": "l.StagingApInvoiceLineId",
    "ApInvoiceBusinessKey": "l.ApInvoiceBusinessKey",
    "ApInvoiceKey": "i.StagingApInvoiceId",
    "SupplierId": "i.SupplierBusinessKey",
    "SupplierTaxRegistrationNumber": UNMAPPED,
    "JurisdictionCode": "l.TaxJurisdictionCode",
    "RegionCode": "i.RegionCode",
    "LedgerCode": "i.LedgerCode",
    "InvoiceDate": "i.InvoiceDate",
    "LineAmount": "l.LineAmount",
    "TaxCode": "l.TaxCode",
    "ServiceCategoryCode": UNMAPPED,
    "LineTypeCode": "l.LineTypeCode",
    "LoadBatchId": "l.BatchId",
}

COST_CENTRE_COLUMNS = {
    "CostCentreCode": "CostCenterCode",
    "LedgerCode": "LedgerCode",
    "RegionCode": "RegionCode",
    "IsActive": "IsActive",
}


def _select(columns: dict, alias: str | None = None) -> str:
    parts = []
    for target, source in columns.items():
        if source == UNMAPPED:
            parts.append(f"CAST(NULL AS STRING) AS {target}")
        else:
            expr = source if (alias is None or "." in source or "(" in source) else f"{alias}.{source}"
            parts.append(f"{expr} AS {target}")
    return ",\n       ".join(parts)


def apInvoiceSql(catalog: str) -> str:
    return f"SELECT {_select(AP_INVOICE_COLUMNS)}\nFROM {naming.table(catalog, 'silver', 'stg_ap_invoice')}"


def glJournalLineSql(catalog: str) -> str:
    return f"SELECT {_select(GL_JOURNAL_LINE_COLUMNS)}\nFROM {naming.table(catalog, 'silver', 'stg_gl_journal_line')}"


def fxRateSql(catalog: str) -> str:
    return f"SELECT {_select(FX_RATE_COLUMNS)}\nFROM {naming.table(catalog, 'silver', 'stg_fx_rate')}"


def paymentTermsSql(catalog: str) -> str:
    return f"SELECT {_select(PAYMENT_TERMS_COLUMNS)}\nFROM {naming.table(catalog, 'silver', 'stg_payment_terms')}"


def apInvoiceLineSql(catalog: str) -> str:
    return (
        f"SELECT {_select(AP_INVOICE_LINE_COLUMNS)}\n"
        f"FROM {naming.table(catalog, 'silver', 'stg_ap_invoice_line')} AS l\n"
        f"LEFT JOIN {naming.table(catalog, 'silver', 'stg_ap_invoice')} AS i\n"
        f"  ON i.ApInvoiceBusinessKey = l.ApInvoiceBusinessKey"
    )


def costCentreSql(catalog: str) -> str:
    return f"SELECT {_select(COST_CENTRE_COLUMNS)}\nFROM {naming.table(catalog, 'silver', 'stg_cost_center')}"


def unmappedColumns() -> dict[str, list[str]]:
    out = {}
    for name, cols in (
        ("stg.ApInvoice", AP_INVOICE_COLUMNS),
        ("stg.GlJournalLine", GL_JOURNAL_LINE_COLUMNS),
        ("stg.FxRate", FX_RATE_COLUMNS),
        ("stg.ApInvoiceLine", AP_INVOICE_LINE_COLUMNS),
    ):
        missing = [c for c, s in cols.items() if s == UNMAPPED]
        if missing:
            out[name] = missing
    return out
