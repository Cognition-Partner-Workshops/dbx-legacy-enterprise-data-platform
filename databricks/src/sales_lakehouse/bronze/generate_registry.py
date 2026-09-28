"""Regenerate ``bronze/registry.py`` from the checked-in legacy DDL.

    python -m sales_lakehouse.bronze.generate_registry [--repo-root PATH]

Only the *shape* (columns, types, primary key) is derived from the DDL. Load
mode, watermark column and lookback come from the SSIS extract package
descriptions (``ssis/01_oracle_extract`` / ``ssis/02_sqlserver_extract``) and
are declared in ``SOURCE_SPECS`` below so they stay reviewable.
"""
from __future__ import annotations

import argparse
import os

from sales_lakehouse.bronze.ddl import findRepoRoot, parseTableFromRepo

# (system, schema, table, loadMode, watermarkColumn, overlapMinutes, legacyPackage)
# LEGACY QUIRK: modes/watermarks mirror the DTS:Description text of the extract
# packages. Tables the legacy estate never extracted default to ``full``.
SOURCE_SPECS: list[tuple[str, str, str, str, str | None, int, str | None]] = [
    # --- SQL Server WideWorldImporters OLTP --------------------------------
    ("sqlserver", "Application", "People", "full", None, 0, "EXT_SQL_People"),
    ("sqlserver", "Application", "SalesTeams", "full", None, 0, None),
    ("sqlserver", "Application", "SalesTeamMembers", "full", None, 0, None),
    ("sqlserver", "Sales", "BuyingGroups", "full", None, 0, None),
    ("sqlserver", "Sales", "CustomerCategories", "full", None, 0, None),
    ("sqlserver", "Sales", "Customers", "full", None, 0, None),
    ("sqlserver", "Sales", "SalesChannels", "full", None, 0, None),
    ("sqlserver", "Sales", "SalesTerritories", "full", None, 0, "EXT_SQL_SalesTerritories"),
    ("sqlserver", "Sales", "CommissionPlans", "full", None, 0, "EXT_SQL_SalesTerritories"),
    ("sqlserver", "Sales", "SalesQuotas", "full", None, 0, "EXT_SQL_SalesTerritories"),
    ("sqlserver", "Sales", "PriceLists", "full", None, 0, None),
    ("sqlserver", "Sales", "PriceListLines", "full", None, 0, None),
    ("sqlserver", "Sales", "Promotions", "full", None, 0, "EXT_SQL_Promotions"),
    ("sqlserver", "Sales", "CustomerSegments", "full", None, 0, "EXT_SQL_CustomerSegments"),
    ("sqlserver", "Sales", "CustomerSegmentAssignments", "full", None, 0, "EXT_SQL_CustomerSegments"),
    ("sqlserver", "Sales", "QuoteHeaders", "full", None, 0, None),
    ("sqlserver", "Sales", "QuoteLines", "full", None, 0, None),
    ("sqlserver", "Sales", "Orders", "incremental", "OrderID", 0, "EXT_SQL_Orders"),
    ("sqlserver", "Sales", "OrderLines", "incremental", "OrderLineID", 0, "EXT_SQL_OrderLines"),
    ("sqlserver", "Sales", "OrderAmendments", "full", None, 0, None),
    ("sqlserver", "Sales", "OrderHolds", "full", None, 0, None),
    ("sqlserver", "Sales", "Backorders", "full", None, 0, None),
    ("sqlserver", "Sales", "Invoices", "incremental", "InvoiceID", 0, "EXT_SQL_Invoices"),
    ("sqlserver", "Sales", "InvoiceLines", "incremental", "InvoiceLineID", 0, "EXT_SQL_InvoiceLines"),
    ("sqlserver", "Sales", "CustomerTransactions", "incremental", "CustomerTransactionID", 0, "EXT_SQL_CustomerTransactions"),
    ("sqlserver", "Sales", "CustomerPayments", "full", None, 0, None),
    ("sqlserver", "Sales", "PaymentAllocations", "full", None, 0, None),
    ("sqlserver", "Sales", "CustomerDisputes", "full", None, 0, None),
    ("sqlserver", "Sales", "CustomerWriteOffs", "full", None, 0, None),
    ("sqlserver", "Sales", "CustomerCreditHolds", "full", None, 0, None),
    ("sqlserver", "Sales", "OrderDeletionLog", "full", None, 0, None),
    ("sqlserver", "Returns", "ReturnReasons", "full", None, 0, "EXT_SQL_Returns"),
    ("sqlserver", "Returns", "ReturnAuthorizations", "full", None, 0, "EXT_SQL_Returns"),
    ("sqlserver", "Returns", "ReturnLines", "incremental", "ReturnLineID", 0, "EXT_SQL_Returns"),
    ("sqlserver", "Returns", "ReturnInspections", "full", None, 0, "EXT_SQL_Returns"),
    ("sqlserver", "Returns", "CreditNotes", "full", None, 0, "EXT_SQL_CreditNotes"),
    ("sqlserver", "Returns", "CreditNoteLines", "incremental", "CreditNoteLineID", 0, "EXT_SQL_CreditNotes"),
    # LEGACY QUIRK: EXT_SQL_StockItems reads ValidFrom with a 240 minute lookback.
    ("sqlserver", "Warehouse", "StockItems", "incremental", "ValidFrom", 240, "EXT_SQL_StockItems"),
    ("sqlserver", "Warehouse", "StockItemHoldings", "full", None, 0, "EXT_SQL_StockItems"),
    ("sqlserver", "Shipping", "ShipmentHeaders", "incremental", "ShipmentID", 0, "EXT_SQL_Shipments"),
    ("sqlserver", "Shipping", "ShipmentLines", "incremental", "ShipmentLineID", 0, "EXT_SQL_ShipmentLines"),
    ("sqlserver", "Integration", "ChangeTrackingWatermark", "full", None, 0, None),
    # --- Oracle WWIGERP -----------------------------------------------------
    # LEGACY QUIRK: EXT_ORA_CustomerMaster uses a LAST_UPDATE_DT watermark with
    # lookback; the DDL column is UPDATED_DT ("Incremental extract source, keyed
    # on UPDATED_DT" in WWI_MDM.CUST_MASTER.sql).
    ("oracle", "WWI_MDM", "CUST_MASTER", "incremental", "UPDATED_DT", 120, "EXT_ORA_CustomerMaster"),
    ("oracle", "WWI_MDM", "PARTY_XREF", "full", None, 0, None),
    ("oracle", "WWI_MDM", "MDM_MERGE_HISTORY", "full", None, 0, None),
    ("oracle", "WWI_REF", "FX_RATE_DAILY", "incremental", "RATE_DT", 0, "EXT_ORA_FxRateDaily"),
    ("oracle", "WWI_REF", "CURRENCY_CODE", "full", None, 0, "EXT_ORA_Currency"),
    ("oracle", "WWI_REF", "CALENDAR_FISCAL", "full", None, 0, None),
    ("oracle", "WWI_REF", "CODE_TRANSLATION", "full", None, 0, "EXT_ORA_CodeTranslation"),
    ("oracle", "WWI_REF", "REGION_REF", "full", None, 0, "EXT_ORA_Geography"),
    ("oracle", "WWI_REF", "COUNTRY_REF", "full", None, 0, "EXT_ORA_Geography"),
    ("oracle", "WWI_REF", "SOURCE_SYSTEM_REF", "full", None, 0, None),
    ("oracle", "WWI_FIN", "GL_PERIOD_STATUS", "full", None, 0, None),
    ("oracle", "WWI_FIN", "TAX_JURISDICTION", "full", None, 0, "EXT_ORA_TaxRate"),
    ("oracle", "WWI_FIN", "TAX_RATE", "full", None, 0, "EXT_ORA_TaxRate"),
    ("oracle", "WWI_FIN", "WITHHOLDING_RULE", "full", None, 0, "EXT_ORA_TaxRate"),
    ("oracle", "WWI_FIN", "COST_CENTER", "full", None, 0, "EXT_ORA_CostCenter"),
    ("oracle", "WWI_FIN", "COST_ALLOCATION_RULE", "full", None, 0, "EXT_ORA_CostCenter"),
    ("oracle", "WWI_FIN", "PAYMENT_TERMS", "full", None, 0, "EXT_ORA_PaymentTerms"),
]

HEADER = '''"""Bronze source registry - GENERATED by ``sales_lakehouse.bronze.generate_registry``.

Do not hand-edit column lists; change the DDL or ``generate_registry.SOURCE_SPECS``
and regenerate. Every entry lands in ``sales_bronze.<system>_<schema>_<table>``
with the source columns verbatim plus the five ``_`` metadata columns
(see CONVENTIONS.md "Bronze tables").
"""
from __future__ import annotations

from sales_lakehouse.bronze.source_table import SourceTable

SOURCE_TABLES: tuple[SourceTable, ...] = (
'''

FOOTER = ''')

REGISTRY: dict[str, SourceTable] = {t.bronzeTable: t for t in SOURCE_TABLES}
BY_SOURCE_OBJECT: dict[str, SourceTable] = {t.sourceObject: t for t in SOURCE_TABLES}
'''


def renderEntry(spec, parsed) -> str:
    system, schema, table, mode, watermark, overlap, package = spec
    lines = [
        "    SourceTable(",
        f"        system={system!r},",
        f"        schema={schema!r},",
        f"        table={table!r},",
        f"        loadMode={mode!r},",
    ]
    if watermark:
        lines.append(f"        watermarkColumn={watermark!r},")
    if overlap:
        lines.append(f"        overlapMinutes={overlap},")
    if package:
        lines.append(f"        legacyPackage={package!r},")
    lines.append(f"        naturalKey={tuple(parsed.primaryKey)!r},")
    lines.append(f"        ddlFiles={tuple(parsed.ddlFiles)!r},")
    lines.append("        columns=(")
    for name, sparkType in parsed.columns:
        lines.append(f"            ({name!r}, {sparkType!r}),")
    lines.append("        ),")
    lines.append("    ),")
    return "\n".join(lines)


def render(repoRoot: str) -> str:
    chunks = [HEADER]
    for spec in SOURCE_SPECS:
        system, schema, table, mode, watermark, _, _ = spec
        parsed = parseTableFromRepo(repoRoot, system, schema, table)
        names = [c for c, _ in parsed.columns]
        if mode == "incremental" and watermark not in names:
            raise ValueError(f"{schema}.{table}: watermark column {watermark} not in DDL columns")
        if not parsed.primaryKey:
            raise ValueError(f"{schema}.{table}: no PRIMARY KEY found in DDL")
        chunks.append(renderEntry(spec, parsed))
    chunks.append(FOOTER)
    return "\n".join(chunks)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=None)
    parser.add_argument("--output", default=os.path.join(os.path.dirname(__file__), "registry.py"))
    args = parser.parse_args()
    repoRoot = args.repo_root or findRepoRoot()
    text = render(repoRoot)
    with open(args.output, "w", encoding="utf-8") as handle:
        handle.write(text)
    count = text.count("    SourceTable(")
    print(f"wrote {args.output} ({count} tables)")


if __name__ == "__main__":
    main()
