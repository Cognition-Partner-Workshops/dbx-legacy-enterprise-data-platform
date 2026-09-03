"""Emit the WWI_Finance domain packages (ssis/10_finance).

The finance mart is the oldest part of the warehouse and still carries the
rules the controllers wrote when the ERP was cut over: aging buckets are
30/60/90 with a separate "not yet due" bucket, the EU entities are VAT
exclusive, APAC reports on a 4-4-5 calendar, and nothing may post into a
locked period.

Run:  python3 ssis/10_finance/build_finance_packages.py
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import project  # noqa: E402
from patterns import (CONN_DW, CONN_STAGING, exec_proc, log_package_start,  # noqa: E402
                      log_package_success, log_row_count, new_package, truncate)
from ssisgen import (Column, DataFlow, DataFlowTask, ExecuteSql, Expression,  # noqa: E402
                     date_col, int_col, money_col, str_col)

PROJECT_NAME = "WWI_Finance"
CONNECTIONS = ["WWI_Staging_DB", "WWI_DW_Destination_DB", "WWI_Reject_Files"]


def bool_col(name):
    return Column(name, "bool")


def rate_col(name):
    return Column(name, "numeric", precision=18, scale=8)


# ---------------------------------------------------------------------------
# FIN_Load_ApAging
# ---------------------------------------------------------------------------

AP_AGING_SQL = """
SELECT  ai.ApInvoiceKey
,       ai.SupplierId
,       ai.SupplierSiteCode
,       ai.InvoiceNumber
,       ai.InvoiceDate
,       ai.DueDate
,       ai.CurrencyCode
,       ai.LedgerCode
,       ai.RegionCode
,       ai.InvoiceAmount
,       ai.PaidAmount
,       ai.InvoiceAmount - ai.PaidAmount        AS OpenAmount
,       DATEDIFF(DAY, ai.DueDate, ?)            AS DaysPastDue
        /* Buckets have not changed since the 2004 conversion. The business
           still calls the last one "90+" even though it is really 91 and over. */
,       CASE
            WHEN DATEDIFF(DAY, ai.DueDate, ?) <= 0  THEN 'CURRENT'
            WHEN DATEDIFF(DAY, ai.DueDate, ?) <= 30 THEN 'B030'
            WHEN DATEDIFF(DAY, ai.DueDate, ?) <= 60 THEN 'B060'
            WHEN DATEDIFF(DAY, ai.DueDate, ?) <= 90 THEN 'B090'
            ELSE 'B090P'
        END                                     AS AgingBucketCode
        /* NA books gross of sales tax, EU books net of recoverable VAT and
           APAC books net of GST input credit. Three different rules, three
           different columns, all of them load into the same fact. */
,       CASE ai.RegionCode
            WHEN 'NA'   THEN ai.InvoiceAmount
            WHEN 'EU'   THEN ai.InvoiceAmount - ISNULL(ai.RecoverableVatAmount, 0)
            WHEN 'APAC' THEN ai.InvoiceAmount - ISNULL(ai.GstInputCreditAmount, 0)
            ELSE ai.InvoiceAmount
        END                                     AS ReportableAmount
,       ISNULL(pt.DiscountPercent, 0)           AS EarlyPaymentDiscountPercent
FROM    stg.ApInvoice AS ai
LEFT OUTER JOIN stg.PaymentTerms AS pt
        ON  pt.PaymentTermsCode = ai.PaymentTermsCode
WHERE   ai.InvoiceAmount - ai.PaidAmount <> 0
AND     ai.InvoiceStatusCode NOT IN ('CANC', 'VOID', 'DRAFT')
AND     ai.LoadBatchId = ?
""".strip()


def build_fin_load_apaging():
    pkg = new_package(
        "FIN_Load_ApAging",
        "Month-end AP aging refresh. Recomputes the open-item aging buckets for every unpaid "
        "supplier invoice and posts the aging rows into the payment fact. Regional amount rules "
        "diverge: NA gross of sales tax, EU net of recoverable VAT, APAC net of GST input credit.",
        source_system="ORAERP",
        connections=(CONN_DW,),
        extra_variables=[("AsOfDate", "1900-01-01", "string"), ("OpenItemCount", 0, "int")],
    )
    pkg.add_parameter("AgingAsOfDate", "1900-01-01", dtype="string",
                      description="Aging as-of date; the close calendar supplies the period end date.")
    pkg.add_parameter("IncludeDisputedInvoices", "False", dtype="bool",
                      description="Disputed invoices are excluded from aging unless the controller asks for them.")

    start = pkg.add(log_package_start(pkg))
    as_of = pkg.add(Expression("Resolve As Of Date",
                               "@[User::AsOfDate] = @[$Package::AgingAsOfDate]"))
    stage_clear = pkg.add(truncate("work.ApAgingStaging"))

    columns = [
        int_col("ApInvoiceKey"), str_col("SupplierId", 20), str_col("SupplierSiteCode", 10),
        str_col("InvoiceNumber", 30), date_col("InvoiceDate"), date_col("DueDate"),
        str_col("CurrencyCode", 3), str_col("LedgerCode", 10), str_col("RegionCode", 4),
        money_col("InvoiceAmount"), money_col("PaidAmount"), money_col("OpenAmount"),
        int_col("DaysPastDue"), str_col("AgingBucketCode", 6), money_col("ReportableAmount"),
        money_col("EarlyPaymentDiscountPercent"),
    ]
    flow = DataFlow("Load AP Aging Buckets")
    flow.oledb_source("stg ApInvoice Open Items", CONN_STAGING, AP_AGING_SQL, columns, timeout=1800)
    flow.derived_column("Derive Aging Attributes", [
        ("IsPastDue", "DaysPastDue > 0 ? (DT_BOOL)1 : (DT_BOOL)0", bool_col("IsPastDue")),
        ("AgingBucketSort",
         '(AgingBucketCode == "CURRENT") ? 0 : ((AgingBucketCode == "B030") ? 1 : '
         '((AgingBucketCode == "B060") ? 2 : ((AgingBucketCode == "B090") ? 3 : 4)))',
         int_col("AgingBucketSort")),
        ("DiscountAtRisk",
         'DaysPastDue > 0 ? (DT_NUMERIC,18,2)0 : OpenAmount * EarlyPaymentDiscountPercent / 100',
         money_col("DiscountAtRisk")),
    ])
    flow.lookup("Lookup Supplier Key", CONN_DW,
                "SELECT [Supplier Key] AS SupplierKey, [WWI Supplier ID] AS SupplierId "
                "FROM Dimension.Supplier WHERE [Valid To] > SYSDATETIME();",
                ["SupplierId"], [int_col("SupplierKey")], no_match="RD")
    flow.row_count("Count Aging Rows", "User::RowsRead")
    flow.oledb_destination("work ApAgingStaging", CONN_STAGING, "[work].[ApAgingStaging]",
                           batch_size=50000)
    flow.reject_destination("Reject Unmatched Suppliers", CONN_STAGING, "[err].[ApAgingReject]",
                            "Lookup Supplier Key", "Lookup No Match Output")
    load = pkg.add(DataFlowTask(flow))

    post = pkg.add(exec_proc(
        "Post Aging To Fact",
        "EXEC Integration.usp_PostApAging @BatchId = ?, @AsOfDate = ?, "
        "@IncludeDisputed = ?, @RowsInserted = ? OUTPUT;",
        connection=CONN_DW,
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("User::AsOfDate", 1, "NVARCHAR"),
            ("$Package::IncludeDisputedInvoices", 2, "BYTE"),
        ],
    ))
    # Row-by-row bucket summary. It is slow, it has been slow since 2007, and
    # the close pack depends on the ordering it produces.
    summary = pkg.add(ExecuteSql(
        "Refresh Aging Summary",
        CONN_DW,
        "DECLARE @LedgerCode nvarchar(10), @BucketCode nvarchar(6);\n"
        "DECLARE aging_cur CURSOR LOCAL FAST_FORWARD FOR\n"
        "    SELECT DISTINCT LedgerCode, AgingBucketCode FROM work.ApAgingStaging ORDER BY LedgerCode, AgingBucketCode;\n"
        "OPEN aging_cur;\n"
        "FETCH NEXT FROM aging_cur INTO @LedgerCode, @BucketCode;\n"
        "WHILE @@FETCH_STATUS = 0\n"
        "BEGIN\n"
        "    EXEC Integration.usp_RefreshApAgingSummary @LedgerCode = @LedgerCode, @BucketCode = @BucketCode;\n"
        "    FETCH NEXT FROM aging_cur INTO @LedgerCode, @BucketCode;\n"
        "END\n"
        "CLOSE aging_cur;\n"
        "DEALLOCATE aging_cur;",
    ))
    counts = pkg.add(log_row_count("Fact.Payment"))
    done = pkg.add(log_package_success())

    pkg.link(start, as_of)
    pkg.link(as_of, stage_clear)
    pkg.link(stage_clear, load)
    pkg.link(load, post)
    pkg.link(post, summary)
    pkg.link(summary, counts, value="Completion")
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# FIN_Load_GlPostings
# ---------------------------------------------------------------------------

GL_SQL = """
SELECT  gl.GlJournalLineId
,       gl.JournalNumber
,       gl.JournalLineNumber
,       gl.LedgerCode
,       gl.CostCentreCode
,       gl.AccountCode
,       gl.PostingDate
,       gl.AccountingPeriod
,       gl.CurrencyCode
,       gl.EnteredDebitAmount
,       gl.EnteredCreditAmount
,       gl.SourceSubledgerCode
,       gl.SourceDocumentNumber
        /* The ERP stores the ledger's functional currency amount only when the
           journal is not in the ledger currency, so the ETL has always had to
           fall back to the entered amount. */
,       ISNULL(gl.FunctionalDebitAmount, gl.EnteredDebitAmount)   AS FunctionalDebitAmount
,       ISNULL(gl.FunctionalCreditAmount, gl.EnteredCreditAmount) AS FunctionalCreditAmount
,       CASE
            WHEN gl.LedgerCode LIKE 'EU%'   THEN 'VAT'
            WHEN gl.LedgerCode LIKE 'APAC%' THEN 'GST'
            ELSE 'SALESTAX'
        END                                                       AS TaxRegimeCode
FROM    stg.GlJournalLine AS gl
INNER JOIN etl.Configuration AS cfg
        ON  cfg.ConfigurationKey = 'Finance.OpenPeriod.' + gl.LedgerCode
        AND cfg.ConfigurationValue = gl.AccountingPeriod
WHERE   gl.JournalStatusCode = 'POSTED'
AND     gl.LoadBatchId = ?
""".strip()


def build_fin_load_glpostings():
    pkg = new_package(
        "FIN_Load_GlPostings",
        "Loads posted GL journal lines into the GL posting fact. Lines whose accounting period is "
        "not the open period for their ledger are held back rather than rejected, because the ERP "
        "back-posts adjustments for up to two periods after close.",
        source_system="ORAERP",
        connections=(CONN_DW,),
        extra_variables=[("HeldLineCount", 0, "int"), ("UnbalancedJournalCount", 0, "int")],
    )
    pkg.add_parameter("AccountingPeriod", "1900-01", dtype="string",
                      description="Accounting period being posted, YYYY-MM.")
    pkg.add_parameter("AllowUnbalancedJournals", "False", dtype="bool",
                      description="Legacy switch used when the ERP ships a known one-sided correction.")

    start = pkg.add(log_package_start(pkg))
    balance_check = pkg.add(ExecuteSql(
        "Check Journal Balance",
        CONN_STAGING,
        "SELECT COUNT(*) AS UnbalancedJournals FROM (\n"
        "    SELECT JournalNumber\n"
        "    FROM   stg.GlJournalLine\n"
        "    WHERE  LoadBatchId = ?\n"
        "    GROUP BY JournalNumber\n"
        "    HAVING ABS(SUM(EnteredDebitAmount) - SUM(EnteredCreditAmount)) > 0.005\n"
        ") AS unbalanced;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::UnbalancedJournalCount")],
    ))

    columns = [
        int_col("GlJournalLineId"), str_col("JournalNumber", 30), int_col("JournalLineNumber"),
        str_col("LedgerCode", 10), str_col("CostCentreCode", 10), str_col("AccountCode", 20),
        date_col("PostingDate"), str_col("AccountingPeriod", 7), str_col("CurrencyCode", 3),
        money_col("EnteredDebitAmount"), money_col("EnteredCreditAmount"),
        str_col("SourceSubledgerCode", 6), str_col("SourceDocumentNumber", 30),
        money_col("FunctionalDebitAmount"), money_col("FunctionalCreditAmount"),
        str_col("TaxRegimeCode", 8),
    ]
    flow = DataFlow("Load GL Postings")
    flow.oledb_source("stg GlJournalLine Posted", CONN_STAGING, GL_SQL, columns, timeout=3600)
    flow.derived_column("Derive Posting Attributes", [
        ("NetAmount", "FunctionalDebitAmount - FunctionalCreditAmount", money_col("NetAmount")),
        ("PostingSide", 'FunctionalDebitAmount > 0 ? "DR" : "CR"', str_col("PostingSide", 2)),
        ("SubledgerSourceKey",
         'SourceSubledgerCode + "|" + SourceDocumentNumber', str_col("SubledgerSourceKey", 40)),
    ])
    flow.conditional_split("Split Held Lines", [
        ("Postable", 'AccountingPeriod == @[$Package::AccountingPeriod]'),
        ("Held", 'AccountingPeriod != @[$Package::AccountingPeriod]'),
    ])
    flow.row_count("Count Postable Lines", "User::RowsInserted")
    flow.oledb_destination("Fact GL Posting", CONN_DW, "[Fact].[GL Posting]", batch_size=100000)
    flow.branch_destination("work GlHeldLine", CONN_STAGING, "[work].[GlHeldLine]",
                            "Split Held Lines", "Held")
    load = pkg.add(DataFlowTask(flow))

    held = pkg.add(ExecuteSql(
        "Log Held Lines",
        CONN_STAGING,
        "EXEC etl.usp_LogRejectedRecord @PackageExecutionId = ?, @BatchId = ?, "
        "@ObjectName = N'stg.GlJournalLine', @RejectStage = N'Fact', "
        "@RejectReasonCode = N'PERIOD_NOT_OPEN', "
        "@RejectReason = N'Journal line belongs to a period other than the one being posted.', "
        "@BusinessKey = N'batch', @RecordPayload = NULL;",
        parameter_bindings=[("User::PackageExecutionId", 0, "LONG"), ("$Package::BatchId", 1, "LONG")],
        is_stored_procedure=True,
    ))
    counts = pkg.add(log_row_count("Fact.GL Posting"))
    done = pkg.add(log_package_success())

    pkg.link(start, balance_check)
    pkg.link(balance_check, load,
             expression="@[User::UnbalancedJournalCount] == 0 || @[$Package::AllowUnbalancedJournals]")
    pkg.link(load, held, value="Completion")
    pkg.link(held, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# FIN_Reconcile_SubledgerToGl
# ---------------------------------------------------------------------------


def build_fin_reconcile_subledger_to_gl():
    pkg = new_package(
        "FIN_Reconcile_SubledgerToGl",
        "Ties the AP and AR subledgers back to the GL control accounts by ledger and period and "
        "writes the variance rows the controllers sign off. A variance inside the ledger tolerance "
        "is recorded as explained; anything larger is raised to operations.",
        source_system="ORAERP",
        connections=(CONN_DW,),
        extra_variables=[("VarianceRowCount", 0, "int"), ("LargestVariance", 0, "decimal")],
    )
    pkg.add_parameter("AccountingPeriod", "1900-01", dtype="string",
                      description="Period being reconciled, YYYY-MM.")
    pkg.add_parameter("VarianceTolerance", 1, dtype="int",
                      description="Absolute functional-currency tolerance per control account.")

    start = pkg.add(log_package_start(pkg))
    build = pkg.add(ExecuteSql(
        "Build Reconciliation Set",
        CONN_DW,
        "WITH subledger AS (\n"
        "    SELECT  p.[Ledger Code]      AS LedgerCode\n"
        "    ,       p.[Accounting Period] AS AccountingPeriod\n"
        "    ,       p.[Control Account]  AS AccountCode\n"
        "    ,       SUM(p.[Functional Amount]) AS SubledgerAmount\n"
        "    FROM    Fact.Payment AS p\n"
        "    WHERE   p.[Accounting Period] = ?\n"
        "    GROUP BY p.[Ledger Code], p.[Accounting Period], p.[Control Account]\n"
        "), ledger AS (\n"
        "    SELECT  g.[Ledger Code]      AS LedgerCode\n"
        "    ,       g.[Accounting Period] AS AccountingPeriod\n"
        "    ,       g.[Account Code]     AS AccountCode\n"
        "    ,       SUM(g.[Functional Debit Amount] - g.[Functional Credit Amount]) AS LedgerAmount\n"
        "    FROM    Fact.[GL Posting] AS g\n"
        "    WHERE   g.[Accounting Period] = ?\n"
        "    GROUP BY g.[Ledger Code], g.[Accounting Period], g.[Account Code]\n"
        ")\n"
        "INSERT INTO etl.ReconciliationResult\n"
        "    (BatchId, ReconciliationName, LedgerCode, AccountingPeriod, AccountCode,\n"
        "     SourceAmount, TargetAmount, VarianceAmount, VarianceStatus, EvaluatedAtUtc)\n"
        "SELECT  ?\n"
        ",       N'Subledger to GL'\n"
        ",       COALESCE(s.LedgerCode, l.LedgerCode)\n"
        ",       COALESCE(s.AccountingPeriod, l.AccountingPeriod)\n"
        ",       COALESCE(s.AccountCode, l.AccountCode)\n"
        ",       ISNULL(s.SubledgerAmount, 0)\n"
        ",       ISNULL(l.LedgerAmount, 0)\n"
        ",       ISNULL(s.SubledgerAmount, 0) - ISNULL(l.LedgerAmount, 0)\n"
        ",       CASE WHEN ABS(ISNULL(s.SubledgerAmount, 0) - ISNULL(l.LedgerAmount, 0)) <= ?\n"
        "             THEN N'Within tolerance' ELSE N'Variance' END\n"
        ",       SYSUTCDATETIME()\n"
        "FROM    subledger AS s\n"
        "FULL OUTER JOIN ledger AS l\n"
        "        ON  l.LedgerCode = s.LedgerCode\n"
        "        AND l.AccountCode = s.AccountCode\n"
        "        AND l.AccountingPeriod = s.AccountingPeriod;",
        parameter_bindings=[
            ("$Package::AccountingPeriod", 0, "NVARCHAR"),
            ("$Package::AccountingPeriod", 1, "NVARCHAR"),
            ("$Package::BatchId", 2, "LONG"),
            ("$Package::VarianceTolerance", 3, "LONG"),
        ],
    ))
    measure = pkg.add(ExecuteSql(
        "Measure Variances",
        CONN_DW,
        "SELECT COUNT(*) AS VarianceRows, ISNULL(MAX(ABS(VarianceAmount)), 0) AS LargestVariance "
        "FROM etl.ReconciliationResult "
        "WHERE BatchId = ? AND VarianceStatus = N'Variance';",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::VarianceRowCount"), ("1", "User::LargestVariance")],
    ))
    explain = pkg.add(ExecuteSql(
        "Apply Known Explanations",
        CONN_DW,
        "UPDATE r SET VarianceStatus = N'Explained', ExplanationCode = c.ConfigurationValue "
        "FROM etl.ReconciliationResult AS r "
        "INNER JOIN etl.Configuration AS c "
        "        ON c.ConfigurationKey = N'Finance.KnownVariance.' + r.AccountCode "
        "WHERE r.BatchId = ? AND r.VarianceStatus = N'Variance';",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    raise_reject = pkg.add(ExecuteSql(
        "Raise Unexplained Variances",
        CONN_STAGING,
        "INSERT INTO etl.RejectedRecord "
        "    (BatchId, ObjectName, RejectStage, RejectReasonCode, RejectReason, "
        "     BusinessKey, LoggedAtUtc, IsReprocessed) "
        "SELECT ?, N'etl.ReconciliationResult', N'Fact', N'SUBLEDGER_VARIANCE', "
        "       N'Subledger to GL variance outside tolerance', "
        "       LedgerCode + N'|' + AccountCode, SYSUTCDATETIME(), 0 "
        "FROM etl.ReconciliationResult "
        "WHERE BatchId = ? AND VarianceStatus = N'Variance';",
        parameter_bindings=[("$Package::BatchId", 0, "LONG"), ("$Package::BatchId", 1, "LONG")],
    ))
    counts = pkg.add(log_row_count("etl.ReconciliationResult"))
    done = pkg.add(log_package_success())

    pkg.link(start, build)
    pkg.link(build, measure)
    pkg.link(measure, explain)
    pkg.link(explain, raise_reject, expression="@[User::VarianceRowCount] > 0")
    pkg.link(explain, counts, expression="@[User::VarianceRowCount] == 0")
    pkg.link(raise_reject, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# FIN_Close_PeriodLock
# ---------------------------------------------------------------------------


def build_fin_close_periodlock():
    pkg = new_package(
        "FIN_Close_PeriodLock",
        "Closes the accounting period once the close sequence has completed: asserts there is no "
        "open reconciliation variance, flips the period status configuration rows per ledger, and "
        "stamps the closing batch. NA and EU close on the calendar month; APAC closes on the last "
        "day of its 4-4-5 period, so the three ledgers are locked separately.",
        source_system="ORAERP",
        connections=(CONN_DW,),
        extra_variables=[("OpenVarianceCount", 0, "int"), ("LockedLedgerCount", 0, "int")],
    )
    pkg.add_parameter("AccountingPeriod", "1900-01", dtype="string", description="Period to lock.")
    pkg.add_parameter("LedgerScope", "ALL", dtype="string",
                      description="ALL, or a single ledger code when one region closes late.")

    start = pkg.add(log_package_start(pkg))
    guard = pkg.add(ExecuteSql(
        "Count Open Variances",
        CONN_DW,
        "SELECT COUNT(*) AS OpenVariances FROM etl.ReconciliationResult "
        "WHERE AccountingPeriod = ? AND VarianceStatus = N'Variance';",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::AccountingPeriod", 0, "NVARCHAR")],
        result_bindings=[("0", "User::OpenVarianceCount")],
    ))
    # Dynamic SQL, because the ledger list is data and the 2011 rewrite that was
    # going to remove this never happened.
    lock = pkg.add(ExecuteSql(
        "Lock Ledgers",
        CONN_DW,
        "DECLARE @sql nvarchar(max) = N'';\n"
        "DECLARE @Period nvarchar(7) = ?;\n"
        "DECLARE @Scope nvarchar(10) = ?;\n"
        "SELECT @sql = @sql + N'UPDATE etl.Configuration SET ConfigurationValue = N''Closed'' "
        "WHERE ConfigurationKey = N''Finance.PeriodStatus.' + LedgerCode + N''';' + CHAR(13)\n"
        "FROM   (SELECT DISTINCT LedgerCode FROM Fact.[GL Posting] WHERE [Accounting Period] = @Period) AS l\n"
        "WHERE  @Scope = N'ALL' OR l.LedgerCode = @Scope;\n"
        "EXEC sp_executesql @sql;",
        parameter_bindings=[
            ("$Package::AccountingPeriod", 0, "NVARCHAR"),
            ("$Package::LedgerScope", 1, "NVARCHAR"),
        ],
    ))
    apac = pkg.add(ExecuteSql(
        "Lock APAC 445 Period",
        CONN_DW,
        "UPDATE etl.Configuration "
        "SET    ConfigurationValue = N'Closed' "
        "WHERE  ConfigurationKey = N'Finance.PeriodStatus.APAC' "
        "AND    EXISTS (SELECT 1 FROM Dimension.Date "
        "               WHERE [Fiscal Period 445] = ? AND [Is Fiscal Period End] = 1 "
        "                 AND [Date] <= CAST(SYSDATETIME() AS date));",
        parameter_bindings=[("$Package::AccountingPeriod", 0, "NVARCHAR")],
    ))
    stamp = pkg.add(ExecuteSql(
        "Stamp Closing Batch",
        CONN_STAGING,
        "UPDATE etl.Batch SET Notes = CONCAT(ISNULL(Notes, N''), N' | period ', ?, N' locked') "
        "WHERE BatchId = ?;",
        parameter_bindings=[
            ("$Package::AccountingPeriod", 0, "NVARCHAR"),
            ("$Package::BatchId", 1, "LONG"),
        ],
    ))
    refuse = pkg.add(ExecuteSql(
        "Refuse Close",
        CONN_STAGING,
        "EXEC etl.usp_LogError @BatchId = ?, @ErrorSeverity = N'Error', @ErrorCode = 51001, "
        "@ErrorDescription = N'Period lock refused: unexplained subledger variances remain.', "
        "@SourceName = N'FIN_Close_PeriodLock';",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        is_stored_procedure=True,
    ))
    counts = pkg.add(log_row_count("etl.Batch"))
    done = pkg.add(log_package_success())

    pkg.link(start, guard)
    pkg.link(guard, lock, expression="@[User::OpenVarianceCount] == 0")
    pkg.link(guard, refuse, expression="@[User::OpenVarianceCount] > 0")
    pkg.link(lock, apac)
    pkg.link(apac, stamp)
    pkg.link(stamp, counts)
    pkg.link(refuse, counts, value="Completion")
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# FIN_Load_CostAllocation
# ---------------------------------------------------------------------------


def build_fin_load_costallocation():
    pkg = new_package(
        "FIN_Load_CostAllocation",
        "Applies the cost-centre allocation rules: shared-service costs are spread across receiving "
        "cost centres on the driver stored with the rule (headcount, floor area or revenue share), "
        "and the allocated result is folded into the finance close summary aggregate. Allocation "
        "runs in rule-sequence order because later rules allocate the output of earlier ones.",
        source_system="ORAERP",
        connections=(CONN_DW,),
        extra_variables=[("RuleCount", 0, "int"), ("UnallocatedAmount", 0, "decimal")],
    )
    pkg.add_parameter("AccountingPeriod", "1900-01", dtype="string", description="Period being allocated.")
    pkg.add_parameter("AllocationRuleSet", "STANDARD", dtype="string",
                      description="STANDARD or STATUTORY; the statutory set is used for EU entity reporting.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.CostAllocationResult"))
    rules = pkg.add(ExecuteSql(
        "Count Allocation Rules",
        CONN_STAGING,
        "SELECT COUNT(*) AS RuleCount FROM stg.CostCentre AS cc "
        "INNER JOIN stg.CostAllocationRule AS r ON r.SourceCostCentreCode = cc.CostCentreCode "
        "WHERE r.RuleSetCode = ? AND r.IsActive = 1;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::AllocationRuleSet", 0, "NVARCHAR")],
        result_bindings=[("0", "User::RuleCount")],
    ))
    # Cursor over the rule sequence: each pass allocates one rule and can pick up
    # amounts written by the previous pass, so the loop cannot be set-based.
    allocate = pkg.add(ExecuteSql(
        "Apply Allocation Rules",
        CONN_STAGING,
        "DECLARE @RuleId int, @Driver nvarchar(20), @Source nvarchar(10), @Basis nvarchar(20);\n"
        "DECLARE rule_cur CURSOR LOCAL FAST_FORWARD FOR\n"
        "    SELECT AllocationRuleId, DriverCode, SourceCostCentreCode, AllocationBasisCode\n"
        "    FROM   stg.CostAllocationRule\n"
        "    WHERE  RuleSetCode = ? AND IsActive = 1\n"
        "    ORDER BY RuleSequence;\n"
        "OPEN rule_cur;\n"
        "FETCH NEXT FROM rule_cur INTO @RuleId, @Driver, @Source, @Basis;\n"
        "WHILE @@FETCH_STATUS = 0\n"
        "BEGIN\n"
        "    INSERT INTO work.CostAllocationResult\n"
        "        (AllocationRuleId, SourceCostCentreCode, TargetCostCentreCode, DriverCode,\n"
        "         AllocatedAmount, AccountingPeriod)\n"
        "    SELECT  @RuleId, @Source, t.TargetCostCentreCode, @Driver,\n"
        "            src.PoolAmount * (t.DriverValue / NULLIF(SUM(t.DriverValue) OVER (), 0)),\n"
        "            ?\n"
        "    FROM    stg.CostAllocationTarget AS t\n"
        "    CROSS APPLY (SELECT SUM(Amount) AS PoolAmount\n"
        "                 FROM   stg.CostCentreBalance\n"
        "                 WHERE  CostCentreCode = @Source AND AccountingPeriod = ?) AS src\n"
        "    WHERE   t.AllocationRuleId = @RuleId;\n"
        "    FETCH NEXT FROM rule_cur INTO @RuleId, @Driver, @Source, @Basis;\n"
        "END\n"
        "CLOSE rule_cur;\n"
        "DEALLOCATE rule_cur;",
        parameter_bindings=[
            ("$Package::AllocationRuleSet", 0, "NVARCHAR"),
            ("$Package::AccountingPeriod", 1, "NVARCHAR"),
            ("$Package::AccountingPeriod", 2, "NVARCHAR"),
        ],
    ))
    residual = pkg.add(ExecuteSql(
        "Measure Unallocated Residual",
        CONN_STAGING,
        "SELECT ISNULL(SUM(b.Amount), 0) - ISNULL((SELECT SUM(AllocatedAmount) "
        "FROM work.CostAllocationResult WHERE AccountingPeriod = ?), 0) AS Residual "
        "FROM stg.CostCentreBalance AS b "
        "INNER JOIN stg.CostAllocationRule AS r ON r.SourceCostCentreCode = b.CostCentreCode "
        "WHERE b.AccountingPeriod = ? AND r.RuleSetCode = ? AND r.IsActive = 1;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[
            ("$Package::AccountingPeriod", 0, "NVARCHAR"),
            ("$Package::AccountingPeriod", 1, "NVARCHAR"),
            ("$Package::AllocationRuleSet", 2, "NVARCHAR"),
        ],
        result_bindings=[("0", "User::UnallocatedAmount")],
    ))

    columns = [
        int_col("AllocationRuleId"), str_col("SourceCostCentreCode", 10),
        str_col("TargetCostCentreCode", 10), str_col("DriverCode", 20),
        money_col("AllocatedAmount"), str_col("AccountingPeriod", 7),
    ]
    flow = DataFlow("Publish Allocation Summary")
    flow.oledb_source(
        "work CostAllocationResult", CONN_STAGING,
        "SELECT AllocationRuleId, SourceCostCentreCode, TargetCostCentreCode, DriverCode, "
        "AllocatedAmount, AccountingPeriod FROM work.CostAllocationResult "
        "WHERE AccountingPeriod = ?;",
        columns, timeout=900)
    flow.aggregate("Summarise By Target", ["TargetCostCentreCode", "AccountingPeriod"],
                   [("AllocatedAmount", "AllocatedAmount", "SUM"),
                    ("AllocationRuleId", "RuleCount", "COUNTDISTINCT")])
    flow.row_count("Count Summary Rows", "User::RowsInserted")
    flow.oledb_destination("Aggregate Finance Close Summary", CONN_DW,
                           "[Aggregate].[Finance Close Summary]", batch_size=20000)
    publish = pkg.add(DataFlowTask(flow))

    counts = pkg.add(log_row_count("Aggregate.Finance Close Summary"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, rules)
    pkg.link(rules, allocate, expression="@[User::RuleCount] > 0")
    pkg.link(rules, counts, expression="@[User::RuleCount] == 0")
    pkg.link(allocate, residual)
    pkg.link(residual, publish)
    pkg.link(publish, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# FIN_Currency_Revaluation
# ---------------------------------------------------------------------------

FX_SQL = """
SELECT  fx.CurrencyCode
,       fx.QuoteCurrencyCode
,       fx.RateDate
,       fx.RateTypeCode
,       fx.ConversionRate
,       fx.RateSourceCode
FROM    stg.FxRate AS fx
WHERE   fx.RateDate = (SELECT MAX(RateDate) FROM stg.FxRate WHERE RateDate <= ?)
AND     fx.RateTypeCode IN ('CLOSING', 'AVERAGE')
""".strip()


def build_fin_currency_revaluation():
    pkg = new_package(
        "FIN_Currency_Revaluation",
        "Month-end revaluation of open foreign-currency items. Balance-sheet items are revalued at "
        "the closing rate and P&L items at the period average, and the unrealised gain or loss is "
        "written back to the payment fact. EU entities revalue against EUR, APAC entities against "
        "the entity currency and NA against USD, which is why the rate lookup is done per ledger.",
        source_system="ORAERP",
        connections=(CONN_DW,),
        extra_variables=[("MissingRateCount", 0, "int"), ("RevaluedItemCount", 0, "int")],
    )
    pkg.add_parameter("RevaluationDate", "1900-01-01", dtype="string",
                      description="Period-end date the closing rate is taken from.")
    pkg.add_parameter("FailOnMissingRate", "True", dtype="bool",
                      description="A missing closing rate normally stops the close.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.FxRevaluationRate"))

    columns = [
        str_col("CurrencyCode", 3), str_col("QuoteCurrencyCode", 3), date_col("RateDate"),
        str_col("RateTypeCode", 10), rate_col("ConversionRate"), str_col("RateSourceCode", 10),
    ]
    flow = DataFlow("Load Closing Rates")
    flow.oledb_source("stg FxRate Closing", CONN_STAGING, FX_SQL, columns, timeout=600)
    flow.derived_column("Derive Inverse Rate", [
        ("InverseRate", "ConversionRate == 0 ? (DT_NUMERIC,18,8)0 : 1 / ConversionRate",
         rate_col("InverseRate")),
        ("IsTriangulated", 'QuoteCurrencyCode != "USD" ? (DT_BOOL)1 : (DT_BOOL)0',
         bool_col("IsTriangulated")),
    ])
    flow.row_count("Count Rates", "User::RowsRead")
    flow.oledb_destination("work FxRevaluationRate", CONN_STAGING, "[work].[FxRevaluationRate]",
                           batch_size=5000)
    rates = pkg.add(DataFlowTask(flow))

    missing = pkg.add(ExecuteSql(
        "Find Missing Rates",
        CONN_STAGING,
        "SELECT COUNT(*) AS MissingRates FROM ("
        "    SELECT DISTINCT c.CurrencyCode FROM stg.ApInvoice AS c "
        "    WHERE c.InvoiceAmount - c.PaidAmount <> 0"
        ") AS open_items "
        "WHERE NOT EXISTS (SELECT 1 FROM work.FxRevaluationRate AS r "
        "                  WHERE r.CurrencyCode = open_items.CurrencyCode "
        "                    AND r.RateTypeCode = N'CLOSING');",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::MissingRateCount")],
    ))
    revalue = pkg.add(ExecuteSql(
        "Revalue Open Items",
        CONN_DW,
        "UPDATE p\n"
        "SET    p.[Revalued Functional Amount] = p.[Transaction Amount] * r.ConversionRate\n"
        ",      p.[Unrealised Gain Loss Amount] =\n"
        "           (p.[Transaction Amount] * r.ConversionRate) - p.[Functional Amount]\n"
        ",      p.[Revaluation Rate Date] = r.RateDate\n"
        ",      p.[Revaluation Rate Type] = CASE WHEN p.[Account Class] = N'PL' THEN N'AVERAGE' ELSE N'CLOSING' END\n"
        "FROM   Fact.Payment AS p\n"
        "INNER JOIN work.FxRevaluationRate AS r\n"
        "        ON  r.CurrencyCode = p.[Transaction Currency Code]\n"
        "        AND r.RateTypeCode = CASE WHEN p.[Account Class] = N'PL' THEN N'AVERAGE' ELSE N'CLOSING' END\n"
        "        AND r.QuoteCurrencyCode = CASE\n"
        "                WHEN p.[Ledger Code] LIKE N'EU%'   THEN N'EUR'\n"
        "                WHEN p.[Ledger Code] LIKE N'APAC%' THEN p.[Entity Currency Code]\n"
        "                ELSE N'USD' END\n"
        "WHERE  p.[Open Amount] <> 0;",
    ))
    count_items = pkg.add(ExecuteSql(
        "Count Revalued Items",
        CONN_DW,
        "SELECT COUNT(*) AS RevaluedItems FROM Fact.Payment "
        "WHERE [Revaluation Rate Date] = CAST(? AS date);",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::RevaluationDate", 0, "NVARCHAR")],
        result_bindings=[("0", "User::RevaluedItemCount")],
    ))
    halt = pkg.add(ExecuteSql(
        "Log Missing Rates",
        CONN_STAGING,
        "EXEC etl.usp_LogError @BatchId = ?, @ErrorSeverity = N'Error', @ErrorCode = 51010, "
        "@ErrorDescription = N'Closing FX rate missing for one or more open-item currencies.', "
        "@SourceName = N'FIN_Currency_Revaluation';",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        is_stored_procedure=True,
    ))
    counts = pkg.add(log_row_count("Fact.Payment"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, rates)
    pkg.link(rates, missing)
    pkg.link(missing, revalue,
             expression="@[User::MissingRateCount] == 0 || !@[$Package::FailOnMissingRate]")
    pkg.link(missing, halt,
             expression="@[User::MissingRateCount] > 0 && @[$Package::FailOnMissingRate]")
    pkg.link(revalue, count_items)
    pkg.link(count_items, counts)
    pkg.link(halt, counts, value="Completion")
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# FIN_Load_WithholdingTax
# ---------------------------------------------------------------------------

WHT_SQL = """
SELECT  l.ApInvoiceLineId
,       l.ApInvoiceKey
,       l.SupplierId
,       l.SupplierTaxRegistrationNumber
,       l.JurisdictionCode
,       l.RegionCode
,       l.LineAmount
,       l.TaxCode
,       l.ServiceCategoryCode
,       ISNULL(t.WithholdingRatePercent, 0)     AS WithholdingRatePercent
,       ISNULL(t.WithholdingThresholdAmount, 0) AS WithholdingThresholdAmount
        /* Three jurisdictions, three completely different rules. NA withholds
           only for 1099-reportable service categories, EU applies the treaty
           rate when the supplier has a registration number, and APAC withholds
           on everything above the local threshold. */
,       CASE
            WHEN l.RegionCode = 'NA'
                 AND l.ServiceCategoryCode IN ('CONS', 'LEGL', 'MEDI', 'RENT')
                THEN l.LineAmount * ISNULL(t.WithholdingRatePercent, 0) / 100
            WHEN l.RegionCode = 'EU'
                 AND NULLIF(l.SupplierTaxRegistrationNumber, '') IS NOT NULL
                THEN l.LineAmount * ISNULL(t.TreatyRatePercent, t.WithholdingRatePercent) / 100
            WHEN l.RegionCode = 'EU'
                THEN l.LineAmount * ISNULL(t.WithholdingRatePercent, 0) / 100
            WHEN l.RegionCode = 'APAC'
                 AND l.LineAmount >= ISNULL(t.WithholdingThresholdAmount, 0)
                THEN l.LineAmount * ISNULL(t.WithholdingRatePercent, 0) / 100
            ELSE 0
        END                                     AS WithholdingAmount
FROM    stg.ApInvoiceLine AS l
LEFT OUTER JOIN stg.WithholdingTaxRate AS t
        ON  t.JurisdictionCode = l.JurisdictionCode
        AND t.ServiceCategoryCode = l.ServiceCategoryCode
        AND l.InvoiceDate BETWEEN t.EffectiveFrom AND ISNULL(t.EffectiveTo, '9999-12-31')
WHERE   l.LoadBatchId = ?
AND     l.LineTypeCode <> 'FREIGHT'
""".strip()


def build_fin_load_withholdingtax():
    pkg = new_package(
        "FIN_Load_WithholdingTax",
        "Splits AP invoice lines into net payable and withholding tax by jurisdiction and writes "
        "the withholding rows into the payment fact. The three regional regimes are genuinely "
        "different: NA 1099 service categories, EU treaty rates keyed on the supplier registration "
        "number, APAC threshold withholding.",
        source_system="ORAERP",
        connections=(CONN_DW,),
        extra_variables=[("UnmappedJurisdictionCount", 0, "int")],
    )
    pkg.add_parameter("JurisdictionScope", "ALL", dtype="string",
                      description="ALL, or a single jurisdiction code for a targeted rerun.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.WithholdingTaxLine"))

    columns = [
        int_col("ApInvoiceLineId"), int_col("ApInvoiceKey"), str_col("SupplierId", 20),
        str_col("SupplierTaxRegistrationNumber", 30), str_col("JurisdictionCode", 10),
        str_col("RegionCode", 4), money_col("LineAmount"), str_col("TaxCode", 10),
        str_col("ServiceCategoryCode", 6), money_col("WithholdingRatePercent"),
        money_col("WithholdingThresholdAmount"), money_col("WithholdingAmount"),
    ]
    flow = DataFlow("Split Withholding Tax")
    flow.oledb_source("stg ApInvoiceLine", CONN_STAGING, WHT_SQL, columns, timeout=1800)
    flow.derived_column("Derive Net Payable", [
        ("NetPayableAmount", "LineAmount - WithholdingAmount", money_col("NetPayableAmount")),
        ("IsWithheld", "WithholdingAmount > 0 ? (DT_BOOL)1 : (DT_BOOL)0", bool_col("IsWithheld")),
        ("WithholdingCertificateRequired",
         'RegionCode == "EU" && WithholdingAmount > 0 ? (DT_BOOL)1 : (DT_BOOL)0',
         bool_col("WithholdingCertificateRequired")),
    ])
    flow.conditional_split("Split Unmapped Jurisdictions", [
        ("Mapped", "WithholdingRatePercent > 0 || WithholdingAmount == 0"),
        ("Unmapped", "WithholdingRatePercent == 0 && WithholdingAmount > 0"),
    ])
    flow.row_count("Count Withholding Lines", "User::RowsRead")
    flow.oledb_destination("work WithholdingTaxLine", CONN_STAGING, "[work].[WithholdingTaxLine]",
                           batch_size=50000)
    flow.branch_destination("Reject Unmapped Jurisdiction", CONN_STAGING,
                            "[err].[WithholdingTaxReject]", "Split Unmapped Jurisdictions", "Unmapped")
    split = pkg.add(DataFlowTask(flow))

    post = pkg.add(exec_proc(
        "Post Withholding To Fact",
        "EXEC Integration.usp_PostWithholdingTax @BatchId = ?, @JurisdictionScope = ?, "
        "@RowsInserted = ? OUTPUT;",
        connection=CONN_DW,
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("$Package::JurisdictionScope", 1, "NVARCHAR"),
        ],
    ))
    certificates = pkg.add(ExecuteSql(
        "Queue EU Withholding Certificates",
        CONN_STAGING,
        "INSERT INTO work.WithholdingCertificateQueue "
        "    (SupplierId, JurisdictionCode, WithholdingAmount, QueuedAtUtc, BatchId) "
        "SELECT SupplierId, JurisdictionCode, SUM(WithholdingAmount), SYSUTCDATETIME(), ? "
        "FROM   work.WithholdingTaxLine "
        "WHERE  WithholdingCertificateRequired = 1 "
        "GROUP BY SupplierId, JurisdictionCode;",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    counts = pkg.add(log_row_count("Fact.Payment"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, split)
    pkg.link(split, post)
    pkg.link(post, certificates)
    pkg.link(certificates, counts)
    pkg.link(counts, done)
    return pkg


BUILDERS = [
    build_fin_load_apaging,
    build_fin_load_glpostings,
    build_fin_reconcile_subledger_to_gl,
    build_fin_close_periodlock,
    build_fin_load_costallocation,
    build_fin_currency_revaluation,
    build_fin_load_withholdingtax,
]


def main():
    written = []
    names = []
    for builder in BUILDERS:
        pkg = builder()
        written.append(pkg.write(os.path.join(HERE, pkg.name + ".dtsx")))
        names.append(pkg.name)
    written.extend(project.write_project(HERE, PROJECT_NAME, names, CONNECTIONS))
    for path in written:
        print(os.path.relpath(path, REPO_ROOT))


if __name__ == "__main__":
    main()
