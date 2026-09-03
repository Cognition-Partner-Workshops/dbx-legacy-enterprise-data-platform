"""Emit the WWI_Orchestration master packages (ssis/00_orchestration).

The master packages own the estate's cadences. They create the batch, drive the
child packages through phase sequence containers in dependency order, record a
batch step per phase, and close the batch out. Children are executed through
Execute Package Tasks using the package names declared in
config/estate-catalog.yaml.

Every master follows the same skeleton, which is deliberately the same skeleton
the operations team has maintained since the 2006 SSIS conversion:

    [Start Batch]
        -> <phase container> -> <phase container> -> ...
        -> [Reconcile Row Counts] -> [End Batch]

    OnError: [Log Error] -> [Mark Batch Failed]

Phase containers are chained with Success constraints where the dependency
graph is strict, Completion constraints where a phase is advisory, and
expression-and-constraint precedence where the phase is conditional (restart
from a named step, environment gating, reject thresholds, fiscal-period gating).

Run:  python3 ssis/00_orchestration/build_orchestration_packages.py
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import project  # noqa: E402
from patterns import CONN_DW, CONN_FILES, CONN_OLTP, CONN_STAGING  # noqa: E402
from ssisgen import Container, ExecuteSql, Expression, ExecutePackage, Package  # noqa: E402

PROJECT_NAME = "WWI_Orchestration"

# Phase step groups recognised by etl.BatchStep.StepGroup.
GROUP_EXTRACT = "Extract"
GROUP_STAGE = "Stage"
GROUP_QUALITY = "Quality"
GROUP_REFERENCE = "Reference"
GROUP_DIMENSION = "Dimension"
GROUP_FACT = "Fact"
GROUP_AGGREGATE = "Aggregate"
GROUP_MART = "Mart"
GROUP_MAINTENANCE = "Maintenance"


def _slug(text):
    return "".join(ch for ch in text.title() if ch.isalnum())


# ---------------------------------------------------------------------------
# master skeleton
# ---------------------------------------------------------------------------


def master_error_handler(pkg):
    """OnError: write the error away and force the batch to Failed.

    Unlike the child-package handler in tools/ssisgen/patterns.py this one owns
    the batch, so it closes the batch out rather than a package execution row.
    """
    log = ExecuteSql(
        "Log Master Error",
        CONN_STAGING,
        "EXEC etl.usp_LogError @BatchId = ?, @ErrorSeverity = N'Critical', @ErrorCode = ?, "
        "@ErrorDescription = ?, @SourceName = ?, @ProcedureName = N'master orchestration';",
        parameter_bindings=[
            ("User::BatchId", 0, "LONG"),
            ("System::ErrorCode", 1, "LONG"),
            ("System::ErrorDescription", 2, "NVARCHAR"),
            ("System::SourceName", 3, "NVARCHAR"),
        ],
        is_stored_procedure=True,
    )
    fail_step = ExecuteSql(
        "Fail Open Batch Step",
        CONN_STAGING,
        "UPDATE etl.BatchStep SET Status = N'Failed', CompletedAtUtc = SYSUTCDATETIME() "
        "WHERE BatchId = ? AND Status = N'Running';",
        parameter_bindings=[("User::BatchId", 0, "LONG")],
    )
    fail_batch = ExecuteSql(
        "Mark Batch Failed",
        CONN_STAGING,
        "EXEC etl.usp_EndBatch @BatchId = ?, @ForceStatus = N'Failed';",
        parameter_bindings=[("User::BatchId", 0, "LONG")],
        is_stored_procedure=True,
    )
    pkg.add_event_handler(
        "OnError",
        [log, fail_step, fail_batch],
        [(log, fail_step, "Completion", None, True), (fail_step, fail_batch, "Completion", None, True)],
    )
    return pkg


def master_package(name, description, connections=(), extra_parameters=(), extra_variables=()):
    pkg = Package(name, description=description)
    pkg.add_parameter("BatchId", 0, dtype="int",
                      description="Batch id to adopt on a restart. Zero means start a new batch.")
    pkg.add_parameter("BusinessDate", "1900-01-01", dtype="string",
                      description="Business date the run is loading. Defaults to the previous day at schedule time.")
    pkg.add_parameter("EnvironmentCode", "DEV", dtype="string",
                      description="DEV / TEST / PROD. Drives etl.Configuration lookups and notification routing.")
    pkg.add_parameter("ReloadFullHistory", "False", dtype="bool",
                      description="Passed down to every child; ignores stored watermarks and reloads all history.")
    pkg.add_parameter("MaxParallelStreams", 4, dtype="int",
                      description="Upper bound on concurrent child streams inside a phase container.")
    pkg.add_parameter("MaxExtractAttempts", 3, dtype="int",
                      description="Bound on the retry loop around the failure-prone extract phases.")
    pkg.add_parameter("RestartFromStep", "", dtype="string",
                      description="When set, phases before the named step are skipped so a failed batch can resume.")
    for param in extra_parameters:
        pkg.add_parameter(*param)

    pkg.add_variable("BatchId", 0, dtype="long")
    pkg.add_variable("ExtractAttempt", 1, dtype="int")
    pkg.add_variable("FailedObjectCount", 0, dtype="int")
    pkg.add_variable("RejectedRowCount", 0, dtype="int")
    pkg.add_variable("ErrorMessage", "", dtype="string")
    for variable in extra_variables:
        pkg.add_variable(*variable)

    pkg.use_connection(CONN_STAGING, *connections)
    pkg.add_sql_log_provider(CONN_STAGING)
    master_error_handler(pkg)
    return pkg


def start_batch(batch_type, notes):
    return ExecuteSql(
        "Start Batch",
        CONN_STAGING,
        "EXEC etl.usp_StartBatch @BatchName = ?, @BatchType = N'%s', @BusinessDate = ?, "
        "@EnvironmentCode = ?, @AllowAdoptRunning = ?, @Notes = N'%s', @BatchId = ? OUTPUT;"
        % (batch_type, notes),
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[
            ("System::PackageName", 0, "NVARCHAR"),
            ("$Package::BusinessDate", 1, "NVARCHAR"),
            ("$Package::EnvironmentCode", 2, "NVARCHAR"),
            ("$Package::BatchId", 3, "LONG"),
        ],
        result_bindings=[("0", "User::BatchId")],
        is_stored_procedure=True,
    )


def end_batch(name="End Batch", force_status=None):
    if force_status:
        sql = ("EXEC etl.usp_EndBatch @BatchId = ?, @ForceStatus = N'%s';" % force_status)
    else:
        sql = "EXEC etl.usp_EndBatch @BatchId = ?;"
    return ExecuteSql(name, CONN_STAGING, sql,
                      parameter_bindings=[("User::BatchId", 0, "LONG")], is_stored_procedure=True)


def reconcile_row_counts(name="Reconcile Row Counts", raise_on_failure=1):
    return ExecuteSql(
        name,
        CONN_STAGING,
        "EXEC etl.usp_AssertRowCountReconciliation @BatchId = ?, @RaiseOnFailure = %d, "
        "@FailedObjectCount = ? OUTPUT;" % raise_on_failure,
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("User::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::FailedObjectCount")],
        is_stored_procedure=True,
    )


def child(package_name, parameter_assignments=None, task_name=None):
    assignments = parameter_assignments or [
        ("BatchId", "User::BatchId"),
        ("ReloadFullHistory", "$Package::ReloadFullHistory"),
    ]
    return ExecutePackage(task_name or ("Run %s" % package_name), package_name, assignments)


def phase(pkg, title, sequence, group, children, streams=1, serialise=False,
          step_variable=None, parameter_assignments=None):
    """Build one phase container: start step -> child packages -> end step.

    ``streams`` splits the children into that many parallel chains; ``serialise``
    forces a single chain regardless (used where the dependency graph does not
    allow concurrency, for example the dimension-then-fact ordering).
    """
    variable = step_variable or ("BatchStepId%s" % _slug(title))
    pkg.add_variable(variable, 0, dtype="long")
    container = Container(title, description="Phase: %s" % title)

    start = container.add(ExecuteSql(
        "Start Step %s" % title,
        CONN_STAGING,
        "EXEC etl.usp_StartBatchStep @BatchId = ?, @StepName = N'%s', @StepSequence = %d, "
        "@StepGroup = N'%s', @BatchStepId = ? OUTPUT;" % (title, sequence, group),
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("User::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::%s" % variable)],
        is_stored_procedure=True,
    ))
    finish = ExecuteSql(
        "End Step %s" % title,
        CONN_STAGING,
        "EXEC etl.usp_EndBatchStep @BatchStepId = ?, @Status = N'Succeeded';",
        parameter_bindings=[("User::%s" % variable, 0, "LONG")],
        is_stored_procedure=True,
    )

    tasks = [container.add(child(name, parameter_assignments)) for name in children]
    if serialise or streams <= 1:
        lanes = [tasks]
    else:
        lanes = [tasks[index::streams] for index in range(streams)]
        lanes = [lane for lane in lanes if lane]
    for lane in lanes:
        container.link(start, lane[0])
        for previous, following in zip(lane, lane[1:]):
            container.link(previous, following)
        container.link(lane[-1], finish)
    container.add(finish)
    return pkg.add(container)


def skip_expression(step_name):
    """Restart gating: run the phase unless the operator resumed past it."""
    return ('@[$Package::RestartFromStep] == "" || @[$Package::RestartFromStep] == "%s"' % step_name)


# ---------------------------------------------------------------------------
# Master_Daily_ETL
# ---------------------------------------------------------------------------

ORACLE_EXTRACTS = [
    "EXT_ORA_CustomerMaster", "EXT_ORA_CustomerAddress", "EXT_ORA_ProductMaster",
    "EXT_ORA_ProductHierarchy", "EXT_ORA_SupplierMaster", "EXT_ORA_PurchaseOrderHdr",
    "EXT_ORA_PurchaseOrderLine", "EXT_ORA_ReceiptLine", "EXT_ORA_ApInvoiceHdr",
    "EXT_ORA_ApInvoiceLine", "EXT_ORA_ApPayment", "EXT_ORA_ApPaymentApply",
    "EXT_ORA_ApAging", "EXT_ORA_GlJournalLine", "EXT_ORA_CostCenter",
    "EXT_ORA_Currency", "EXT_ORA_FxRateDaily", "EXT_ORA_TaxRate",
    "EXT_ORA_PaymentTerms", "EXT_ORA_Geography", "EXT_ORA_CodeTranslation",
    "EXT_ORA_VendorContract",
]

SQL_EXTRACTS = [
    "EXT_SQL_Orders", "EXT_SQL_OrderLines", "EXT_SQL_Invoices", "EXT_SQL_InvoiceLines",
    "EXT_SQL_Shipments", "EXT_SQL_ShipmentLines", "EXT_SQL_Returns", "EXT_SQL_CreditNotes",
    "EXT_SQL_CustomerTransactions", "EXT_SQL_SupplierTransactions", "EXT_SQL_StockItems",
    "EXT_SQL_StockMovements", "EXT_SQL_StockTransfers", "EXT_SQL_People", "EXT_SQL_Cities",
    "EXT_SQL_SalesTerritories", "EXT_SQL_CustomerSegments", "EXT_SQL_PaymentMethods",
    "EXT_SQL_TransactionTypes", "EXT_SQL_Promotions", "EXT_SQL_WebSessions",
]

STAGING_LOADS = [
    "STG_Load_Customer", "STG_Load_CustomerAddress", "STG_Load_Product", "STG_Load_StockItem",
    "STG_Load_Supplier", "STG_Load_Employee", "STG_Load_Geography", "STG_Load_Currency",
    "STG_Load_TaxAndTerms", "STG_Load_PromotionAndTerritory", "STG_Load_Order", "STG_Load_Sale",
    "STG_Load_Shipment", "STG_Load_ReturnAndCredit", "STG_Load_Payment", "STG_Load_ApInvoice",
    "STG_Load_GlJournal", "STG_Load_CostCenter", "STG_Load_PurchaseOrder", "STG_Load_VendorContract",
    "STG_Load_StockMovement", "STG_Load_LoyaltyLedger", "STG_Load_PartnerSale", "STG_Load_WebSession",
]

STAGING_WORK = [
    "STG_Work_CustomerDedup", "STG_Work_ProductCrosswalk", "STG_Work_PaymentMatch",
    "STG_Work_InventoryPosition",
]

QUALITY_SCREENS = [
    "DQ_Rule_Engine", "DQ_Customer_Screen", "DQ_Supplier_Screen", "DQ_OrderLine_Screen",
    "DQ_InvoiceLine_Screen", "DQ_Payment_Screen", "DQ_Referential_Screen", "DQ_Threshold_Gate",
]

DIMENSIONS = [
    "DIM_Load_City", "DIM_Load_CustomerCategory", "DIM_Load_CustomerSegment",
    "DIM_NA_Load_Customer", "DIM_EU_Load_Customer", "DIM_APAC_Load_Customer",
    "DIM_Load_StockItem", "DIM_Load_ProductCategory", "DIM_Load_Supplier",
    "DIM_Load_VendorContract", "DIM_Load_Employee", "DIM_Load_Salesperson",
    "DIM_Load_SalesTerritory", "DIM_Load_Promotion", "DIM_Rekey_LateArriving",
]

FACTS = [
    "FACT_Load_Order", "FACT_NA_Load_Sale", "FACT_EU_Load_Sale", "FACT_APAC_Load_Sale",
    "FACT_Dedup_Sale", "FACT_Load_Shipment", "FACT_Load_OrderFulfilment", "FACT_Load_Return",
    "FACT_Load_CreditNote", "FACT_Load_Payment", "FACT_Load_SupplierPayment",
    "FACT_Load_CustomerTransaction", "FACT_Load_SupplierTransaction", "FACT_Load_Transaction",
    "FACT_Load_Purchase", "FACT_Load_PurchaseReceipt", "FACT_Load_Movement",
    "FACT_Load_StockHolding", "FACT_Load_DailyInventorySnapshot", "FACT_Load_DailySalesSnapshot",
    "FACT_Load_GLPosting", "FACT_Load_LoyaltyPoints", "FACT_Load_WebSession",
]


def build_master_daily_etl():
    pkg = master_package(
        "Master_Daily_ETL",
        "Nightly full warehouse load: extract -> stage -> quality -> reference -> dimensions -> "
        "facts -> aggregates -> domain marts -> publish. Supports resuming a failed batch from a "
        "named step and retries the two extract phases up to MaxExtractAttempts.",
        connections=(CONN_OLTP, CONN_DW),
    )

    begin = pkg.add(start_batch("Daily", "Nightly warehouse load driven by the WWI - Daily ETL agent job."))
    announce = pkg.add(Expression(
        "Initialise Attempt Counter",
        "@[User::ExtractAttempt] = 1",
    ))
    pkg.link(begin, announce)

    oracle = phase(pkg, "Extract Oracle", 10, GROUP_EXTRACT, ORACLE_EXTRACTS, streams=4)
    sqlsrc = phase(pkg, "Extract SQL Server", 20, GROUP_EXTRACT, SQL_EXTRACTS, streams=3)

    # Bounded, expression-driven retry around the failure-prone extract phases.
    # The ERP link drops often enough that the 2009 change control added a second
    # attempt rather than failing the whole night.
    bump = pkg.add(Expression(
        "Increment Extract Attempt",
        "@[User::ExtractAttempt] = @[User::ExtractAttempt] + 1",
    ))
    record_attempt = pkg.add(ExecuteSql(
        "Record Extract Attempt",
        CONN_STAGING,
        "UPDATE etl.BatchStep SET AttemptNumber = ? WHERE BatchId = ? AND StepGroup = N'Extract' "
        "AND Status = N'Failed';",
        parameter_bindings=[("User::ExtractAttempt", 0, "LONG"), ("User::BatchId", 1, "LONG")],
    ))
    retry_oracle = phase(pkg, "Extract Oracle Retry", 11, GROUP_EXTRACT, ORACLE_EXTRACTS, streams=2)
    retry_sql = phase(pkg, "Extract SQL Server Retry", 21, GROUP_EXTRACT, SQL_EXTRACTS, streams=2)

    stage = phase(pkg, "Stage Load", 30, GROUP_STAGE, STAGING_LOADS, streams=4)
    stage_work = phase(pkg, "Stage Work Tables", 35, GROUP_STAGE, STAGING_WORK, serialise=True)
    quality = phase(pkg, "Data Quality", 40, GROUP_QUALITY, QUALITY_SCREENS, streams=2)
    reject_route = phase(pkg, "Reject Routing", 45, GROUP_QUALITY,
                         ["DQ_Reject_Reprocess", "ERR_Route_RejectedRows"], serialise=True)
    reference = phase(pkg, "Reference Refresh", 50, GROUP_REFERENCE,
                      ["REF_Load_UnknownMembers", "REF_Load_CodeTranslation", "REF_Load_Currency"],
                      serialise=True)
    dimensions = phase(pkg, "Dimensions", 60, GROUP_DIMENSION, DIMENSIONS, streams=3)
    facts = phase(pkg, "Facts", 70, GROUP_FACT, FACTS, streams=4)
    aggregates = phase(pkg, "Aggregates", 80, GROUP_AGGREGATE,
                       ["AGG_Refresh_DailySalesSummary", "AGG_Refresh_DailyInventoryHealth"], streams=2)
    sales_mart = phase(pkg, "Sales Mart", 90, GROUP_MART,
                       ["SLS_NA_Load_Commission", "SLS_EU_Load_Commission", "SLS_APAC_Load_Commission",
                        "SLS_Load_QuotaAttainment", "SLS_Load_PromotionRedemption",
                        "SLS_Export_PartnerFeed"], streams=3)
    inventory_mart = phase(pkg, "Inventory Mart", 91, GROUP_MART,
                           ["INV_Load_DailySnapshot", "INV_Load_StockTransfer",
                            "INV_Load_CycleCountVariance", "INV_Load_Replenishment",
                            "INV_Reconcile_OnHand"], streams=2)
    procurement_mart = phase(pkg, "Procurement Mart", 92, GROUP_MART,
                             ["PRC_Load_PurchaseSpend", "PRC_Load_ReceiptMatching",
                              "PRC_Load_ContractCompliance", "PRC_Load_SupplierScorecard",
                              "PRC_Export_SupplierStatement"], streams=2)
    customer_mart = phase(pkg, "Customer 360", 93, GROUP_MART,
                          ["C360_Build_CustomerProfile", "C360_Build_RollingMetrics",
                           "C360_Build_LoyaltyOverlay", "C360_Build_ChurnFlags",
                           "C360_Publish_Segments"], serialise=True)
    publish = phase(pkg, "Publish Reporting Layer", 95, GROUP_AGGREGATE,
                    ["AGG_Publish_ReportingLayer"], serialise=True)
    failure_paths = phase(pkg, "Failure Handling", 98, GROUP_MAINTENANCE,
                          ["ERR_Reconcile_RowCounts", "ERR_Handle_PackageFailure",
                           "ERR_Notify_Operations"], serialise=True)

    reconcile = pkg.add(reconcile_row_counts())
    close = pkg.add(end_batch())

    # Extract phases: success continues, failure enters the bounded retry.
    pkg.link(announce, oracle, expression=skip_expression("Extract Oracle"))
    pkg.link(oracle, sqlsrc)
    pkg.link(oracle, bump, value="Failure")
    pkg.link(sqlsrc, bump, value="Failure")
    pkg.link(bump, record_attempt)
    pkg.link(record_attempt, retry_oracle,
             expression="@[User::ExtractAttempt] <= @[$Package::MaxExtractAttempts]")
    pkg.link(retry_oracle, retry_sql)
    pkg.link(retry_oracle, failure_paths, value="Failure")
    pkg.link(retry_sql, stage)
    pkg.link(retry_sql, failure_paths, value="Failure")

    pkg.link(sqlsrc, stage, expression=skip_expression("Stage Load"))
    pkg.link(stage, stage_work)
    pkg.link(stage_work, quality)
    # The quality gate is advisory: rejects are routed and the load continues
    # unless DQ_Threshold_Gate raised, which fails the container.
    pkg.link(quality, reject_route, value="Completion")
    pkg.link(reject_route, reference, value="Completion")
    pkg.link(quality, failure_paths, value="Failure")
    pkg.link(reference, dimensions)
    pkg.link(dimensions, facts)
    pkg.link(facts, aggregates)
    pkg.link(aggregates, sales_mart)
    pkg.link(aggregates, inventory_mart)
    pkg.link(aggregates, procurement_mart)
    pkg.link(sales_mart, customer_mart)
    pkg.link(inventory_mart, customer_mart)
    pkg.link(procurement_mart, customer_mart)
    pkg.link(customer_mart, publish)
    pkg.link(publish, reconcile)
    pkg.link(reconcile, close)
    pkg.link(failure_paths, close)
    return pkg


# ---------------------------------------------------------------------------
# Master_Hourly_Incremental
# ---------------------------------------------------------------------------


def build_master_hourly_incremental():
    pkg = master_package(
        "Master_Hourly_Incremental",
        "Hourly incremental order, invoice and shipment load. Runs 06:00-22:00 on the hour, skips "
        "itself when the nightly batch is still running, and keeps its own watermark window.",
        connections=(CONN_OLTP, CONN_DW),
        extra_parameters=[("SkipWhenNightlyRunning", "True", "bool", False, False,
                           "Intraday runs stand down while a Daily batch is open.")],
        extra_variables=[("NightlyBatchRunning", 0, "int"), ("OrdersChanged", 0, "int")],
    )

    guard = pkg.add(ExecuteSql(
        "Check Nightly Batch",
        CONN_STAGING,
        "SELECT COUNT(*) AS RunningBatches FROM etl.Batch "
        "WHERE BatchType = N'Daily' AND Status = N'Running';",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::NightlyBatchRunning")],
    ))
    begin = pkg.add(start_batch("Intraday", "Hourly incremental slice."))
    pkg.link(guard, begin,
             expression="@[User::NightlyBatchRunning] == 0 || !@[$Package::SkipWhenNightlyRunning]")

    extract = phase(pkg, "Incremental Extract", 10, GROUP_EXTRACT,
                    ["EXT_SQL_Orders", "EXT_SQL_OrderLines", "EXT_SQL_Invoices",
                     "EXT_SQL_InvoiceLines", "EXT_SQL_Shipments", "EXT_SQL_ShipmentLines"],
                    streams=2)
    retry = phase(pkg, "Incremental Extract Retry", 11, GROUP_EXTRACT,
                  ["EXT_SQL_Orders", "EXT_SQL_Invoices", "EXT_SQL_Shipments"], serialise=True)
    bump = pkg.add(Expression("Increment Extract Attempt",
                              "@[User::ExtractAttempt] = @[User::ExtractAttempt] + 1"))
    stage = phase(pkg, "Incremental Stage", 20, GROUP_STAGE,
                  ["STG_Load_Order", "STG_Load_Sale", "STG_Load_Shipment"], serialise=True)
    screen = phase(pkg, "Intraday Screen", 30, GROUP_QUALITY,
                   ["DQ_OrderLine_Screen", "DQ_InvoiceLine_Screen"], streams=2)
    facts = phase(pkg, "Intraday Facts", 40, GROUP_FACT,
                  ["FACT_Load_Order", "FACT_Load_Shipment", "FACT_Load_OrderFulfilment"], serialise=True)
    aggregate = phase(pkg, "Intraday Aggregate", 50, GROUP_AGGREGATE,
                      ["AGG_Refresh_DailySalesSummary"], serialise=True)
    notify = phase(pkg, "Intraday Failure Notice", 90, GROUP_MAINTENANCE,
                   ["ERR_Notify_Operations"], serialise=True)

    ingest = pkg.add(child("ING_FILE_FxOverride"))
    carrier = pkg.add(child("ING_FILE_CarrierScan"))
    partner_na = pkg.add(child("ING_FILE_PartnerSales_NA"))
    partner_eu = pkg.add(child("ING_FILE_PartnerSales_EU"))
    partner_apac = pkg.add(child("ING_FILE_PartnerSales_APAC"))
    catalog = pkg.add(child("ING_FILE_SupplierCatalog"))
    quarantine = pkg.add(child("ING_FILE_QuarantineMalformed"))

    close = pkg.add(end_batch())
    stand_down = pkg.add(ExecuteSql(
        "Log Stand Down",
        CONN_STAGING,
        "EXEC etl.usp_LogError @BatchId = NULL, @ErrorSeverity = N'Information', @ErrorCode = 0, "
        "@ErrorDescription = N'Intraday run skipped: a Daily batch is still running.', "
        "@SourceName = N'Master_Hourly_Incremental';",
        is_stored_procedure=True,
    ))
    pkg.link(guard, stand_down,
             expression="@[User::NightlyBatchRunning] > 0 && @[$Package::SkipWhenNightlyRunning]")

    pkg.link(begin, extract)
    pkg.link(extract, bump, value="Failure")
    pkg.link(bump, retry, expression="@[User::ExtractAttempt] <= @[$Package::MaxExtractAttempts]")
    pkg.link(retry, stage)
    pkg.link(retry, notify, value="Failure")
    pkg.link(extract, stage)
    # Partner and carrier drops land on the hour as well; they run in their own
    # stream and never block the OLTP slice.
    pkg.link(begin, ingest)
    pkg.link(ingest, partner_na)
    pkg.link(partner_na, partner_eu)
    pkg.link(partner_eu, partner_apac)
    pkg.link(begin, carrier)
    pkg.link(carrier, catalog)
    pkg.link(partner_apac, quarantine, value="Completion")
    pkg.link(catalog, quarantine, value="Completion")
    pkg.link(stage, screen)
    pkg.link(screen, facts, value="Completion")
    pkg.link(facts, aggregate)
    pkg.link(aggregate, close)
    pkg.link(quarantine, close)
    pkg.link(notify, close)
    return pkg


# ---------------------------------------------------------------------------
# Master_Finance_Close
# ---------------------------------------------------------------------------


def build_master_finance_close():
    pkg = master_package(
        "Master_Finance_Close",
        "Month-end finance close sequence. Gated on the accounting period being open, runs the "
        "subledger loads, revalues open items at the month-end rate, ties the subledger to the GL "
        "and only then locks the period.",
        connections=(CONN_DW,),
        extra_parameters=[
            ("AccountingPeriod", "1900-01", "string", False, False,
             "YYYY-MM accounting period being closed."),
            ("AllowCloseWithVariance", "False", "bool", False, False,
             "Set by the controller to close over an unexplained subledger-to-GL variance."),
        ],
        extra_variables=[("PeriodStatus", "", "string"), ("SubledgerVariance", 0, "decimal")],
    )

    begin = pkg.add(start_batch("Monthly", "Month-end finance close."))
    period_check = pkg.add(ExecuteSql(
        "Read Period Status",
        CONN_STAGING,
        "SELECT TOP (1) ConfigurationValue AS PeriodStatus FROM etl.Configuration "
        "WHERE ConfigurationKey = N'Finance.PeriodStatus' "
        "AND EnvironmentCode IN (?, N'ALL') ORDER BY EnvironmentCode DESC;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::EnvironmentCode", 0, "NVARCHAR")],
        result_bindings=[("0", "User::PeriodStatus")],
    ))
    pkg.link(begin, period_check)

    subledger = phase(pkg, "Subledger Loads", 10, GROUP_MART,
                      ["FIN_Load_ApAging", "FIN_Load_WithholdingTax", "FIN_Load_CostAllocation"],
                      streams=2)
    revalue = phase(pkg, "FX Revaluation", 20, GROUP_MART, ["FIN_Currency_Revaluation"], serialise=True)
    gl = phase(pkg, "General Ledger", 30, GROUP_MART, ["FIN_Load_GlPostings"], serialise=True)
    tie_out = phase(pkg, "Subledger Tie Out", 40, GROUP_MART,
                    ["FIN_Reconcile_SubledgerToGl"], serialise=True)
    close_summary = phase(pkg, "Close Aggregates", 50, GROUP_AGGREGATE,
                          ["AGG_Refresh_FinanceCloseSummary", "AGG_Refresh_MonthlyMarginAnalysis"],
                          streams=2)
    lock = phase(pkg, "Period Lock", 60, GROUP_MART, ["FIN_Close_PeriodLock"], serialise=True)
    escalate = phase(pkg, "Close Escalation", 90, GROUP_MAINTENANCE,
                     ["ERR_Notify_Operations", "ERR_Reconcile_RowCounts"], serialise=True)

    read_variance = pkg.add(ExecuteSql(
        "Read Subledger Variance",
        CONN_STAGING,
        "SELECT ISNULL(SUM(ABS(ra.SourceRowCount - ra.TargetRowCount)), 0) AS Variance "
        "FROM etl.RowCountAudit AS ra "
        "INNER JOIN etl.PackageExecution AS pe ON pe.PackageExecutionId = ra.PackageExecutionId "
        "WHERE pe.BatchId = ? AND ra.ObjectName IN (N'Fact.Payment', N'Fact.GL Posting');",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("User::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::SubledgerVariance")],
    ))
    reconcile = pkg.add(reconcile_row_counts(raise_on_failure=0))
    close = pkg.add(end_batch())
    hold = pkg.add(end_batch("End Batch With Warnings", force_status="SucceededWithWarnings"))

    pkg.link(period_check, subledger, expression='@[User::PeriodStatus] != "Closed"')
    pkg.link(period_check, escalate, expression='@[User::PeriodStatus] == "Closed"')
    pkg.link(subledger, revalue)
    pkg.link(revalue, gl)
    pkg.link(gl, tie_out)
    pkg.link(tie_out, read_variance, value="Completion")
    pkg.link(read_variance, close_summary,
             expression="@[User::SubledgerVariance] == 0 || @[$Package::AllowCloseWithVariance]")
    pkg.link(read_variance, escalate,
             expression="@[User::SubledgerVariance] != 0 && !@[$Package::AllowCloseWithVariance]")
    pkg.link(close_summary, lock)
    pkg.link(lock, reconcile)
    pkg.link(reconcile, close)
    pkg.link(escalate, hold)
    return pkg


# ---------------------------------------------------------------------------
# Master_Weekly_Reference_Load
# ---------------------------------------------------------------------------


def build_master_weekly_reference_load():
    pkg = master_package(
        "Master_Weekly_Reference_Load",
        "Weekly full refresh of reference data. Reference tables are small and are always reloaded "
        "in full, then the unknown members are re-seeded and the code translation map is rebuilt "
        "before any dimension can use it.",
        connections=(CONN_DW,),
        extra_parameters=[("FailOnCodeMapGap", "True", "bool", False, False,
                           "Fail the run when a source code has no translation row.")],
        extra_variables=[("UnmappedCodeCount", 0, "int")],
    )

    begin = pkg.add(start_batch("Reference", "Weekly reference-data refresh."))
    force_full = pkg.add(Expression(
        "Force Full Reload",
        "@[User::ExtractAttempt] = 1",
    ))
    pkg.link(begin, force_full)

    geography = phase(pkg, "Geography And Sites", 10, GROUP_REFERENCE,
                      ["REF_Load_Geography", "REF_Load_WarehouseSite"], serialise=True)
    money = phase(pkg, "Currency And Terms", 20, GROUP_REFERENCE,
                  ["REF_Load_Currency", "REF_Load_PaymentTerms", "REF_Load_PaymentMethod"], streams=3)
    commercial = phase(pkg, "Commercial Codes", 30, GROUP_REFERENCE,
                       ["REF_Load_SalesChannel", "REF_Load_TransactionType", "REF_Load_ReturnReason",
                        "REF_Load_LoyaltyTier", "REF_Load_Carrier", "REF_Load_CostCenter"], streams=2)
    calendar = phase(pkg, "Calendar", 40, GROUP_REFERENCE, ["REF_Load_DateDimension"], serialise=True)
    translation = phase(pkg, "Code Translation", 50, GROUP_REFERENCE,
                        ["REF_Load_CodeTranslation", "REF_Load_UnknownMembers"], serialise=True)
    rekey = phase(pkg, "Late Arriving Rekey", 60, GROUP_DIMENSION,
                  ["DIM_Rekey_LateArriving"], serialise=True)
    escalate = phase(pkg, "Unmapped Code Escalation", 90, GROUP_MAINTENANCE,
                     ["ERR_Notify_Operations"], serialise=True)

    gap_check = pkg.add(ExecuteSql(
        "Count Unmapped Codes",
        CONN_STAGING,
        "SELECT COUNT(*) AS UnmappedCodes FROM etl.RejectedRecord "
        "WHERE BatchId = ? AND RejectReasonCode = N'CODE_UNMAPPED' AND IsReprocessed = 0;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("User::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::UnmappedCodeCount")],
    ))
    close = pkg.add(end_batch())
    warn_close = pkg.add(end_batch("End Batch With Warnings", force_status="SucceededWithWarnings"))

    pkg.link(force_full, geography)
    pkg.link(geography, money)
    pkg.link(geography, commercial)
    pkg.link(money, calendar)
    pkg.link(commercial, calendar)
    pkg.link(calendar, translation)
    pkg.link(translation, gap_check, value="Completion")
    pkg.link(gap_check, rekey, expression="@[User::UnmappedCodeCount] == 0")
    pkg.link(gap_check, escalate, expression="@[User::UnmappedCodeCount] > 0")
    pkg.link(rekey, close)
    pkg.link(escalate, warn_close,
             expression="!@[$Package::FailOnCodeMapGap]")
    return pkg


# ---------------------------------------------------------------------------
# Master_Month_End
# ---------------------------------------------------------------------------


def build_master_month_end():
    pkg = master_package(
        "Master_Month_End",
        "Month-end snapshot, corrections and aggregate rebuild. Applies the accounting corrections "
        "the business raised during the month, rebuilds every monthly aggregate and refreshes the "
        "customer marts on top of the corrected facts.",
        connections=(CONN_DW,),
        extra_parameters=[
            ("SnapshotMonth", "1900-01", "string", False, False, "YYYY-MM month being snapshotted."),
            ("RebuildAllAggregates", "True", "bool", False, False,
             "Rebuild every aggregate rather than only the ones flagged stale."),
        ],
        extra_variables=[("CorrectionCount", 0, "int")],
    )

    begin = pkg.add(start_batch("Monthly", "Month-end aggregate rebuild."))
    count_corrections = pkg.add(ExecuteSql(
        "Count Pending Corrections",
        CONN_STAGING,
        "SELECT COUNT(*) AS PendingCorrections FROM etl.RejectedRecord "
        "WHERE IsReprocessed = 0 AND RejectStage IN (N'Fact', N'Dimension');",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::CorrectionCount")],
    ))
    pkg.link(begin, count_corrections)

    corrections = phase(pkg, "Fact Corrections", 10, GROUP_FACT,
                        ["FACT_Apply_Corrections", "FACT_Dedup_Sale"], serialise=True)
    sales_aggregates = phase(pkg, "Sales Aggregates", 20, GROUP_AGGREGATE,
                             ["AGG_Refresh_MonthlySalesSummary", "AGG_Refresh_RegionalSalesPerformance",
                              "AGG_Refresh_MonthlyMarginAnalysis", "AGG_Refresh_PromotionEffectiveness"],
                             streams=2)
    ops_aggregates = phase(pkg, "Operations Aggregates", 30, GROUP_AGGREGATE,
                           ["AGG_Refresh_ProductPerformance", "AGG_Refresh_SupplierPerformance",
                            "AGG_Refresh_DeliveryPerformanceSummary"], streams=3)
    finance_aggregates = phase(pkg, "Finance Aggregates", 40, GROUP_AGGREGATE,
                               ["AGG_Refresh_FinanceCloseSummary"], serialise=True)
    customer = phase(pkg, "Customer Aggregates", 50, GROUP_AGGREGATE,
                     ["AGG_Refresh_CustomerRolling12Month", "AGG_Refresh_Customer360"], serialise=True)
    mart = phase(pkg, "Customer Mart Refresh", 60, GROUP_MART,
                 ["C360_Build_RollingMetrics", "C360_Build_ChurnFlags", "C360_Publish_Segments"],
                 serialise=True)
    housekeeping = phase(pkg, "Month End Housekeeping", 80, GROUP_MAINTENANCE,
                         ["MNT_Update_Statistics", "MNT_Purge_ControlHistory"], serialise=True)
    escalate = phase(pkg, "Month End Escalation", 90, GROUP_MAINTENANCE,
                     ["ERR_Reconcile_RowCounts", "ERR_Notify_Operations"], serialise=True)

    reconcile = pkg.add(reconcile_row_counts())
    close = pkg.add(end_batch())

    pkg.link(count_corrections, corrections, expression="@[User::CorrectionCount] > 0")
    pkg.link(count_corrections, sales_aggregates, expression="@[User::CorrectionCount] == 0")
    pkg.link(corrections, sales_aggregates)
    pkg.link(corrections, escalate, value="Failure")
    pkg.link(sales_aggregates, ops_aggregates)
    pkg.link(sales_aggregates, finance_aggregates)
    pkg.link(ops_aggregates, customer)
    pkg.link(finance_aggregates, customer)
    pkg.link(customer, mart)
    pkg.link(mart, housekeeping, value="Completion")
    pkg.link(housekeeping, reconcile)
    pkg.link(reconcile, close)
    pkg.link(escalate, close)
    return pkg


# ---------------------------------------------------------------------------
# Master_Weekly_Maintenance
# ---------------------------------------------------------------------------


def build_master_weekly_maintenance():
    pkg = master_package(
        "Master_Weekly_Maintenance",
        "Weekend maintenance window. Pre-flight checks first, then purge, then index and statistics "
        "maintenance, then archive. Everything after the pre-flight uses completion precedence so a "
        "single failed housekeeping task never leaves the window half done.",
        connections=(CONN_DW, CONN_FILES),
        extra_parameters=[
            ("MaintenanceWindowMinutes", 240, "int", False, False,
             "Operator-declared window; long-running steps are skipped when it is short."),
            ("SkipIndexRebuild", "False", "bool", False, False,
             "Set during quarter end when the window is too small for a rebuild."),
        ],
        extra_variables=[("FreeSpaceGb", 0, "int")],
    )

    begin = pkg.add(start_batch("Maintenance", "Weekend maintenance window."))
    preflight = phase(pkg, "Pre Flight", 10, GROUP_MAINTENANCE,
                      ["MNT_Check_DiskSpace", "MNT_Validate_Configuration"], serialise=True)
    purge = phase(pkg, "Purge", 20, GROUP_MAINTENANCE,
                  ["MNT_Purge_StagingHistory", "MNT_Purge_ControlHistory"], serialise=True)
    indexes = phase(pkg, "Index Maintenance", 30, GROUP_MAINTENANCE,
                    ["MNT_Rebuild_Indexes"], serialise=True)
    statistics = phase(pkg, "Statistics", 40, GROUP_MAINTENANCE,
                       ["MNT_Update_Statistics"], serialise=True)
    archive = phase(pkg, "Archive", 50, GROUP_MAINTENANCE,
                    ["MNT_Archive_ProcessedFiles"], serialise=True)
    rejects = phase(pkg, "Reject Housekeeping", 60, GROUP_MAINTENANCE,
                    ["ERR_Route_RejectedRows", "ERR_Reconcile_RowCounts"], streams=2)
    notify = phase(pkg, "Maintenance Notice", 90, GROUP_MAINTENANCE,
                   ["ERR_Notify_Operations"], serialise=True)

    close = pkg.add(end_batch())

    pkg.link(begin, preflight)
    pkg.link(preflight, purge)
    pkg.link(preflight, notify, value="Failure")
    pkg.link(purge, indexes,
             expression="!@[$Package::SkipIndexRebuild] && @[$Package::MaintenanceWindowMinutes] >= 120")
    pkg.link(purge, statistics,
             expression="@[$Package::SkipIndexRebuild] || @[$Package::MaintenanceWindowMinutes] < 120")
    pkg.link(indexes, statistics, value="Completion")
    pkg.link(statistics, archive, value="Completion")
    pkg.link(archive, rejects, value="Completion")
    pkg.link(rejects, close, value="Completion")
    pkg.link(notify, close)
    return pkg


# ---------------------------------------------------------------------------
# Master_Customer_Sync
# ---------------------------------------------------------------------------


def build_master_customer_sync():
    pkg = master_package(
        "Master_Customer_Sync",
        "Nightly customer master synchronisation from the ERP. Runs ahead of the main nightly load "
        "so that the regional customer dimensions and the customer-360 mart see the same ERP "
        "snapshot; the three regional dimension loads run as separate streams because each region "
        "applies its own consent and retention rules.",
        connections=(CONN_OLTP, CONN_DW),
        extra_parameters=[
            ("SuppressConsentWithdrawn", "True", "bool", False, False,
             "EU consent withdrawals are suppressed from the mart on the same night they arrive."),
            ("DedupeThresholdScore", 85, "int", False, False,
             "Identity-resolution match score at or above which two customers are merged."),
        ],
        extra_variables=[("SourceCustomerCount", 0, "int"), ("DedupeCandidateCount", 0, "int")],
    )

    begin = pkg.add(start_batch("Daily", "Customer master synchronisation."))
    extract = phase(pkg, "Customer Extract", 10, GROUP_EXTRACT,
                    ["EXT_ORA_CustomerMaster", "EXT_ORA_CustomerAddress", "EXT_ORA_Geography",
                     "EXT_SQL_CustomerSegments", "EXT_SQL_People"], streams=2)
    retry = phase(pkg, "Customer Extract Retry", 11, GROUP_EXTRACT,
                  ["EXT_ORA_CustomerMaster", "EXT_ORA_CustomerAddress"], serialise=True)
    bump = pkg.add(Expression("Increment Extract Attempt",
                              "@[User::ExtractAttempt] = @[User::ExtractAttempt] + 1"))
    stage = phase(pkg, "Customer Stage", 20, GROUP_STAGE,
                  ["STG_Load_Customer", "STG_Load_CustomerAddress", "STG_Load_Employee"], serialise=True)
    dedupe = phase(pkg, "Identity Resolution", 30, GROUP_STAGE,
                   ["STG_Work_CustomerDedup"], serialise=True)
    screen = phase(pkg, "Customer Screen", 40, GROUP_QUALITY,
                   ["DQ_Customer_Screen", "DQ_Referential_Screen"], serialise=True)
    dimensions = phase(pkg, "Regional Customer Dimensions", 50, GROUP_DIMENSION,
                       ["DIM_NA_Load_Customer", "DIM_EU_Load_Customer", "DIM_APAC_Load_Customer"],
                       streams=3)
    support = phase(pkg, "Customer Support Dimensions", 55, GROUP_DIMENSION,
                    ["DIM_Load_CustomerCategory", "DIM_Load_CustomerSegment", "DIM_Load_City"], streams=2)
    mart = phase(pkg, "Customer Mart", 60, GROUP_MART,
                 ["C360_Build_CustomerProfile", "C360_Build_LoyaltyOverlay", "C360_Publish_Segments"],
                 serialise=True)
    escalate = phase(pkg, "Customer Sync Escalation", 90, GROUP_MAINTENANCE,
                     ["ERR_Route_RejectedRows", "ERR_Notify_Operations"], serialise=True)

    count_candidates = pkg.add(ExecuteSql(
        "Count Merge Candidates",
        CONN_STAGING,
        "SELECT COUNT(*) AS Candidates FROM etl.RejectedRecord "
        "WHERE BatchId = ? AND ObjectName = N'work.CustomerDedup' "
        "AND RejectReasonCode = N'DUP_CANDIDATE';",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("User::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::DedupeCandidateCount")],
    ))
    close = pkg.add(end_batch())

    pkg.link(begin, extract)
    pkg.link(extract, bump, value="Failure")
    pkg.link(bump, retry, expression="@[User::ExtractAttempt] <= @[$Package::MaxExtractAttempts]")
    pkg.link(retry, stage)
    pkg.link(retry, escalate, value="Failure")
    pkg.link(extract, stage)
    pkg.link(stage, dedupe)
    pkg.link(dedupe, count_candidates, value="Completion")
    pkg.link(count_candidates, screen)
    pkg.link(count_candidates, escalate,
             expression="@[User::DedupeCandidateCount] > 500")
    pkg.link(screen, dimensions, value="Completion")
    pkg.link(dimensions, support)
    pkg.link(support, mart)
    pkg.link(mart, close)
    pkg.link(escalate, close)
    return pkg


# ---------------------------------------------------------------------------
# Master_Intraday_Inventory
# ---------------------------------------------------------------------------


def build_master_intraday_inventory():
    pkg = master_package(
        "Master_Intraday_Inventory",
        "Intraday inventory movement refresh. Runs every two hours during warehouse operating "
        "hours, keeps the movement fact and the on-hand position current, and stands the "
        "replenishment suggestion refresh down outside the picking window.",
        connections=(CONN_OLTP, CONN_DW),
        extra_parameters=[
            ("PickingWindowOpen", "True", "bool", False, False,
             "Replenishment only refreshes while the warehouse picking window is open."),
            ("OnHandVarianceTolerance", 25, "int", False, False,
             "Units of DW-to-OLTP on-hand variance tolerated before operations is paged."),
        ],
        extra_variables=[("OnHandVariance", 0, "int"), ("MovementRowCount", 0, "int")],
    )

    begin = pkg.add(start_batch("Intraday", "Intraday inventory refresh."))
    extract = phase(pkg, "Inventory Extract", 10, GROUP_EXTRACT,
                    ["EXT_SQL_StockItems", "EXT_SQL_StockMovements", "EXT_SQL_StockTransfers"],
                    streams=3)
    retry = phase(pkg, "Inventory Extract Retry", 11, GROUP_EXTRACT,
                  ["EXT_SQL_StockMovements"], serialise=True)
    bump = pkg.add(Expression("Increment Extract Attempt",
                              "@[User::ExtractAttempt] = @[User::ExtractAttempt] + 1"))
    stage = phase(pkg, "Inventory Stage", 20, GROUP_STAGE,
                  ["STG_Load_StockItem", "STG_Load_StockMovement", "STG_Work_InventoryPosition"],
                  serialise=True)
    facts = phase(pkg, "Inventory Facts", 30, GROUP_FACT,
                  ["FACT_Load_Movement", "FACT_Load_StockHolding"], serialise=True)
    marts = phase(pkg, "Inventory Marts", 40, GROUP_MART,
                  ["INV_Load_StockTransfer", "INV_Load_CycleCountVariance", "INV_Reconcile_OnHand"],
                  streams=2)
    replenish = phase(pkg, "Replenishment", 50, GROUP_MART, ["INV_Load_Replenishment"], serialise=True)
    health = phase(pkg, "Inventory Health", 60, GROUP_AGGREGATE,
                   ["AGG_Refresh_DailyInventoryHealth"], serialise=True)
    page = phase(pkg, "On Hand Variance Escalation", 90, GROUP_MAINTENANCE,
                 ["ERR_Reconcile_RowCounts", "ERR_Notify_Operations"], serialise=True)

    read_variance = pkg.add(ExecuteSql(
        "Read On Hand Variance",
        CONN_STAGING,
        "SELECT ISNULL(MAX(ABS(ra.SourceRowCount - ra.TargetRowCount)), 0) AS OnHandVariance "
        "FROM etl.RowCountAudit AS ra "
        "INNER JOIN etl.PackageExecution AS pe ON pe.PackageExecutionId = ra.PackageExecutionId "
        "WHERE pe.BatchId = ? AND ra.ObjectName = N'Fact.Stock Holding';",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("User::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::OnHandVariance")],
    ))
    close = pkg.add(end_batch())

    pkg.link(begin, extract)
    pkg.link(extract, bump, value="Failure")
    pkg.link(bump, retry, expression="@[User::ExtractAttempt] <= @[$Package::MaxExtractAttempts]")
    pkg.link(retry, stage)
    pkg.link(retry, page, value="Failure")
    pkg.link(extract, stage)
    pkg.link(stage, facts)
    pkg.link(facts, marts)
    pkg.link(marts, read_variance, value="Completion")
    pkg.link(read_variance, replenish,
             expression="@[$Package::PickingWindowOpen] && "
                        "@[User::OnHandVariance] <= @[$Package::OnHandVarianceTolerance]")
    pkg.link(read_variance, page,
             expression="@[User::OnHandVariance] > @[$Package::OnHandVarianceTolerance]")
    pkg.link(replenish, health)
    pkg.link(health, close)
    pkg.link(page, close)
    return pkg


# ---------------------------------------------------------------------------
# Master_File_Ingestion
# ---------------------------------------------------------------------------


def build_master_file_ingestion():
    pkg = master_package(
        "Master_File_Ingestion",
        "Partner and carrier file ingestion cycle. The three regional partner drops arrive on "
        "different schedules and in different formats, so each is ingested in its own stream; "
        "anything unparsable is quarantined rather than failing the cycle.",
        connections=(CONN_FILES, CONN_OLTP),
        extra_parameters=[
            ("RequirePartnerFiles", "False", "bool", False, False,
             "When set, a missing regional partner drop fails the cycle instead of warning."),
            ("QuarantineAfterAttempts", 2, "int", False, False,
             "Attempts before an unparsable file is moved to the quarantine share."),
        ],
        extra_variables=[("MissingFileCount", 0, "int"), ("QuarantinedFileCount", 0, "int")],
    )

    begin = pkg.add(start_batch("FileIngestion", "Partner and carrier file cycle."))
    inventory_files = pkg.add(ExecuteSql(
        "Count Expected Files Missing",
        CONN_STAGING,
        "SELECT COUNT(*) AS MissingFiles FROM etl.Configuration AS c "
        "WHERE c.ConfigurationKey LIKE N'Ingest.ExpectedFile.%' "
        "AND c.EnvironmentCode IN (?, N'ALL') "
        "AND NOT EXISTS (SELECT 1 FROM etl.PackageExecution AS pe "
        "                WHERE pe.PackageName = c.ConfigurationValue "
        "                  AND pe.StartedAtUtc >= DATEADD(HOUR, -24, SYSUTCDATETIME()) "
        "                  AND pe.Status = N'Succeeded');",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::EnvironmentCode", 0, "NVARCHAR")],
        result_bindings=[("0", "User::MissingFileCount")],
    ))
    pkg.link(begin, inventory_files)

    partner = phase(pkg, "Partner Drops", 10, GROUP_EXTRACT,
                    ["ING_FILE_PartnerSales_NA", "ING_FILE_PartnerSales_EU",
                     "ING_FILE_PartnerSales_APAC"], streams=3)
    carrier = phase(pkg, "Carrier And Catalog", 20, GROUP_EXTRACT,
                    ["ING_FILE_CarrierScan", "ING_FILE_SupplierCatalog", "ING_FILE_FxOverride"],
                    streams=2)
    quarantine = phase(pkg, "Quarantine", 30, GROUP_QUALITY,
                       ["ING_FILE_QuarantineMalformed", "ERR_Quarantine_BadFiles"], serialise=True)
    screen = phase(pkg, "File Screen", 40, GROUP_QUALITY, ["DQ_File_Screen"], serialise=True)
    stage = phase(pkg, "File Stage", 50, GROUP_STAGE,
                  ["STG_Load_PartnerSale", "STG_Load_Currency"], serialise=True)
    archive = phase(pkg, "Archive Files", 60, GROUP_MAINTENANCE,
                    ["MNT_Archive_ProcessedFiles"], serialise=True)
    notify = phase(pkg, "Missing File Notice", 90, GROUP_MAINTENANCE,
                   ["ERR_Notify_Operations"], serialise=True)

    close = pkg.add(end_batch())
    warn_close = pkg.add(end_batch("End Batch With Warnings", force_status="SucceededWithWarnings"))

    pkg.link(inventory_files, partner,
             expression="@[User::MissingFileCount] == 0 || !@[$Package::RequirePartnerFiles]")
    pkg.link(inventory_files, notify, expression="@[User::MissingFileCount] > 0")
    pkg.link(partner, carrier, value="Completion")
    pkg.link(partner, quarantine, value="Failure")
    pkg.link(carrier, quarantine, value="Completion")
    pkg.link(quarantine, screen, value="Completion")
    pkg.link(screen, stage)
    pkg.link(stage, archive)
    pkg.link(archive, close)
    pkg.link(notify, warn_close)
    return pkg


BUILDERS = [
    build_master_daily_etl,
    build_master_hourly_incremental,
    build_master_finance_close,
    build_master_weekly_reference_load,
    build_master_month_end,
    build_master_weekly_maintenance,
    build_master_customer_sync,
    build_master_intraday_inventory,
    build_master_file_ingestion,
]

CONNECTIONS = ["WWI_Oracle_ERP", "WWI_Source_DB", "WWI_Staging_DB", "WWI_DW_Destination_DB",
               "WWI_Inbound_Files", "WWI_Archive_Files", "WWI_Reject_Files"]


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
