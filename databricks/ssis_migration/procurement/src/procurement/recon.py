"""Reconciliation evidence for the 22 procurement packages -> otterorders_migration.evidence.recon_results.

Every package gets a row_count and a checksum check (SUM(xxhash64(<business cols cast to text>)),
order independent). Where the legacy SSIS output is populated on the host the comparison is
legacy vs. Delta (PASS/FAIL); where the legacy target is empty the expected result is derived
independently from the source data with the package's own filter (baseline=source_derived) and
the verdict is capped at PARTIAL."""

import json
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from procurement import io
from procurement.aggregates import GOLD_AGG_SUPPLIER_PERFORMANCE
from procurement.catalog_ingest import BRONZE_CATALOG, FEED_SUBDIR, REJECTED_FILE_ROW
from procurement.config import (
    EVIDENCE_ACTOR,
    EVIDENCE_BRANCH,
    EVIDENCE_SCHEMA,
    HARNESS_VERSION,
    LANDING_VOLUME,
    LEGACY_DW,
    LEGACY_OLTP,
    LEGACY_ORACLE,
    PACKAGES,
    PARAMS,
    UNIT_TYPE,
    qualified,
    volumePath,
)
from procurement.dimensions import GOLD_DIM_SUPPLIER, GOLD_DIM_VENDOR_CONTRACT
from procurement.extracts import (
    BRONZE_PO_HDR,
    BRONZE_PO_LINE,
    BRONZE_RECEIPT_LINE,
    BRONZE_SUPPLIER,
    BRONZE_SUPPLIER_TRANSACTION,
    BRONZE_VENDOR_CONTRACT,
    lowerColumns,
)
from procurement.facts import (
    GOLD_FACT_PURCHASE,
    GOLD_FACT_PURCHASE_P2P,
    GOLD_FACT_PURCHASE_RECEIPT,
    GOLD_FACT_SUPPLIER_TRANSACTION,
)
from procurement.marts import (
    GOLD_AGG_CONTRACT_COMPLIANCE,
    GOLD_AGG_SUPPLIER_SCORECARD,
    GOLD_FACT_PURCHASE_SPEND,
    GOLD_FACT_RECEIPT_MATCHING,
    GOLD_SUPPLIER_STATEMENT,
)
from procurement.quality import DQ_RESULT_TABLE
from procurement.staging import SILVER_PURCHASE_ORDER, SILVER_PURCHASE_ORDER_LINE, SILVER_SUPPLIER, SILVER_VENDOR_CONTRACT

RECON_TABLE = qualified("recon_results", EVIDENCE_SCHEMA)
LEGACY_DW_DB = "WideWorldImportersDW"
LEGACY_STG_DB = "WideWorldImporters_Staging"
LEGACY_OLTP_DB = "WideWorldImporters"

EVIDENCE_SCHEMA_STRUCT = T.StructType(
    [
        T.StructField("run_id", T.StringType()), T.StructField("run_at", T.TimestampType()), T.StructField("unit", T.StringType()),
        T.StructField("unit_type", T.StringType()), T.StructField("verdict", T.StringType()), T.StructField("branch", T.StringType()),
        T.StructField("source_object", T.StringType()), T.StructField("target_object", T.StringType()), T.StructField("checks", T.StringType()),
        T.StructField("summary", T.StringType()), T.StructField("git_sha", T.StringType()), T.StructField("actor", T.StringType()),
        T.StructField("harness_version", T.StringType()),
    ]
)


@dataclass
class ReconSpec:
    package: str
    sourceObject: str            # legacy object(s) the SSIS package writes
    targetTable: str             # Delta table (unqualified) in the procurement schema
    columns: Dict[str, str]      # business column -> cast type used on both sides
    expected: Callable           # spark -> DataFrame with `columns` (legacy output, or source-derived expectation)
    actual: Callable             # spark -> DataFrame with `columns`
    legacyBaseline: bool         # True when `expected` IS the legacy SSIS output (PASS possible)
    summary: str
    legacyCount: Optional[Callable] = None   # spark -> int, row count of the (possibly empty) legacy target
    extraChecks: Callable = field(default=lambda spark: [])


def normalize(df: DataFrame, columns: Dict[str, str]) -> DataFrame:
    return df.select(*[F.col(c).cast(t).alias(c) for c, t in columns.items()])


def checksum(df: DataFrame, columns: Dict[str, str]):
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in columns]
    row = df.agg(F.count(F.lit(1)).alias("n"), F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("h")).collect()[0]
    return int(row["n"]), (None if row["h"] is None else str(row["h"]))


def legacyCountSql(database, sql):
    return lambda spark: int(io.readLegacySql(spark, sql, database).collect()[0][0])


def evaluate(spark, spec: ReconSpec):
    columns = spec.columns
    expectedDf = normalize(spec.expected(spark), columns)
    actualDf = normalize(spec.actual(spark), columns)
    eCount, eHash = checksum(expectedDf, columns)
    aCount, aHash = checksum(actualDf, columns)
    checks = [
        {"check": "row_count", "source": eCount, "target": aCount, "pass": eCount == aCount},
        {"check": "checksum", "method": f"sum(cast(xxhash64({', '.join(columns)}) as decimal(38,0)))", "source": eHash, "target": aHash, "pass": eHash == aHash},
    ]
    if not spec.legacyBaseline:
        checks[0]["baseline"] = "source_derived"
        checks[1]["baseline"] = "source_derived"
        if spec.legacyCount is not None:
            try:
                legacyRows = spec.legacyCount(spark)
            except Exception as exc:  # legacy object unreadable -> record, do not fail the run
                legacyRows = f"unavailable: {type(exc).__name__}"
            checks.append({"check": "legacy_target_row_count", "source": legacyRows, "target": aCount, "pass": None,
                           "note": "legacy target not populated by the SSIS estate; informational"})
    checks.extend(spec.extraChecks(spark))
    allPass = all(c["pass"] for c in checks if c["pass"] is not None)
    if spec.legacyBaseline:
        verdict = "PASS" if allPass else "FAIL"
    else:
        verdict = "PARTIAL" if allPass else "FAIL"
    summary = spec.summary
    if verdict == "FAIL":
        failed = [c["check"] for c in checks if c["pass"] is False]
        summary = f"FAILED checks {failed}. " + summary
    return verdict, checks, summary


def writeEvidence(spark, rows, runId, gitSha):
    df = spark.createDataFrame(rows, EVIDENCE_SCHEMA_STRUCT)
    df.write.format("delta").mode("append").saveAsTable(RECON_TABLE)
    return df


def runRecon(spark, gitSha, runId=None, specs=None):
    runId = runId or io.newRunId()
    specs = specs or buildSpecs()
    assert sorted(s.package for s in specs) == sorted(PACKAGES), "recon specs must cover exactly the 22 procurement packages"
    rows = []
    for spec in specs:
        try:
            verdict, checks, summary = evaluate(spark, spec)
        except Exception as exc:
            verdict = "FAIL"
            checks = [{"check": "row_count", "source": None, "target": None, "pass": False, "error": f"{type(exc).__name__}: {str(exc)[:400]}"},
                      {"check": "checksum", "source": None, "target": None, "pass": False}]
            summary = f"recon raised {type(exc).__name__}; {spec.summary}"
        rows.append(
            (runId, io.nowUtc(), spec.package, UNIT_TYPE, verdict, EVIDENCE_BRANCH, spec.sourceObject, qualified(spec.targetTable),
             json.dumps(checks, default=str), summary, gitSha, EVIDENCE_ACTOR, HARNESS_VERSION)
        )
    writeEvidence(spark, rows, runId, gitSha)
    return runId, rows


# --------------------------------------------------------------------------------------
# helpers for source-derived expectations
# --------------------------------------------------------------------------------------
def oracle(spark, table):
    return lowerColumns(spark.table(f"{LEGACY_ORACLE}.{table}"))


def tbl(name):
    return lambda spark: spark.table(qualified(name))


def catalogFilesExpected(spark):
    """Independent re-parse of the landed files: DTL rows of Processed files minus rejected rows."""
    raw = spark.read.text(f"{volumePath(LANDING_VOLUME)}/{FEED_SUBDIR}/*.psv").select(
        "value", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1).alias("source_file_name")
    )
    parts = F.split(F.col("value"), "\\|")
    dtl = raw.where(parts[0] == "DTL").select("source_file_name", parts[1].alias("supplier_code"), parts[2].alias("supplier_item_code"), parts[8].alias("net_price"))
    processed = spark.table(qualified("ctl_landing_file")).where("file_status = 'Processed'").select("source_file_name").distinct()
    rejectTable = qualified(REJECTED_FILE_ROW)
    rejected = (
        spark.table(rejectTable).select("source_file_name", F.col("SupplierCode").alias("supplier_code"), F.col("SupplierItemCode").alias("supplier_item_code"))
        if io.tableExists(spark, rejectTable) else spark.createDataFrame([], "source_file_name string, supplier_code string, supplier_item_code string")
    )
    return dtl.join(processed, "source_file_name", "inner").join(rejected, ["source_file_name", "supplier_code", "supplier_item_code"], "left_anti")


def statementFileCheck(spark):
    import glob
    files = glob.glob(f"{volumePath('exports')}/supplier_statement/supplier_statement_*.csv")
    exists = len(files) > 0
    rows = 0
    if exists:
        with open(sorted(files)[-1], encoding="utf-8") as fh:
            rows = sum(1 for _ in fh) - 1
    tableRows = spark.table(qualified(GOLD_SUPPLIER_STATEMENT)).count()
    return [{"check": "export_file", "file": sorted(files)[-1] if exists else None, "source": tableRows, "target": rows, "pass": exists and rows == tableRows}]


def buildSpecs() -> List[ReconSpec]:
    src = "source_derived expectation: "
    return [
        ReconSpec(
            "EXT_ORA_SupplierMaster", "WideWorldImporters_Staging.raw.OracleSupplierMaster", BRONZE_SUPPLIER,
            {"supp_id": "long", "supp_nbr": "string", "supp_name": "string", "supp_status_cd": "string"},
            lambda spark: oracle(spark, "wwi_mdm.supp_master").where(
                "coalesce(deleted_flg,'N') = 'N' AND (supp_status_cd IN ('AC','ACTV') OR last_po_dt >= add_months(current_timestamp(), -%d))" % PARAMS["dormantSupplierMonths"]),
            tbl(BRONZE_SUPPLIER), False,
            src + "wwi_mdm.supp_master rows passing the package's dormant/deleted filter; legacy raw.OracleSupplierMaster is empty on the host.",
            legacyCountSql(LEGACY_STG_DB, "SELECT COUNT(*) FROM raw.OracleSupplierMaster"),
        ),
        ReconSpec(
            "EXT_ORA_PurchaseOrderHdr", "WideWorldImporters_Staging.raw.OraclePurchaseOrderHdr", BRONZE_PO_HDR,
            {"po_id": "long", "po_nbr": "string", "supp_id": "long", "po_status_cd": "string", "total_amt": "decimal(19,4)"},
            lambda spark: oracle(spark, "wwi_proc.purchase_order_hdr").where("po_status_cd IN ('OPEN','PART','CLSD','CANC') AND coalesce(approval_status_cd,'') <> 'DRFT'"),
            tbl(BRONZE_PO_HDR), False,
            src + "wwi_proc.purchase_order_hdr rows in the package's status/approval filter; legacy raw target empty.",
            legacyCountSql(LEGACY_STG_DB, "SELECT COUNT(*) FROM raw.OraclePurchaseOrderHdr"),
        ),
        ReconSpec(
            "EXT_ORA_PurchaseOrderLine", "WideWorldImporters_Staging.raw.OraclePurchaseOrderLine", BRONZE_PO_LINE,
            {"po_line_id": "long", "po_id": "long", "order_qty": "decimal(18,4)", "unit_price": "decimal(19,4)"},
            lambda spark: oracle(spark, "wwi_proc.purchase_order_line"), tbl(BRONZE_PO_LINE), False,
            src + "all wwi_proc.purchase_order_line rows (numeric watermark starts at 0 on first run); legacy raw target empty.",
            legacyCountSql(LEGACY_STG_DB, "SELECT COUNT(*) FROM raw.OraclePurchaseOrderLine"),
        ),
        ReconSpec(
            "EXT_ORA_ReceiptLine", "WideWorldImporters_Staging.raw.OracleReceiptLine", BRONZE_RECEIPT_LINE,
            {"receipt_line_id": "long", "receipt_id": "long", "po_line_id": "long", "received_qty": "decimal(18,4)"},
            lambda spark: oracle(spark, "wwi_proc.po_receipt_line").alias("l")
            .join(oracle(spark, "wwi_proc.po_receipt_hdr").where("receipt_status_cd <> 'VOID'").select("receipt_id"), "receipt_id", "inner")
            .join(oracle(spark, "wwi_proc.purchase_order_line").select("po_line_id"), "po_line_id", "inner"),
            tbl(BRONZE_RECEIPT_LINE), False,
            src + "po_receipt_line rows whose header is not VOID and whose PO line exists; legacy raw target empty.",
            legacyCountSql(LEGACY_STG_DB, "SELECT COUNT(*) FROM raw.OracleReceiptLine"),
        ),
        ReconSpec(
            "EXT_ORA_VendorContract", "WideWorldImporters_Staging.raw.OracleVendorContract", BRONZE_VENDOR_CONTRACT,
            {"contract_id": "long", "contract_nbr": "string", "supp_id": "long", "committed_amt": "decimal(19,4)"},
            lambda spark: oracle(spark, "wwi_proc.vendor_contract").where("contract_status_cd <> 'DELT'"), tbl(BRONZE_VENDOR_CONTRACT), False,
            src + "full reload of wwi_proc.vendor_contract excluding DELT; legacy raw target empty.",
            legacyCountSql(LEGACY_STG_DB, "SELECT COUNT(*) FROM raw.OracleVendorContract"),
        ),
        ReconSpec(
            "EXT_SQL_SupplierTransactions", "WideWorldImporters_Staging.raw.SqlInvoice", BRONZE_SUPPLIER_TRANSACTION,
            {"supplier_transaction_id": "long", "supplier_id": "long", "transaction_amount": "decimal(19,4)"},
            lambda spark: spark.table(f"{LEGACY_OLTP}.Purchasing.SupplierTransactions").select(
                F.col("SupplierTransactionID").alias("supplier_transaction_id"), F.col("SupplierID").alias("supplier_id"), F.col("TransactionAmount").alias("transaction_amount")),
            tbl(BRONZE_SUPPLIER_TRANSACTION), False,
            src + "all Purchasing.SupplierTransactions rows. The inventoried legacy target raw.SqlInvoice holds 2,820 Sales.Invoices rows written by a sibling package, so no legacy supplier-transaction output exists to compare against.",
            legacyCountSql(LEGACY_STG_DB, "SELECT COUNT(*) FROM raw.SqlInvoice"),
        ),
        ReconSpec(
            "ING_FILE_SupplierCatalog", "WideWorldImporters_Staging.raw.FileSupplierCatalog", BRONZE_CATALOG,
            {"source_file_name": "string", "supplier_code": "string", "supplier_item_code": "string", "net_price": "decimal(19,4)"},
            catalogFilesExpected, tbl(BRONZE_CATALOG), False,
            src + "independent re-parse of the DTL records of Processed sample files minus rejected rows; no supplier_catalog files exist on the host and raw.FileSupplierCatalog is empty.",
            legacyCountSql(LEGACY_STG_DB, "SELECT COUNT(*) FROM raw.FileSupplierCatalog"),
        ),
        ReconSpec(
            "STG_Load_Supplier", "WideWorldImporters_Staging.stg.Supplier", SILVER_SUPPLIER,
            {"supplier_business_key": "string"},
            lambda spark: spark.table(qualified(BRONZE_SUPPLIER)).select(F.upper(F.trim(F.col("supp_nbr"))).alias("supplier_business_key")).distinct(),
            lambda spark: spark.table(qualified(SILVER_SUPPLIER)).where("is_survivor_row"), False,
            src + "one survivor row per distinct supplier number in bronze; legacy stg.Supplier empty.",
            legacyCountSql(LEGACY_STG_DB, "SELECT COUNT(*) FROM stg.Supplier"),
        ),
        ReconSpec(
            "STG_Load_PurchaseOrder", "WideWorldImporters_Staging.stg.PurchaseOrder; WideWorldImporters_Staging.stg.PurchaseOrderLine", SILVER_PURCHASE_ORDER_LINE,
            {"purchase_order_line_business_key": "long", "purchase_order_business_key": "long", "order_quantity": "decimal(18,4)"},
            lambda spark: spark.table(qualified(BRONZE_PO_LINE)).where("order_qty > 0 AND coalesce(unit_price, 0) >= 0").select(
                F.col("po_line_id").alias("purchase_order_line_business_key"), F.col("po_id").alias("purchase_order_business_key"), F.col("order_qty").alias("order_quantity")),
            lambda spark: spark.table(qualified(SILVER_PURCHASE_ORDER_LINE)).where("dq_status_code <> 'FAIL'"), False,
            src + "bronze PO lines passing the positive-quantity / non-negative-price rule; legacy stg.PurchaseOrder(Line) empty.",
            legacyCountSql(LEGACY_STG_DB, "SELECT (SELECT COUNT(*) FROM stg.PurchaseOrder) + (SELECT COUNT(*) FROM stg.PurchaseOrderLine)"),
        ),
        ReconSpec(
            "STG_Load_VendorContract", "WideWorldImporters_Staging.stg.VendorContract", SILVER_VENDOR_CONTRACT,
            {"contract_business_key": "long", "contract_number": "string"},
            lambda spark: spark.table(qualified(BRONZE_VENDOR_CONTRACT)).select(F.col("contract_id").alias("contract_business_key"), F.upper(F.trim(F.col("contract_nbr"))).alias("contract_number"))
            .join(spark.table(qualified("err_rejected_row")).where("package_name = 'STG_Load_VendorContract'").select(F.col("business_key").alias("contract_number")), "contract_number", "left_anti"),
            tbl(SILVER_VENDOR_CONTRACT), False,
            src + "bronze contracts minus the rows rejected to err_rejected_row (err.RejectedLookup: inverted dates / unknown supplier / missing FX); legacy stg.VendorContract empty.",
            legacyCountSql(LEGACY_STG_DB, "SELECT COUNT(*) FROM stg.VendorContract"),
        ),
        ReconSpec(
            "DQ_Supplier_Screen", "WideWorldImporters_Staging.err.RejectedSupplier", DQ_RESULT_TABLE,
            {"supplier_business_key": "string"},
            lambda spark: spark.table(qualified(SILVER_SUPPLIER)).where("is_survivor_row").select("supplier_business_key")
            .join(spark.table(qualified("err_rejected_row")).where("package_name = 'DQ_Supplier_Screen'").select(F.col("business_key").alias("supplier_business_key")), "supplier_business_key", "left_anti"),
            lambda spark: spark.table(qualified(DQ_RESULT_TABLE)).select("supplier_business_key").distinct(), False,
            src + "screened suppliers = survivors minus rows rejected to err_rejected_row(err.RejectedSupplier); legacy err.RejectedSupplier empty.",
            legacyCountSql(LEGACY_STG_DB, "SELECT COUNT(*) FROM err.RejectedSupplier"),
        ),
        ReconSpec(
            "DIM_Load_Supplier", "WideWorldImportersDW.Dimension.Supplier", GOLD_DIM_SUPPLIER,
            {"supplier_business_key": "string", "supplier_name": "string"},
            lambda spark: spark.table(f"{LEGACY_DW}.Dimension.Supplier").select(
                F.concat(F.lit("WWI:"), F.col("`WWI Supplier ID`").cast("string")).alias("supplier_business_key"), F.col("Supplier").alias("supplier_name"))
            .unionByName(spark.table(qualified(SILVER_SUPPLIER)).where("is_survivor_row AND coalesce(dq_status_code,'PASS') <> 'FAIL'").select("supplier_business_key", "supplier_name")),
            lambda spark: spark.table(qualified(GOLD_DIM_SUPPLIER)).where("is_current_row"), False,
            "current dimension rows = 30 legacy Dimension.Supplier rows (seeded, keys preserved) + one current version per DQ-passing Oracle supplier. Legacy stg.Supplier is empty so the SSIS run never versioned the dimension; PARTIAL by contract.",
            legacyCountSql(LEGACY_DW_DB, "SELECT COUNT(*) FROM Dimension.Supplier"),
        ),
        ReconSpec(
            "DIM_Load_VendorContract", "WideWorldImportersDW.Dimension.Vendor Contract", GOLD_DIM_VENDOR_CONTRACT,
            {"contract_number": "string", "supplier_business_key": "string", "committed_amount": "decimal(19,4)"},
            lambda spark: spark.table(qualified(SILVER_VENDOR_CONTRACT)).select("contract_number", "supplier_business_key", "committed_amount").dropDuplicates(["contract_number"]),
            lambda spark: spark.table(qualified(GOLD_DIM_VENDOR_CONTRACT)).where("is_current_row"), False,
            src + "one current version per staged contract; legacy Dimension.[Vendor Contract] is empty (0 rows).",
            legacyCountSql(LEGACY_DW_DB, "SELECT COUNT(*) FROM Dimension.[Vendor Contract]"),
        ),
        ReconSpec(
            "FACT_Load_Purchase", "WideWorldImportersDW.Fact.Purchase", GOLD_FACT_PURCHASE,
            {"date_key": "date", "supplier_key": "int", "stock_item_key": "int", "wwi_purchase_order_id": "long", "ordered_outers": "int",
             "ordered_quantity": "int", "received_outers": "int", "package": "string", "is_order_finalized": "int"},
            lambda spark: spark.table(f"{LEGACY_DW}.Fact.Purchase").select(
                F.col("`Date Key`").alias("date_key"), F.col("`Supplier Key`").alias("supplier_key"), F.col("`Stock Item Key`").alias("stock_item_key"),
                F.col("`WWI Purchase Order ID`").alias("wwi_purchase_order_id"), F.col("`Ordered Outers`").alias("ordered_outers"), F.col("`Ordered Quantity`").alias("ordered_quantity"),
                F.col("`Received Outers`").alias("received_outers"), F.col("Package").alias("package"), F.col("`Is Order Finalized`").cast("int").alias("is_order_finalized")),
            lambda spark: spark.table(qualified(GOLD_FACT_PURCHASE)).withColumn("is_order_finalized", F.col("is_order_finalized").cast("int")), True,
            "legacy Fact.Purchase (8,367 rows) vs gold_fact_purchase rebuilt from WWI OLTP purchase order lines with legacy supplier/stock-item key resolution; business columns compared, surrogate/lineage keys excluded. Oracle-lineage rows land separately in gold_fact_purchase_p2p.",
        ),
        ReconSpec(
            "FACT_Load_PurchaseReceipt", "WideWorldImportersDW.Fact.Purchase Receipt", GOLD_FACT_PURCHASE_RECEIPT,
            {"receipt_line_id": "long", "quantity_source_uom": "decimal(18,4)", "unit_cost": "decimal(19,4)"},
            lambda spark: spark.table(qualified(BRONZE_RECEIPT_LINE)).select("receipt_line_id", F.col("received_qty").alias("quantity_source_uom"), "unit_cost"),
            tbl(GOLD_FACT_PURCHASE_RECEIPT), False,
            src + "one fact row per bronze receipt line (unknown-supplier rows kept with inferred_member_flag); legacy Fact.[Purchase Receipt] empty.",
            legacyCountSql(LEGACY_DW_DB, "SELECT COUNT(*) FROM Fact.[Purchase Receipt]"),
        ),
        ReconSpec(
            "FACT_Load_SupplierTransaction", "WideWorldImportersDW.Fact.Supplier Transaction", GOLD_FACT_SUPPLIER_TRANSACTION,
            {"supplier_transaction_business_key": "long", "transaction_amount": "decimal(19,4)"},
            lambda spark: spark.table(f"{LEGACY_OLTP}.Purchasing.SupplierTransactions").join(
                spark.table(f"{LEGACY_OLTP}.Application.TransactionTypes").select("TransactionTypeID", "TransactionTypeName"), "TransactionTypeID").select(
                F.col("SupplierTransactionID").alias("supplier_transaction_business_key"),
                F.when(F.col("TransactionTypeName") == "Supplier Payment Issued", -F.abs(F.col("TransactionAmount"))).otherwise(F.col("TransactionAmount")).alias("transaction_amount")),
            lambda spark: spark.table(qualified(GOLD_FACT_SUPPLIER_TRANSACTION)).where("NOT is_reversal"), False,
            src + "signed amount per Purchasing.SupplierTransactions row (payments negative); legacy Fact.[Supplier Transaction] empty.",
            legacyCountSql(LEGACY_DW_DB, "SELECT COUNT(*) FROM Fact.[Supplier Transaction]"),
        ),
        ReconSpec(
            "PRC_Load_PurchaseSpend", "WideWorldImportersDW.Fact.Purchase (spend extension)", GOLD_FACT_PURCHASE_SPEND,
            {"purchase_order_line_business_key": "long", "spend_amount": "decimal(19,4)"},
            lambda spark: spark.table(qualified(SILVER_PURCHASE_ORDER_LINE)).join(
                spark.table(qualified(SILVER_PURCHASE_ORDER)).where("order_status_code NOT IN ('CANC','DRAFT')").select("purchase_order_business_key"), "purchase_order_business_key")
            .select("purchase_order_line_business_key", F.col("extended_amount").alias("spend_amount")),
            tbl(GOLD_FACT_PURCHASE_SPEND), False,
            src + "one spend row per staged PO line on a non-cancelled/non-draft order; the legacy host carries no spend-classified rows.",
            legacyCountSql(LEGACY_DW_DB, "SELECT COUNT(*) FROM Fact.Purchase WHERE [Lineage Key] <> 10"),
        ),
        ReconSpec(
            "PRC_Load_ReceiptMatching", "WideWorldImportersDW.Fact.Purchase Receipt (match extension)", GOLD_FACT_RECEIPT_MATCHING,
            {"receipt_line_id": "long", "receipt_value": "decimal(19,4)"},
            lambda spark: spark.table(qualified(GOLD_FACT_PURCHASE_RECEIPT)).select("receipt_line_id", "receipt_value"), tbl(GOLD_FACT_RECEIPT_MATCHING), False,
            src + "one match row per receipt fact row; Oracle ap_invoice_line has no po_line_id/receipt_line_id populated so every receipt evaluates to GRNI. Legacy Fact.[Purchase Receipt] empty.",
            legacyCountSql(LEGACY_DW_DB, "SELECT COUNT(*) FROM Fact.[Purchase Receipt]"),
        ),
        ReconSpec(
            "PRC_Load_ContractCompliance", "WideWorldImportersDW.Aggregate.Supplier Performance", GOLD_AGG_CONTRACT_COMPLIANCE,
            {"source_supplier_id": "long", "calendar_month": "string", "region_code": "string"},
            lambda spark: spark.table(qualified(GOLD_FACT_PURCHASE_SPEND)).where(f"spend_date >= date_sub(current_date(), {PARAMS['complianceWindowDays']})")
            .select("source_supplier_id", "calendar_month", "region_code").distinct(),
            tbl(GOLD_AGG_CONTRACT_COMPLIANCE), False,
            src + "one compliance row per (supplier, month, region) with spend inside the compliance window; legacy Aggregate.[Supplier Performance] empty.",
            legacyCountSql(LEGACY_DW_DB, "SELECT COUNT(*) FROM Aggregate.[Supplier Performance]"),
        ),
        ReconSpec(
            "PRC_Load_SupplierScorecard", "WideWorldImportersDW.Aggregate.Supplier Performance", GOLD_AGG_SUPPLIER_SCORECARD,
            {"source_supplier_id": "long", "supplier_business_key": "string"},
            lambda spark: spark.table(qualified(SILVER_SUPPLIER)).where("is_survivor_row").select("source_supplier_id", "supplier_business_key"),
            tbl(GOLD_AGG_SUPPLIER_SCORECARD), False,
            src + "one scorecard row per staged supplier (INSUFFICIENT_DATA below the minimum order count); etl.SupplierScoringWeight is empty on the host so package default weights apply. Legacy aggregate empty.",
            legacyCountSql(LEGACY_DW_DB, "SELECT COUNT(*) FROM Aggregate.[Supplier Performance]"),
        ),
        ReconSpec(
            "PRC_Export_SupplierStatement", "WideWorldImportersDW.Fact.Supplier Transaction -> file:supplier_statement.csv", GOLD_SUPPLIER_STATEMENT,
            {"transaction_reference": "long", "transaction_amount": "decimal(19,4)"},
            lambda spark: spark.table(qualified(GOLD_FACT_SUPPLIER_TRANSACTION)).alias("f").join(
                spark.table(qualified(GOLD_SUPPLIER_STATEMENT)).select("statement_period").distinct(),
                F.date_format(F.col("transaction_date_key"), "yyyy-MM") == F.col("statement_period")).where("NOT is_reversal")
            .select(F.col("supplier_transaction_business_key").alias("transaction_reference"), "transaction_amount"),
            tbl(GOLD_SUPPLIER_STATEMENT), False,
            src + "one statement line per non-reversal supplier transaction in the statement period, exported to /Volumes/.../exports/supplier_statement; legacy Fact.[Supplier Transaction] is empty and the host has no statement file.",
            legacyCountSql(LEGACY_DW_DB, "SELECT COUNT(*) FROM Fact.[Supplier Transaction]"),
            extraChecks=statementFileCheck,
        ),
        ReconSpec(
            "AGG_Refresh_SupplierPerformance", "WideWorldImportersDW.Aggregate.Supplier Performance", GOLD_AGG_SUPPLIER_PERFORMANCE,
            {"calendar_month": "string", "supplier_key": "int", "region_code": "string"},
            lambda spark: spark.table(qualified(GOLD_FACT_PURCHASE_P2P)).select(F.date_format("date_key", "yyyy-MM").alias("calendar_month"), "supplier_key", "region_code")
            .union(spark.table(qualified(GOLD_FACT_PURCHASE_RECEIPT)).select(F.date_format("receipt_date_key", "yyyy-MM").alias("calendar_month"), "supplier_key", "region_code"))
            .union(spark.table(qualified(GOLD_FACT_PURCHASE_SPEND)).join(spark.table(qualified(GOLD_DIM_SUPPLIER)).where("is_current_row").select("supplier_business_key", "supplier_key"), "supplier_business_key", "left")
                   .select("calendar_month", F.coalesce(F.col("supplier_key"), F.lit(0)).alias("supplier_key"), "region_code")).distinct(),
            tbl(GOLD_AGG_SUPPLIER_PERFORMANCE), False,
            src + "one aggregate row per (month, supplier_key, region) present in the purchase, receipt or spend facts; legacy Aggregate.[Supplier Performance] empty.",
            legacyCountSql(LEGACY_DW_DB, "SELECT COUNT(*) FROM Aggregate.[Supplier Performance]"),
        ),
    ]


def summarize(rows):
    counts = {}
    for r in rows:
        counts[r[4]] = counts.get(r[4], 0) + 1
    return counts
