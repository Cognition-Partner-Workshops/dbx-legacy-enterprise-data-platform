"""FACT_Apply_Corrections: month-end correction rows applied to the gold facts with an audit trail.

Fact.Sale corrections are reversal + replacement rows (the original row is preserved and
flagged as reversed); Fact.Order and Fact.Payment are restated in place. Queue rows that target
anything else are rejected. Every application is recorded in gold_fact_correction_audit.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_performance import config
from sales_performance.commissions import writeMetrics
from sales_performance.common import mergeInto, readStaging, readTable, readTableOrEmpty, saveTable, snakeCaseColumns, withAudit
from sales_performance.sale_line import FACT_ORDER_TABLE, FACT_PAYMENT_TABLE, FACT_SALE_TABLE

PACKAGE = "FACT_Apply_Corrections"
QUEUE_TABLE = "work_fact_rekey_queue"
AUDIT_TABLE = "gold_fact_correction_audit"
REJECT_TABLE = "gold_fact_correction_reject"
SUPPORTED_TARGETS = ("Fact.Sale", "Fact.Order", "Fact.Payment")
MONEY = "decimal(19,4)"

QUEUE_SCHEMA = """
    queue_row_id bigint, batch_id bigint, fact_object_name string, fact_business_key string, dimension_name string,
    current_surrogate_key bigint, corrected_surrogate_key bigint, corrected_amount decimal(19,4), correction_type_code string,
    rekey_reason_code string, effective_date date, rekey_priority int, applied_flag boolean, applied_at_utc timestamp,
    attempt_count int, last_error_text string, created_at_utc timestamp
"""


def normalizeQueue(legacyQueue: DataFrame) -> DataFrame:
    """Map work.FactRekeyQueue (legacy DDL) onto the queue schema this package consumes."""
    q = snakeCaseColumns(legacyQueue)
    for col, dtype in (("corrected_amount", MONEY), ("correction_type_code", "string")):
        if col not in q.columns:
            q = q.withColumn(col, F.lit(None).cast(dtype))
    return q.select(
        F.col("queue_row_id").cast("bigint"),
        F.col("batch_id").cast("bigint"),
        "fact_object_name",
        "fact_business_key",
        "dimension_name",
        F.col("current_surrogate_key").cast("bigint"),
        F.col("corrected_surrogate_key").cast("bigint"),
        F.col("corrected_amount").cast(MONEY),
        "correction_type_code",
        "rekey_reason_code",
        F.col("effective_date").cast("date"),
        F.col("rekey_priority").cast("int"),
        F.col("applied_flag").cast("boolean"),
        F.col("applied_at_utc").cast("timestamp"),
        F.col("attempt_count").cast("int"),
        "last_error_text",
        F.col("created_at_utc").cast("timestamp"),
    )


def classifyQueue(queue: DataFrame, maxCorrections: int = 50000):
    pending = (
        queue.filter(~F.coalesce(F.col("applied_flag"), F.lit(False)))
        .orderBy("rekey_priority", "created_at_utc", "queue_row_id")
        .limit(maxCorrections)
    )
    pending = pending.withColumn(
        "correction_style_code", F.when(F.col("fact_object_name") == "Fact.Sale", "REVERSAL").otherwise("RESTATEMENT")
    ).withColumn(
        "signed_amount",
        F.when(F.col("correction_type_code") == "DECREASE", F.col("corrected_amount") * -1).otherwise(F.col("corrected_amount")),
    )
    supported = pending.filter(F.col("fact_object_name").isin(*SUPPORTED_TARGETS))
    rejected = pending.filter(~F.col("fact_object_name").isin(*SUPPORTED_TARGETS) | F.col("fact_object_name").isNull()).withColumn(
        "reject_reason_code", F.lit("UNSUPPORTED_TARGET")
    )
    return supported, rejected


def _dimensionKeyColumn(dimensionName):
    return (
        F.when(F.lower(dimensionName).contains("customer"), "customer_key")
        .when(F.lower(dimensionName).contains("stock"), "stock_item_key")
        .when(F.lower(dimensionName).contains("salesperson") | F.lower(dimensionName).contains("employee"), "salesperson_key")
        .when(F.lower(dimensionName).contains("city"), "city_key")
        .otherwise(F.lit(None))
    )


def applySaleCorrections(factSale: DataFrame, saleQueue: DataFrame, lineageKey: int):
    """Return (reversalAndReplacementRows, reversedOriginalKeys, audit) for Fact.Sale corrections.

    Natural key: ``invoice_number|invoice_line_number``. The reversal negates the measures of the
    original; the replacement carries the corrected dimension key / amount."""
    keyed = saleQueue.withColumn("target_column", _dimensionKeyColumn(F.col("dimension_name")))
    keyed = keyed.withColumn("split_key", F.split(F.col("fact_business_key"), "\\|"))
    keyed = keyed.withColumn("q_invoice_number", F.col("split_key").getItem(0).cast("bigint")).withColumn(
        "q_invoice_line_number", F.col("split_key").getItem(1).cast("int")
    )
    originals = factSale.filter(~F.col("is_reversal")).join(
        keyed.select(
            "queue_row_id",
            "q_invoice_number",
            "q_invoice_line_number",
            "target_column",
            "corrected_surrogate_key",
            "signed_amount",
            "rekey_reason_code",
        ),
        (F.col("invoice_number") == F.col("q_invoice_number")) & (F.col("invoice_line_number") == F.col("q_invoice_line_number")),
        "inner",
    )
    maxKey = factSale.agg(F.coalesce(F.max("sale_key"), F.lit(0))).collect()[0][0]
    negate = lambda c: (F.col(c) * -1).cast(MONEY)  # noqa: E731
    measures = ["extended_price", "tax_amount", "total_including_tax", "profit", "cost_amount", "net_amount"]
    base = originals.withColumn("_ord", F.row_number().over(Window.orderBy("queue_row_id")))
    reversal = base
    for m in measures:
        reversal = reversal.withColumn(m, negate(m))
    reversal = (
        reversal.withColumn("quantity", (F.col("quantity") * -1).cast("decimal(18,3)"))
        .withColumn("reverses_sale_key", F.col("sale_key"))
        .withColumn("sale_key", F.lit(maxKey) + F.col("_ord") * 2 - 1)
        .withColumn("is_reversal", F.lit(True))
        .withColumn("is_correction", F.lit(True))
    )
    replacement = (
        base.withColumn("reverses_sale_key", F.col("sale_key"))
        .withColumn("sale_key", F.lit(maxKey) + F.col("_ord") * 2)
        .withColumn("is_reversal", F.lit(False))
        .withColumn("is_correction", F.lit(True))
    )
    for keyCol in ("customer_key", "stock_item_key", "salesperson_key", "city_key"):
        replacement = replacement.withColumn(
            keyCol,
            F.when(
                (F.col("target_column") == keyCol) & F.col("corrected_surrogate_key").isNotNull(), F.col("corrected_surrogate_key")
            ).otherwise(F.col(keyCol)),
        )
    replacement = (
        replacement.withColumn("extended_price", F.coalesce(F.col("signed_amount"), F.col("extended_price")).cast(MONEY))
        .withColumn("net_amount", F.coalesce(F.col("signed_amount"), F.col("net_amount")).cast(MONEY))
        .withColumn("total_including_tax", (F.col("extended_price") + F.coalesce(F.col("tax_amount"), F.lit(0))).cast(MONEY))
    )
    newRows = (
        reversal.unionByName(replacement)
        .withColumn("correction_lineage_key", F.lit(lineageKey).cast("bigint"))
        .drop(
            "_ord",
            "queue_row_id",
            "q_invoice_number",
            "q_invoice_line_number",
            "target_column",
            "corrected_surrogate_key",
            "signed_amount",
            "rekey_reason_code",
        )
    )
    audit = originals.select(
        "queue_row_id",
        F.lit("Fact.Sale").alias("fact_object_name"),
        F.concat_ws("|", "invoice_number", "invoice_line_number").alias("fact_business_key"),
        F.lit("REVERSAL").alias("correction_style_code"),
        F.col("sale_key").alias("original_fact_key"),
        "rekey_reason_code",
        F.to_json(F.struct("customer_key", "stock_item_key", "salesperson_key", "extended_price")).alias("before_state"),
        F.to_json(F.struct("target_column", "corrected_surrogate_key", "signed_amount")).alias("after_state"),
    )
    return newRows, originals.select("sale_key").distinct(), audit


def restateInPlace(fact: DataFrame, queue: DataFrame, keyColumns, amountColumn: str, factName: str):
    """In-place restatement for Fact.Order / Fact.Payment. Natural key columns come from the caller."""
    keyed = queue.withColumn("split_key", F.split(F.col("fact_business_key"), "\\|")).withColumn(
        "target_column", _dimensionKeyColumn(F.col("dimension_name"))
    )
    for i, col in enumerate(keyColumns):
        keyed = keyed.withColumn(f"q_{col}", F.col("split_key").getItem(i).cast(fact.schema[col].dataType))
    cond = None
    for col in keyColumns:
        c = fact[col] == keyed[f"q_{col}"]
        cond = c if cond is None else (cond & c)
    matched = fact.join(
        keyed.select(
            "queue_row_id",
            "target_column",
            "corrected_surrogate_key",
            "signed_amount",
            "rekey_reason_code",
            *[f"q_{c}" for c in keyColumns],
        ),
        cond,
        "inner",
    )
    restated = matched
    if amountColumn in fact.columns:
        restated = restated.withColumn(amountColumn, F.coalesce(F.col("signed_amount"), F.col(amountColumn)).cast(MONEY))
    for keyCol in ("customer_key", "stock_item_key", "salesperson_key", "city_key"):
        if keyCol in fact.columns:
            restated = restated.withColumn(
                keyCol,
                F.when(
                    (F.col("target_column") == keyCol) & F.col("corrected_surrogate_key").isNotNull(), F.col("corrected_surrogate_key")
                ).otherwise(F.col(keyCol)),
            )
    restated = restated.withColumn("is_restated", F.lit(True))
    audit = matched.select(
        "queue_row_id",
        F.lit(factName).alias("fact_object_name"),
        F.concat_ws("|", *keyColumns).alias("fact_business_key"),
        F.lit("RESTATEMENT").alias("correction_style_code"),
        F.col(keyColumns[0]).cast("bigint").alias("original_fact_key"),
        "rekey_reason_code",
        F.to_json(F.struct(*[c for c in ("customer_key", amountColumn) if c in fact.columns])).alias("before_state"),
        F.to_json(F.struct("target_column", "corrected_surrogate_key", "signed_amount")).alias("after_state"),
    )
    return restated.drop(
        "queue_row_id",
        "target_column",
        "corrected_surrogate_key",
        "signed_amount",
        "rekey_reason_code",
        "split_key",
        *[f"q_{c}" for c in keyColumns],
    ), audit


def loadQueue(spark: SparkSession) -> DataFrame:
    legacy = normalizeQueue(readStaging(spark, "work", "FactRekeyQueue"))
    local = readTableOrEmpty(spark, QUEUE_TABLE, QUEUE_SCHEMA)
    return legacy.unionByName(local.select(*legacy.columns), allowMissingColumns=True)


def runCorrections(spark: SparkSession, batchId: int, correctionPeriodCode: str = None, maxCorrections: int = 50000):
    queue = loadQueue(spark)
    supported, rejected = classifyQueue(queue, maxCorrections)
    supported = supported
    sourceCount = queue.filter(~F.coalesce(F.col("applied_flag"), F.lit(False))).count()
    fact = readTable(spark, FACT_SALE_TABLE)
    if "is_correction" not in fact.columns:
        fact = fact.withColumn("is_correction", F.lit(False))
    if "correction_lineage_key" not in fact.columns:
        fact = fact.withColumn("correction_lineage_key", F.lit(None).cast("bigint"))
    saleQueue = supported.filter(F.col("fact_object_name") == "Fact.Sale")
    newRows, reversedKeys, audit = applySaleCorrections(fact, saleQueue, batchId)
    # Materialise everything derived from the fact table before appending to it: Delta reads are
    # lazy, so the audit/reversed keys would otherwise see the rows being inserted.
    reversedList = [r["sale_key"] for r in reversedKeys.collect()]
    audit = spark.createDataFrame(audit.collect(), audit.schema)
    newRows = spark.createDataFrame(newRows.select(*fact.columns).collect(), fact.schema)
    insertCount = newRows.count()
    if insertCount:
        saveTable(newRows, FACT_SALE_TABLE, mode="append")
        spark.sql(
            "UPDATE {t} SET is_correction = true WHERE sale_key IN ({keys})".format(
                t=config.tableName(FACT_SALE_TABLE), keys=",".join(str(k) for k in reversedList)
            )
        )
    updateCount = 0
    audits = [audit]
    for factName, table, keys, amountColumn in (
        ("Fact.Order", FACT_ORDER_TABLE, ["order_number", "order_line_number"], "extended_price"),
        ("Fact.Payment", FACT_PAYMENT_TABLE, ["receipt_number", "receipt_line_number"], "payment_amount"),
    ):
        subset = supported.filter(F.col("fact_object_name") == factName)
        if subset.limit(1).count() == 0 or not spark.catalog.tableExists(config.tableName(table)):
            continue
        factDf = readTable(spark, table)
        if "is_restated" not in factDf.columns:
            factDf = factDf.withColumn("is_restated", F.lit(False))
            saveTable(factDf, table)
        restated, restateAudit = restateInPlace(factDf, subset, keys, amountColumn, factName)
        n = restated.count()
        if n:
            mergeInto(spark, restated, table, keys, insertAll=False)
        updateCount += n
        audits.append(restateAudit)
    auditAll = audits[0]
    for a in audits[1:]:
        auditAll = auditAll.unionByName(a)
    auditAll = withAudit(auditAll.withColumn("accounting_period_code", F.lit(correctionPeriodCode)), PACKAGE, batchId)
    saveTable(auditAll, AUDIT_TABLE, mode="append")
    rejectCount = rejected.count()
    if rejectCount:
        saveTable(withAudit(rejected, PACKAGE, batchId), REJECT_TABLE, mode="append")
    appliedIds = [r["queue_row_id"] for r in supported.select("queue_row_id").collect()]
    local = readTableOrEmpty(spark, QUEUE_TABLE, QUEUE_SCHEMA)
    if appliedIds and local.limit(1).count() > 0:
        closed = local.withColumn(
            "applied_flag", F.when(F.col("queue_row_id").isin(appliedIds), F.lit(True)).otherwise(F.col("applied_flag"))
        ).withColumn(
            "applied_at_utc", F.when(F.col("queue_row_id").isin(appliedIds), F.current_timestamp()).otherwise(F.col("applied_at_utc"))
        )
        saveTable(closed, QUEUE_TABLE)
    metrics = {
        "source_row_count": sourceCount,
        "insert_row_count": insertCount,
        "update_row_count": updateCount,
        "reject_row_count": rejectCount,
    }
    writeMetrics(spark, PACKAGE, metrics, batchId)
    return metrics
