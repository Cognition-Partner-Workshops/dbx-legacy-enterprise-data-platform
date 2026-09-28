"""ERR_Route_RejectedRows: reject file naming and per-object err.* table contract."""

# Legacy err.* reject tables and the column used as the steward-facing business key.
ERR_TABLES = {
    "silver.err_rejected_customer": "CustomerBusinessKey",
    "silver.err_rejected_supplier": "SupplierBusinessKey",
    "silver.err_rejected_product": "ProductBusinessKey",
    "silver.err_rejected_order_line": "OrderLineBusinessKey",
    "silver.err_rejected_invoice_line": "InvoiceLineBusinessKey",
    "silver.err_rejected_payment": "PaymentBusinessKey",
    "silver.err_rejected_shipment": "ShipmentBusinessKey",
    "silver.err_rejected_file_row": "SourceFileName",
    "silver.err_rejected_lookup_failure": "SourceBusinessKey",
    "silver.err_rejected_constraint_violation": "ViolatingBusinessKey",
}

REPROCESS = "REPROCESS"
QUARANTINE = "QUARANTINE"


def rejectFileName(batchId, asOfDate):
    """Legacy: "rejects_" + BatchId + "_" + yyyyMMdd + ".csv"."""
    return "rejects_%s_%s.csv" % (int(batchId), asOfDate.strftime("%Y%m%d"))


def routingDestination(isEscalated):
    """Escalated (aged) rejects go to quarantine; standard ones stay on the reprocess queue."""
    return QUARANTINE if isEscalated else REPROCESS


def classifyRejects(routingSet, rejectEscalationDays, fileName):
    """Legacy Classify Reject Age derived columns + Split Escalations destination."""
    from pyspark.sql import functions as F

    return (
        routingSet
        .withColumn("IsEscalated", F.col("AgeDays") > F.lit(int(rejectEscalationDays)))
        .withColumn("RejectFileLine", F.concat_ws("|", F.col("ObjectName"), F.col("RejectReasonCode"), F.coalesce(F.col("SourceKey"), F.lit(""))))
        .withColumn("RoutingDestination", F.when(F.col("IsEscalated"), F.lit(QUARANTINE)).otherwise(F.lit(REPROCESS)))
        .withColumn("RejectFileName", F.lit(fileName))
        .withColumn("RoutedAtUtc", F.current_timestamp())
    )
