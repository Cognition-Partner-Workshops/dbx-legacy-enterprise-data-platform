from datetime import date, datetime

import pytest

from wwi_dimensions import rekey, scd, specs, tables, unknown

NOW = datetime(2026, 2, 1, 4, 0, 0)
BUSINESS_DATE = date(2026, 2, 1)


def _queue(spark, catalog, rows):
    table = tables.ensureLateArrivingQueue(spark, catalog)
    df = spark.createDataFrame(
        rows,
        "QueueRowId BIGINT, BatchId BIGINT, PackageExecutionId BIGINT, DimensionName STRING, MissingBusinessKey STRING, "
        "SourceSystemCode STRING, FirstSeenObjectName STRING, FirstSeenAtUtc TIMESTAMP, OccurrenceCount INT, InferredAttributesJson STRING, "
        "StubCreatedFlag BOOLEAN, StubCreatedAtUtc TIMESTAMP, PlaceholderKey INT, RetryCount INT, ResolvedFlag BOOLEAN, ResolvedAtUtc TIMESTAMP, "
        "ResolvedByExecutionId BIGINT, ResolutionNote STRING",
    )
    df.write.format("delta").mode("append").saveAsTable(table)
    return table


def test_classification_regional_limits(spark):
    df = spark.createDataFrame(
        [(1, "Customer", datetime(2026, 1, 1), 0, "SQLSTG_NA", None), (2, "Customer", datetime(2026, 1, 30), 3, "SQLSTG_NA", None),
         (3, "Customer", datetime(2026, 1, 30), 3, "SQLSTG_EU", None), (4, "City", datetime(2026, 1, 30), 5, None, None),
         (5, "Stock Item", datetime(2026, 1, 30), 20, None, '{"RegionCode":"APAC"}')],
        "QueueRowId BIGINT, DimensionName STRING, FirstSeenAtUtc TIMESTAMP, RetryCount INT, SourceSystemCode STRING, InferredAttributesJson STRING",
    )
    out = {r["QueueRowId"]: r for r in rekey.classifyQueue(df, BUSINESS_DATE, 30).collect()}
    assert out[1]["EscalationCode"] == "ESCALATE" and out[1]["QueueAgeDays"] == 31
    assert out[2]["EscalationCode"] == "MANUAL" and out[2]["RetryLimit"] == 3      # NA limit 3
    assert out[3]["EscalationCode"] == "RETRY" and out[3]["RetryLimit"] == 7       # EU limit 7
    assert out[4]["EscalationCode"] == "MANUAL" and out[4]["RetryLimit"] == 5      # default 5
    assert out[5]["EscalationCode"] == "RETRY" and out[5]["RetryLimit"] == 21      # APAC limit 21


@pytest.mark.usefixtures("cleanGold")
def test_run_rekey_assign_repoint_close(spark, catalog):
    # Dimension with one real customer; fact pointing at Unknown for CUST-2 (not yet arrived) and CUST-1 (arrived late)
    tables.ensureDimensionTable(spark, catalog, specs.CUSTOMER)
    unknown.ensureUnknownMembers(spark, catalog, specs.CUSTOMER)
    scd.applyScd(spark, catalog, specs.CUSTOMER,
                 spark.createDataFrame([("CUST-1", "Arrived", "NA")], "CustomerBusinessKey STRING, Customer STRING, RegionCode STRING"),
                 1, 10, loadTimestamp=NOW)
    spark.createDataFrame([(1, "CUST-1", -1, 5.0), (2, "CUST-2", -1, 7.0), (3, "CUST-3", -1, 1.0)],
                          "SaleKey INT, CustomerBusinessKey STRING, CustomerKey INT, Amount DOUBLE").write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.fact_sale")
    queue = _queue(spark, catalog, [
        (1, 1, 10, "Customer", "CUST-1", "SQLSTG_NA", "Fact.Sale", datetime(2026, 1, 25), 1, None, False, None, None, 0, False, None, None, None),
        (2, 1, 10, "Customer", "CUST-2", "SQLSTG_NA", "Fact.Sale", datetime(2026, 1, 25), 1, None, False, None, None, 0, False, None, None, None),
        (3, 1, 10, "Customer", "CUST-3", "SQLSTG_NA", "Fact.Sale", datetime(2025, 12, 1), 1, None, False, None, None, 0, False, None, None, None),
        (4, 1, 10, "Employee", "EMP-9", "SQLSTG_NA", "Fact.Sale", datetime(2026, 1, 25), 1, None, False, None, None, 0, False, None, None, None),
    ])
    out = rekey.runRekey(spark, catalog, batchId=2, packageExecutionId=20, businessDate=BUSINESS_DATE, maxQueueAgeDays=30, now=NOW)
    res = out["result"]
    assert res.queueDepth == 4 and res.rowsEscalated == 1 and res.rowsResolved == 2 and res.rowsStubbed == 1
    facts = {r["CustomerBusinessKey"]: r["CustomerKey"] for r in spark.table(f"{catalog}.gold.fact_sale").collect()}
    assert facts["CUST-1"] == 1                    # repointed at the real member
    assert facts["CUST-2"] > 1                     # repointed at a positive inferred stub, not -4
    assert facts["CUST-3"] == -1                   # escalated: left on Unknown
    assert res.factRowsRekeyed == 2 and res.remainingUnknownFacts == 1
    q = {r["QueueRowId"]: r for r in spark.table(queue).collect()}
    assert q[1]["ResolvedFlag"] is True and "Resolved to surrogate key 1" in q[1]["ResolutionNote"]
    assert q[2]["ResolvedFlag"] is False and q[2]["StubCreatedFlag"] is True and q[2]["PlaceholderKey"] == facts["CUST-2"] and q[2]["RetryCount"] == 1
    assert q[3]["ResolvedFlag"] is True and q[3]["ResolutionNote"].startswith("ABANDONED (HOLD_EXPIRED)")
    assert q[4]["ResolvedFlag"] is False and q[4]["RetryCount"] == 1        # Employee: no inferred support, retried
    stub = spark.table(specs.CUSTOMER.fullTableName(catalog)).where("CustomerBusinessKey = 'CUST-2'").collect()[0]
    assert stub["IsInferredMember"] is True and stub["Customer"] == "Inferred: CUST-2"
    rq = spark.table(f"{catalog}.silver.work_fact_rekey_queue").collect()
    assert {r["RekeyReasonCode"] for r in rq} == {"LATE_ARRIVING", "INFERRED_STUB", "UNRESOLVED"}
    assert all(r["AppliedFlag"] for r in rq if r["CorrectedSurrogateKey"] is not None)

    # Rerun same batch is idempotent for the rekey queue and does not double-stub
    out2 = rekey.runRekey(spark, catalog, batchId=2, packageExecutionId=21, businessDate=BUSINESS_DATE, maxQueueAgeDays=30, now=NOW)
    assert out2["result"].rowsStubbed == 0
    assert spark.table(specs.CUSTOMER.fullTableName(catalog)).where("CustomerBusinessKey = 'CUST-2'").count() == 1
    assert spark.table(f"{catalog}.silver.work_fact_rekey_queue").where("BatchId = 2").count() == 2   # CUST-2 stub + EMP-9 retry

    # The real CUST-2 arrives: enrichment in place keeps the key the fact already points at, queue closes
    scd.applyScd(spark, catalog, specs.CUSTOMER,
                 spark.createDataFrame([("CUST-2", "Real Two", "NA")], "CustomerBusinessKey STRING, Customer STRING, RegionCode STRING"),
                 3, 30, loadTimestamp=NOW)
    out3 = rekey.runRekey(spark, catalog, batchId=3, packageExecutionId=31, businessDate=BUSINESS_DATE, maxQueueAgeDays=30, now=NOW)
    assert out3["result"].rowsResolved == 1
    q = {r["QueueRowId"]: r for r in spark.table(queue).collect()}
    assert q[2]["ResolvedFlag"] is True
    assert spark.table(f"{catalog}.gold.fact_sale").where("CustomerBusinessKey = 'CUST-2'").collect()[0]["CustomerKey"] == facts["CUST-2"]
