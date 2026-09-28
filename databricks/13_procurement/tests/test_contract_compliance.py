from datetime import date
from decimal import Decimal

from pyspark.sql import Row

from procurement_lib import contract_compliance as cc


def _sources(spark):
    lines = spark.createDataFrame([
        Row(PurchaseOrderNumber="PO-1", LineNumber=1, CategoryCode="PACK", OrderedOuters=10, ExpectedUnitPricePerOuter=Decimal("10.00")),   # COMPLIANT
        Row(PurchaseOrderNumber="PO-1", LineNumber=2, CategoryCode="PACK", OrderedOuters=10, ExpectedUnitPricePerOuter=Decimal("10.20")),   # within 2% -> COMPLIANT
        Row(PurchaseOrderNumber="PO-1", LineNumber=3, CategoryCode="PACK", OrderedOuters=10, ExpectedUnitPricePerOuter=Decimal("11.00")),   # PRICE_LEAKAGE 10.00
        Row(PurchaseOrderNumber="PO-2", LineNumber=1, CategoryCode="CHEM", OrderedOuters=5, ExpectedUnitPricePerOuter=Decimal("3.00")),     # EXPIRED_CONTRACT
        Row(PurchaseOrderNumber="PO-3", LineNumber=1, CategoryCode="PACK", OrderedOuters=5, ExpectedUnitPricePerOuter=Decimal("3.00")),     # NON_PREFERRED
        Row(PurchaseOrderNumber="PO-3", LineNumber=2, CategoryCode="MISC", OrderedOuters=5, ExpectedUnitPricePerOuter=Decimal("3.00")),     # NO_CONTRACT
        Row(PurchaseOrderNumber="PO-OLD", LineNumber=1, CategoryCode="MISC", OrderedOuters=5, ExpectedUnitPricePerOuter=Decimal("3.00")),   # outside window
    ])
    headers = spark.createDataFrame([
        Row(PurchaseOrderNumber="PO-1", SupplierId=100, OrderDate=date(2024, 3, 20)),
        Row(PurchaseOrderNumber="PO-2", SupplierId=100, OrderDate=date(2024, 3, 21)),
        Row(PurchaseOrderNumber="PO-3", SupplierId=200, OrderDate=date(2024, 3, 22)),
        Row(PurchaseOrderNumber="PO-OLD", SupplierId=200, OrderDate=date(2024, 1, 1)),
    ])
    contracts = spark.createDataFrame([
        Row(SupplierId=100, CategoryCode="PACK", ContractNumber="C-1", ContractPricePerOuter=Decimal("10.00"), ContractStartDate=date(2024, 1, 1), ContractEndDate=None, IsPreferred=True),
        Row(SupplierId=100, CategoryCode="CHEM", ContractNumber="C-2", ContractPricePerOuter=Decimal("2.00"), ContractStartDate=date(2023, 1, 1), ContractEndDate=date(2023, 12, 31), IsPreferred=False),
    ])
    return lines, headers, contracts


def test_compliance_classification_and_leakage(spark):
    lines, headers, contracts = _sources(spark)
    work = cc.evaluateContractCompliance(lines, headers, contracts, date(2024, 3, 31), 30, 2)
    rows = {(r.PurchaseOrderNumber, r.LineNumber): r for r in work.collect()}
    assert ("PO-OLD", 1) not in rows and len(rows) == 6
    assert rows[("PO-1", 1)].ComplianceStatusCode == "COMPLIANT" and rows[("PO-1", 1)].LeakageAmount == Decimal("0.00")
    assert rows[("PO-1", 2)].ComplianceStatusCode == "COMPLIANT" and rows[("PO-1", 2)].LeakageAmount == Decimal("2.00")
    assert rows[("PO-1", 3)].ComplianceStatusCode == "PRICE_LEAKAGE" and rows[("PO-1", 3)].LeakageAmount == Decimal("10.00")
    assert rows[("PO-2", 1)].ComplianceStatusCode == "EXPIRED_CONTRACT" and rows[("PO-2", 1)].LeakageAmount == Decimal("5.00")
    assert rows[("PO-3", 1)].ComplianceStatusCode == "NON_PREFERRED"
    assert rows[("PO-3", 2)].ComplianceStatusCode == "NO_CONTRACT"

    leakage, expired = cc.measureLeakage(work)
    assert leakage == Decimal("17.00") and expired == 1

    summary = {r.SupplierId: r for r in cc.summariseComplianceBySupplier(work).collect()}
    # supplier 100: total 100+102+110+15 = 327, off-contract 110+15 = 125
    assert summary[100].TotalAmount == Decimal("327.00") and summary[100].OffContractAmount == Decimal("125.00")
    assert summary[100].LeakageAmount == Decimal("17.00")
    assert summary[100].CompliancePercent == Decimal("61.7737")
    assert summary[200].CompliancePercent == Decimal("0.0000")

    rejects = cc.nonCompliantLines(work)
    assert sorted(r.BusinessKey for r in rejects.collect()) == ["PO-1|3", "PO-2|1", "PO-3|1", "PO-3|2"]
