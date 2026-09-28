from decimal import Decimal

from pyspark.sql import Row

import finance_rules as rules


def test_sequential_allocation_consumes_earlier_outputs(spark):
    rules_ = [
        {"AllocationRuleId": 2, "RuleSequence": 20, "SourceCostCentreCode": "IT", "DriverCode": "HEADCOUNT", "RuleSetCode": "STANDARD"},
        {"AllocationRuleId": 1, "RuleSequence": 10, "SourceCostCentreCode": "FAC", "DriverCode": "SQFT", "RuleSetCode": "STANDARD"},
    ]
    targets = spark.createDataFrame([
        Row(AllocationRuleId=1, TargetCostCentreCode="IT", DriverValue=Decimal("100")),
        Row(AllocationRuleId=1, TargetCostCentreCode="SALES", DriverValue=Decimal("300")),
        Row(AllocationRuleId=2, TargetCostCentreCode="SALES", DriverValue=Decimal("10")),
        Row(AllocationRuleId=2, TargetCostCentreCode="OPS", DriverValue=Decimal("30")),
    ])
    balances = spark.createDataFrame([
        Row(CostCentreCode="FAC", AccountingPeriod="2024-03", Amount=Decimal("400")),
        Row(CostCentreCode="IT", AccountingPeriod="2024-03", Amount=Decimal("300")),
        Row(CostCentreCode="IT", AccountingPeriod="2024-02", Amount=Decimal("9999")),
    ])
    out = rules.allocateCosts(rules_, targets, balances, "2024-03", 5)
    rows = {(r["AllocationRuleId"], r["TargetCostCentreCode"]): r for r in out.collect()}
    # rule 10: FAC 400 split 100/300 -> IT 100, SALES 300
    assert rows[(1, "IT")]["AllocatedAmount"] == Decimal("100.0000") and rows[(1, "SALES")]["AllocatedAmount"] == Decimal("300.0000")
    # rule 20: IT pool = 300 own + 100 received = 400 split 10/30 -> SALES 100, OPS 300
    assert rows[(2, "SALES")]["PoolAmount"] == Decimal("400.0000")
    assert rows[(2, "SALES")]["AllocatedAmount"] == Decimal("100.0000") and rows[(2, "OPS")]["AllocatedAmount"] == Decimal("300.0000")

    summary = {r["TargetCostCentreCode"]: r for r in rules.summariseAllocationsByTarget(out).collect()}
    assert summary["SALES"]["AllocatedCostAmount"] == Decimal("400.0000") and summary["SALES"]["AllocationRuleCount"] == 2
    assert summary["OPS"]["AllocationRuleCount"] == 1

    ruleDf = spark.createDataFrame([Row(AllocationRuleId=r["AllocationRuleId"], RuleSetCode="STANDARD", RuleSequence=r["RuleSequence"],
                                        SourceCostCentreCode=r["SourceCostCentreCode"], DriverCode=r["DriverCode"], IsActive=True) for r in rules_])
    # pools 400 + 300 = 700, allocated 800 (IT re-allocates the 100 it received) -> residual -100
    assert rules.unallocatedResidual(balances, ruleDf, out, "2024-03", "STANDARD") == -100.0


def test_active_rules_require_known_source_cost_centre(spark):
    ruleDf = spark.createDataFrame([
        Row(AllocationRuleId=1, RuleSetCode="STANDARD", RuleSequence=10, SourceCostCentreCode="FAC", DriverCode="SQFT", IsActive=True),
        Row(AllocationRuleId=2, RuleSetCode="STANDARD", RuleSequence=20, SourceCostCentreCode="GHOST", DriverCode="SQFT", IsActive=True),
        Row(AllocationRuleId=3, RuleSetCode="STANDARD", RuleSequence=30, SourceCostCentreCode="FAC", DriverCode="SQFT", IsActive=False),
        Row(AllocationRuleId=4, RuleSetCode="ALT", RuleSequence=10, SourceCostCentreCode="FAC", DriverCode="SQFT", IsActive=True),
    ])
    cc = spark.createDataFrame([Row(CostCentreCode="FAC", LedgerCode="NA01", RegionCode="NA", IsActive=True)])
    assert [r["AllocationRuleId"] for r in rules.activeRules(ruleDf, cc, "STANDARD").collect()] == [1]
