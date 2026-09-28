import pytest

from dq_quality import legacy_rules, rules


def test_seeded_rule_count_and_text_preserved():
    assert len(legacy_rules.LEGACY_RULES) == 29
    byCode = {r[0]: r for r in legacy_rules.LEGACY_RULES}
    assert len(byCode) == 29
    # rule text is the seeded T-SQL fragment, verbatim (sqlserver/control/05_seed_data_quality_rules.sql)
    assert byCode["CUST_NULL_NAME"][4] == "CustomerName IS NULL OR LTRIM(RTRIM(CustomerName)) = N''"


def test_translate_preserves_predicate_and_maps_functions():
    sql = rules.translateRuleExpression("CustomerName IS NULL OR LTRIM(RTRIM(CustomerName)) = ''",
                                        "stg.Customer", "wwi_dev")
    assert "TRIM(CustomerName)" in sql
    assert "LTRIM" not in sql


def test_translate_rewrites_legacy_table_references():
    sql = rules.translateRuleExpression("NOT EXISTS (SELECT 1 FROM stg.StockItem s WHERE s.StockItemBusinessKey = t.StockItemBusinessKey)",
                                        "stg.OrderLine", "wwi_dev")
    assert "wwi_dev.silver.stg_stock_item" in sql
    assert "stg.StockItem" not in sql


def test_rule_count_sql_targets_catalog_object():
    sql = rules.ruleCountSql("wwi_dev", "stg.Payment", "PaymentAmount <= 0")
    assert "COUNT(*)" in sql
    assert "wwi_dev.silver.stg_payment" in sql
    assert "WHERE PaymentAmount <= 0" in sql or "WHERE (PaymentAmount <= 0)" in sql


@pytest.mark.parametrize("measured, threshold, severity, expected", [
    (0, 0, "FAIL", "Passed"),
    (3, 5, "FAIL", "Passed"),
    (6, 5, "FAIL", "Failed"),
    (6, 5, "WARN", "Warned"),
    (1, None, None, "Warned"),
    (-1, 0, "FAIL", "NotEvaluated"),
])
def test_result_status(measured, threshold, severity, expected):
    assert rules.resultStatus(measured, threshold, severity) == expected


def _rule(code="R1", expr="x > 1", threshold=0.0, severity="FAIL"):
    return rules.RuleDefinition(1, code, "G", "stg.Customer", expr, severity, threshold)


def test_evaluate_rule_records_minus_one_on_error():
    def boom(sql):
        raise RuntimeError("syntax error")
    outcome = rules.evaluateRule(_rule(), "wwi_dev", boom)
    assert outcome.measuredValue == -1.0
    assert outcome.status == "NotEvaluated"
    assert outcome.breached is False
    assert "RuntimeError" in outcome.error


def test_evaluate_rules_sorted_and_failure_count():
    calls = []

    def runner(sql):
        calls.append(sql)
        return 7.0

    outcomes = rules.evaluateRules([_rule("B", threshold=10), _rule("A", threshold=2)], "wwi_dev", runner)
    assert [o.rule.ruleCode for o in outcomes] == ["A", "B"]
    assert rules.countFailingRules(outcomes) == 1
    rows = rules.outcomeRows(outcomes, 42, 7)
    assert rows[0]["BatchId"] == 42 and rows[0]["RuleCode"] == "A" and rows[0]["ResultStatus"] == "Failed"
    assert rows[1]["ResultStatus"] == "Passed"
