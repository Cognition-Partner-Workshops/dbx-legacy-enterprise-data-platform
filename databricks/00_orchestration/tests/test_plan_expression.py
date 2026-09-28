"""The SSIS expression subset used by ssis/orchestration-plan.json edges and Expression Tasks."""
import pytest

from wwi_orchestration import plan_expression as pe


def _vars(**kw):
    base = {"$Package::RestartFromStep": "", "$Package::MaxExtractAttempts": 3, "User::ExtractAttempt": 1,
            "User::ExtractFailed": False, "User::NightlyBatchRunning": 0, "$Package::SkipWhenNightlyRunning": True,
            "User::SubledgerVariance": 0.0, "$Package::AllowCloseWithVariance": False, "User::BatchId": 0}
    base.update(kw)
    return base


def test_restart_gate_expressions():
    expr = '@[$Package::RestartFromStep] == "" || @[$Package::RestartFromStep] == "Stage Load"'
    assert pe.test(expr, _vars()) is True
    assert pe.test(expr, _vars(**{"$Package::RestartFromStep": "Stage Load"})) is True
    assert pe.test(expr, _vars(**{"$Package::RestartFromStep": "Facts"})) is False


def test_logical_not_and_comparison_precedence():
    expr = "@[User::NightlyBatchRunning] == 0 || !@[$Package::SkipWhenNightlyRunning]"
    assert pe.test(expr, _vars()) is True
    assert pe.test(expr, _vars(**{"User::NightlyBatchRunning": 1})) is False
    assert pe.test(expr, _vars(**{"User::NightlyBatchRunning": 1, "$Package::SkipWhenNightlyRunning": False})) is True
    assert pe.evaluate("1 + 2 * 3 > 6 && !FALSE", {}) is True


def test_retry_loop_expressions():
    v = _vars()
    assert pe.test("@[User::ExtractFailed] && @[User::ExtractAttempt] < @[$Package::MaxExtractAttempts]", v) is False
    v["User::ExtractFailed"] = True
    assert pe.test("@[User::ExtractFailed] && @[User::ExtractAttempt] < @[$Package::MaxExtractAttempts]", v) is True
    name, value = pe.assign("@[User::ExtractAttempt] = @[User::ExtractAttempt] + 1", v)
    assert (name, value, v["User::ExtractAttempt"]) == ("User::ExtractAttempt", 2, 2)
    with pytest.raises(pe.PlanExpressionError):
        pe.assign("@[User::Undeclared] = 1", v)


def test_non_boolean_verdict_is_a_plan_defect():
    with pytest.raises(pe.PlanExpressionError):
        pe.test("@[User::ExtractAttempt] + 1", _vars())


def test_coerce_follows_default_type():
    assert pe.coerce("True", False) is True
    assert pe.coerce("0", False) is False
    assert pe.coerce("7", 0) == 7
    assert pe.coerce("", 3) == 3
    assert pe.coerce("{{tasks.x.values.y}}", 3) == 3
    assert pe.coerce("1.5", 0.0) == 1.5
    assert pe.coerce("Facts", "") == "Facts"


def test_referenced_variables():
    assert pe.referencedVariables('@[$Package::RestartFromStep] == "" || @[User::X] > 1') == ["$Package::RestartFromStep", "User::X"]


def test_every_plan_expression_evaluates_to_a_boolean(plan):
    for root in plan["roots"]:
        variables = pe.variableTable(root)
        for e in root["edges"]:
            if e.get("expression"):
                assert isinstance(pe.evaluate(e["expression"], variables), bool), (root["root"], e["expression"])
        for n in root["nodes"]:
            if n.get("assignment"):
                pe.assign(n["assignment"], variables)
