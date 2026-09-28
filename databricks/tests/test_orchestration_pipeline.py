"""Orchestrator: stage ordering, selection syntax and failure propagation."""

from dataclasses import replace

import pytest

from sales_lakehouse.orchestration import pipeline
from sales_lakehouse.orchestration.pipeline import RunOptions, Stage, parseStages, runAll


def test_default_stages_follow_layer_dependency_order():
    names = list(parseStages("all"))
    assert names[:2] == ["bronze", "silver_reference"]
    assert names.index("silver_party") < names.index("silver_customers") < names.index("silver_dimensions")
    assert names.index("silver_dimensions") < names.index("silver_transactions") < names.index("gold_facts")
    assert names.index("gold_facts") < names.index("gold_aggregates") < names.index("gold_reporting")
    assert names.index("gold_aggregates") < names.index("sales_ops") < names.index("validation")
    assert names.index("quality") < names.index("validation")
    assert "mock" not in names and "month_end" not in names


def test_every_dependency_is_declared_before_its_dependant():
    seen: set[str] = set()
    for stage in pipeline.STAGES:
        assert set(stage.dependsOn) <= seen, f"{stage.name} depends on a later stage"
        seen.add(stage.name)


def test_parse_stages_accepts_optional_additions_and_reorders_selection():
    assert parseStages("+mock")[0] == "mock"
    assert parseStages("+month_end")[-3:] == ("month_end", "quality", "validation")
    assert parseStages("gold_facts, bronze") == ("bronze", "gold_facts")
    assert parseStages(["validation", "silver_customers"]) == ("silver_customers", "validation")
    assert parseStages(None) == parseStages("") == parseStages("all")
    with pytest.raises(ValueError):
        parseStages("silver_facts")


def _stubStages(monkeypatch, calls: list[str], failing: set[str]) -> None:
    def make(stage: Stage) -> Stage:
        def run(spark, cfg, opts):
            calls.append(stage.name)
            if stage.name in failing:
                raise RuntimeError(f"{stage.name} exploded")
            return f"{stage.name} ok"

        return replace(stage, run=run)

    stubbed = {name: make(stage) for name, stage in pipeline._BY_NAME.items()}
    monkeypatch.setattr(pipeline, "_BY_NAME", stubbed)


def test_run_all_stops_at_first_failure_and_skips_dependants(spark, cfg, monkeypatch):
    calls: list[str] = []
    _stubStages(monkeypatch, calls, {"silver_party"})
    run = runAll(spark, cfg, stages="bronze,silver_reference,silver_party,silver_customers,gold_facts")
    assert calls == ["bronze", "silver_reference", "silver_party"]
    status = {r.stage: r.status for r in run.results}
    assert status == {
        "bronze": "OK",
        "silver_reference": "OK",
        "silver_party": "FAILED",
        "silver_customers": "SKIPPED",
        "gold_facts": "SKIPPED",
    }
    assert not run.ok and "RuntimeError" in next(r for r in run.results if r.stage == "silver_party").message


def test_run_all_keep_going_runs_independent_stages(spark, cfg, monkeypatch):
    calls: list[str] = []
    _stubStages(monkeypatch, calls, {"silver_party"})
    run = runAll(spark, cfg, stages="bronze,silver_reference,silver_party,silver_customers", options=RunOptions(failFast=False))
    assert calls == ["bronze", "silver_reference", "silver_party"]
    status = {r.stage: r.status for r in run.results}
    assert status["silver_reference"] == "OK" and status["silver_customers"] == "SKIPPED"
    assert "silver_party" in next(r for r in run.results if r.stage == "silver_customers").message
    assert "1 failed" in run.summary() and "stage" in run.table()


def test_partner_feed_dir_defaults_under_mock_root(cfg):
    assert pipeline.partnerFeedDir(cfg, RunOptions()).startswith(cfg.mockDataRoot)
    assert pipeline.partnerFeedDir(cfg, RunOptions(partnerFeedDir="/x/feed")) == "/x/feed"
