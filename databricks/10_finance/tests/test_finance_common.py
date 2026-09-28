import datetime as dt

import finance_common as fc
from dbx_etl_common import control as fakeControl


def test_restart_from_step_skips_earlier_phases():
    assert fc.shouldSkipForRestart("FIN_Load_ApAging", "General Ledger")
    assert fc.shouldSkipForRestart("FIN_Currency_Revaluation", "General Ledger")
    assert not fc.shouldSkipForRestart("FIN_Load_GlPostings", "General Ledger")
    assert not fc.shouldSkipForRestart("FIN_Close_PeriodLock", "General Ledger")
    assert not fc.shouldSkipForRestart("FIN_Load_ApAging", "")
    assert not fc.shouldSkipForRestart("FIN_Load_ApAging", "Not A Phase")


def test_phase_sequences_follow_master_finance_close():
    seqs = {name: seq for name, (_s, seq, _g) in fc.PHASES.items()}
    assert seqs["FIN_Load_ApAging"] == seqs["FIN_Load_WithholdingTax"] == seqs["FIN_Load_CostAllocation"] == 10
    assert seqs["FIN_Currency_Revaluation"] == 20
    assert seqs["FIN_Load_GlPostings"] == 30
    assert seqs["FIN_Reconcile_SubledgerToGl"] == 40
    assert seqs["FIN_Close_PeriodLock"] == 60


def test_finance_params_derive_from_business_date():
    fin = fc.buildFinanceParams({}, dt.date(2024, 3, 31))
    assert fin.accountingPeriod == "2024-03"
    assert fin.agingAsOfDate == dt.date(2024, 3, 31)
    assert fin.revaluationDate == dt.date(2024, 3, 31)
    assert fin.varianceTolerance == 1
    assert fin.failOnMissingRate is True
    assert fin.allowCloseWithVariance is False


def test_finance_params_explicit_override():
    fin = fc.buildFinanceParams(
        {"AccountingPeriod": "2024-02", "AllowCloseWithVariance": "true", "VarianceTolerance": "5", "LedgerScope": "eu01"},
        dt.date(2024, 3, 31),
    )
    assert fin.accountingPeriod == "2024-02"
    assert fin.allowCloseWithVariance is True
    assert fin.varianceTolerance == 5
    assert fin.ledgerScope == "EU01"


def test_period_end_date():
    assert fc.periodEndDate("2024-02") == dt.date(2024, 2, 29)
    assert fc.periodEndDate("2024-12") == dt.date(2024, 12, 31)


def test_resolve_batch_adopts_when_missing():
    fakeControl.calls.clear()
    batchId, owns = fc.resolveBatchId(None, "wwi_dev", {"batchId": 0, "businessDate": dt.date(2024, 3, 31), "environmentCode": "DEV"})
    assert owns and batchId > 0
    assert fakeControl.calls[-1] == ("startBatch", "Master_Finance_Close", "Monthly", True)
    assert fc.resolveBatchId(None, "wwi_dev", {"batchId": 42}) == (42, False)


def test_package_execution_logs_failure_and_reraises():
    fakeControl.calls.clear()
    try:
        with fc.PackageExecution(None, "wwi_dev", 7, "FIN_Load_GlPostings", "General Ledger") as run:
            run.rowsRead = 3
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    kinds = [c[0] for c in fakeControl.calls]
    assert kinds == ["logPackageStart", "logError", "logPackageEnd"]
    assert fakeControl.calls[-1][2] == "Failed"
    assert fakeControl.calls[0][2] == "WWI_Finance"
