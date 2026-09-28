from dq_quality import notebook_support as ns


def test_restart_skips_earlier_steps_only():
    assert ns.shouldSkipForRestart("", "Data Quality") is False
    assert ns.shouldSkipForRestart(None, "Data Quality") is False
    assert ns.shouldSkipForRestart("Referential Screen", "Data Quality") is True
    assert ns.shouldSkipForRestart("Referential Screen", "Referential Screen") is False
    assert ns.shouldSkipForRestart("Referential Screen", "Reject Routing") is False
    assert ns.shouldSkipForRestart("Unknown Step", "Data Quality") is False


def test_every_package_has_a_step():
    assert len(ns.PACKAGE_STEP) == 10
    assert set(ns.PACKAGE_STEP.values()) <= set(ns.QUALITY_STEPS)
