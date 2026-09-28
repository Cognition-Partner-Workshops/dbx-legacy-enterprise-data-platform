from err_handling import retry


def test_backoff_grows_with_attempt():
    assert retry.backoffSeconds(30, 1) == 60
    assert retry.backoffSeconds(30, 2) == 90
    assert retry.backoffSeconds("10", "0") == 10


def test_sweep_and_exhaustion_expressions_are_complementary():
    for attempt in range(0, 6):
        for retryable in (0, 1, 5):
            again = retry.shouldSweepAgain(attempt, 3, retryable)
            done = retry.retriesExhausted(attempt, 3, retryable)
            assert again != done


def test_max_sweeps_is_two_like_legacy():
    assert retry.MAX_SWEEPS_PER_INVOCATION == 2
