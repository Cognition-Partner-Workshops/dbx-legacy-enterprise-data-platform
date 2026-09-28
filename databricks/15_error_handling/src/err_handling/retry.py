"""ERR_Retry_FailedSteps: bounded retry arithmetic (no Spark)."""


def backoffSeconds(backoffBaseSeconds, attemptNumber):
    """Legacy: BackoffSeconds = BackoffBaseSeconds * (AttemptNumber + 1)."""
    return int(backoffBaseSeconds) * (int(attemptNumber) + 1)


def shouldSweepAgain(attemptNumber, maxRetryAttempts, retryableStepCount):
    """Expression on the Record Retry Attempt -> Retry Sweep Final edge."""
    return int(attemptNumber) <= int(maxRetryAttempts) and int(retryableStepCount) > 0


def retriesExhausted(attemptNumber, maxRetryAttempts, retryableStepCount):
    """Expression on the Record Retry Attempt -> Mark Retries Exhausted edge."""
    return int(attemptNumber) > int(maxRetryAttempts) or int(retryableStepCount) == 0


MAX_SWEEPS_PER_INVOCATION = 2  # legacy: "Retry Sweep" then "Retry Sweep Final"
