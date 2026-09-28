"""Expression-guarded warning / failure gates (SSIS ``raise_gate`` pattern).

In the legacy packages each gate is an Execute SQL Task behind a precedence
constraint such as ``@[User::MeasuredValue] > 2``; a Warning gate logs
ErrorCode 50000 through etl.usp_LogError, a Failure gate logs 50001 and then
THROWs so the package fails. Here the same decision is made in Python and the
logging goes through ``dbx_etl_common.control.logError``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence

WARNING_ERROR_CODE = 50000
FAILURE_ERROR_CODE = 50001


class DataQualityGateError(RuntimeError):
    """Raised when a Failure gate fires; mirrors ``THROW 50001``."""


@dataclass(frozen=True)
class Gate:
    name: str
    description: str
    severity: str  # "Warning" | "Failure"
    condition: Callable[[dict], bool]
    expression: str  # legacy precedence-constraint expression, kept for lineage


def evaluateGates(gates: Sequence[Gate], measures: dict) -> tuple[list, list]:
    """Return ``(warnings, failures)`` - the gates whose guard expression is true."""
    warnings, failures = [], []
    for gate in gates:
        if gate.condition(measures):
            (failures if gate.severity == "Failure" else warnings).append(gate)
    return warnings, failures


def applyGates(spark, catalog, gates: Sequence[Gate], measures: dict, packageExecutionId,
               batchId, packageName, logError) -> list:
    """Log every fired gate; raise for the first Failure gate after logging all of them.

    ``logError`` is ``dbx_etl_common.control.logError`` (injected so the pure part is
    testable). Returns the fired warning gates.
    """
    warnings, failures = evaluateGates(gates, measures)
    for gate in warnings:
        logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                 errorSeverity="Warning", errorCode=WARNING_ERROR_CODE, sourceName=packageName,
                 sourceComponent=gate.name, errorDescription=gate.description)
    for gate in failures:
        logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                 errorSeverity="Error", errorCode=FAILURE_ERROR_CODE, sourceName=packageName,
                 sourceComponent=gate.name, errorDescription=gate.description)
    if failures:
        raise DataQualityGateError("; ".join(g.description for g in failures))
    return warnings
