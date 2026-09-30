"""Package-name -> implementation dispatch used by the thin ``notebooks/run_package.py`` task."""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable
from dataclasses import asdict, is_dataclass

from logistics_returns.aggregate import runAggRefreshDeliveryPerformance
from logistics_returns.carrier_scan import runCarrierScanIngestion
from logistics_returns.config import RunContext
from logistics_returns.extracts import SPECS, runExtract
from logistics_returns.facts import runFactLoadCreditNote, runFactLoadOrderFulfilment, runFactLoadReturn, runFactLoadShipment
from logistics_returns.staging import runStgLoadReturnAndCredit, runStgLoadShipment

PACKAGES: dict[str, Callable[[RunContext], object]] = {
    **{name: (lambda ctx, n=name: runExtract(ctx, n)) for name in SPECS},
    "ING_FILE_CarrierScan": runCarrierScanIngestion,
    "STG_Load_Shipment": runStgLoadShipment,
    "STG_Load_ReturnAndCredit": runStgLoadReturnAndCredit,
    "FACT_Load_Shipment": runFactLoadShipment,
    "FACT_Load_OrderFulfilment": runFactLoadOrderFulfilment,
    "FACT_Load_Return": runFactLoadReturn,
    "FACT_Load_CreditNote": runFactLoadCreditNote,
    "AGG_Refresh_DeliveryPerformanceSummary": runAggRefreshDeliveryPerformance,
}


def toPlain(result: object) -> object:
    if is_dataclass(result) and not isinstance(result, type):
        return asdict(result)
    if isinstance(result, list):
        return [toPlain(r) for r in result]
    return result


def runPackage(ctx: RunContext, packageName: str) -> object:
    if packageName not in PACKAGES:
        raise KeyError(f"Unknown package {packageName!r}; expected one of {sorted(PACKAGES)}")
    return toPlain(PACKAGES[packageName](ctx))


def stageLandingFiles(ctx: RunContext, samplesDir: str) -> list[str]:
    """Copy the bundled sample carrier files into the landing volume (idempotent; existing files are left alone)."""
    target = ctx.volumePath("inbound", "carrier")
    for sub in (("inbound", "carrier"), ("inbound", "processed"), ("inbound", "failed"), ("archive",)):
        os.makedirs(ctx.volumePath(*sub), exist_ok=True)
    copied: list[str] = []
    for name in sorted(os.listdir(samplesDir)):
        destination = os.path.join(target, name)
        if not os.path.exists(destination):
            shutil.copyfile(os.path.join(samplesDir, name), destination)
            copied.append(name)
    return copied
