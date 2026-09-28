"""Foreach-loop control logic of the ING_FILE_* packages, per file.

The loop tasks after the Data Flow ("Read Footer Record Count", "Reconcile
Control Totals", "Compute Price Checksum", ...) reduce to a handful of numbers
per file and a routing decision: archive the file or move it to the poison
folder. This module holds that decision as pure functions so it can be unit
tested without Spark or a workspace.

``ControlTotalMode``:

* ``legacy`` (default) reproduces what the packages effectively did - the
  footer values are read and recorded, but only the SupplierCatalog checksum
  gate poisons a file, because every other package's "Reconcile Control Totals"
  clause was written so that it always evaluates true (see the mapping doc).
* ``strict`` gates archive on the control totals the footers actually carry
  (NA T-record count, EU 9-record amount, APAC #TOTAL amount, carrier sidecar
  count, supplier TRL row count + checksum).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Optional

from . import feeds

MODE_LEGACY = "legacy"
MODE_STRICT = "strict"
MODES = (MODE_LEGACY, MODE_STRICT)

STATUS_PROCESSED = "Processed"
STATUS_QUARANTINED = "Quarantined"
STATUS_DUPLICATE = "Duplicate"
STATUS_SWEPT = "Swept"
STATUS_UNREADABLE = "Unreadable"


@dataclass
class FileTotals:
    """What one file's Data Flow and control tasks produced."""

    fileName: str
    linesRead: int = 0
    detailRowCount: int = 0  # rows loaded to the target (Count Detail Rows)
    malformedRowCount: int = 0  # rows sent to err.RejectedFileRow by the package's own validation
    conversionRowCount: int = 0  # Flat File Source error output / failed casts / column count
    unknownRecordCount: int = 0
    footerRowCount: Optional[int] = None  # NA T-record count, supplier TRL row count
    footerAmountTotal: Optional[Decimal] = None  # EU 9-record / APAC #TOTAL
    detailAmountTotal: Optional[Decimal] = None
    footerChecksum: Optional[int] = None  # supplier TRL checksum
    priceChecksum: Optional[int] = None  # computed supplier checksum
    sidecarRowCount: Optional[int] = None  # carrier etl.FileControlTotal.ExpectedRowCount
    duplicateScanCount: int = 0
    outOfToleranceCount: int = 0
    replayEligibleCount: int = 0
    warnings: List[str] = field(default_factory=list)

    @property
    def rejectedRowCount(self) -> int:
        return self.malformedRowCount + self.conversionRowCount + self.unknownRecordCount


def _amountsMatch(expected: Optional[Decimal], actual: Optional[Decimal]) -> bool:
    if expected is None:
        return True
    return Decimal(actual or 0).quantize(Decimal("0.01")) == Decimal(expected).quantize(Decimal("0.01"))


def intendedTotalsMatch(spec: feeds.FeedSpec, totals: FileTotals) -> bool:
    """The control-total comparison each footer / sidecar was designed for."""
    name = spec.packageName
    if name == feeds.PARTNER_SALES_NA.packageName:
        # "@Landed = ? OR ? = 0" with the footer count bound twice: a missing footer passes.
        return totals.footerRowCount in (None, 0) or totals.detailRowCount == totals.footerRowCount
    if name == feeds.PARTNER_SALES_EU.packageName:
        return _amountsMatch(totals.footerAmountTotal, totals.detailAmountTotal)
    if name == feeds.PARTNER_SALES_APAC.packageName:
        return _amountsMatch(totals.footerAmountTotal, totals.detailAmountTotal)
    if name == feeds.CARRIER_SCAN.packageName:
        return totals.sidecarRowCount in (None, 0) or totals.detailRowCount == totals.sidecarRowCount
    if name == feeds.SUPPLIER_CATALOG.packageName:
        rowsOk = totals.footerRowCount in (None, 0) or totals.detailRowCount == totals.footerRowCount
        return rowsOk and checksumMatches(totals)
    return True


def checksumMatches(totals: FileTotals) -> bool:
    """SupplierCatalog: @[User::PriceChecksum] == @[User::FooterChecksum]."""
    if totals.footerChecksum is None:
        return False
    return int(totals.priceChecksum or 0) == int(totals.footerChecksum)


def legacyTotalsMatch(spec: feeds.FeedSpec, totals: FileTotals) -> bool:
    """What "Reconcile Control Totals" evaluated to in the shipped packages.

    NA bound the footer count that nothing ever populated (0 -> true), EU
    compared ABS(sum) >= 0, APAC / carrier / supplier / FX compared @Landed >= 0.
    Only SupplierCatalog then adds the checksum comparison on the archive edge.
    """
    if spec.packageName == feeds.SUPPLIER_CATALOG.packageName:
        return checksumMatches(totals)
    return True


def controlTotalsMatch(spec: feeds.FeedSpec, totals: FileTotals, mode: str = MODE_LEGACY) -> bool:
    if mode not in MODES:
        raise ValueError("ControlTotalMode must be one of %s, got %r" % (MODES, mode))
    if mode == MODE_STRICT:
        return intendedTotalsMatch(spec, totals)
    if not intendedTotalsMatch(spec, totals):
        totals.warnings.append(
            "%s: control totals do not reconcile (detail=%s footerRows=%s footerAmount=%s detailAmount=%s "
            "sidecar=%s checksum=%s/%s); file archived under ControlTotalMode=legacy."
            % (totals.fileName, totals.detailRowCount, totals.footerRowCount, totals.footerAmountTotal,
               totals.detailAmountTotal, totals.sidecarRowCount, totals.priceChecksum, totals.footerChecksum)
        )
    return legacyTotalsMatch(spec, totals)


def fileStatus(spec: feeds.FeedSpec, totals: FileTotals, mode: str = MODE_LEGACY) -> str:
    """Route the file: Processed (archive) or Quarantined (poison folder)."""
    if spec.packageName == feeds.QUARANTINE_MALFORMED.packageName:
        # log_swept -> archive when any line is replay-eligible, poison otherwise
        return STATUS_SWEPT if totals.replayEligibleCount > 0 else STATUS_UNREADABLE
    return STATUS_PROCESSED if controlTotalsMatch(spec, totals, mode) else STATUS_QUARANTINED


def isDuplicateFile(fileName: str, fileSizeBytes: int, priorFiles) -> bool:
    """Duplicate-file detection against etl.FileIngestionLog.

    A file is a duplicate when a file of the same name and byte size has already
    been Processed / Swept. Same name with a different size is a re-send and is
    loaded again, as the legacy Foreach loop would have done.
    """
    for prior in priorFiles:
        if prior["FileName"] != fileName:
            continue
        if prior["Status"] not in (STATUS_PROCESSED, STATUS_SWEPT):
            continue
        if prior.get("FileSizeBytes") is None or int(prior["FileSizeBytes"]) == int(fileSizeBytes):
            return True
    return False


def archivePath(catalog: str, spec: feeds.FeedSpec, fileName: str, modifiedAtUtc) -> str:
    """archive/{feed}/{yyyy}/{MM}/{original_filename} (config/landing-zone.yaml archive layout)."""
    return feeds.volumePath(
        catalog, feeds.VOLUME_ARCHIVE, spec.archiveSubfolder,
        "%04d" % modifiedAtUtc.year, "%02d" % modifiedAtUtc.month, fileName,
    )


def poisonPath(catalog: str, fileName: str) -> str:
    return feeds.volumePath(catalog, feeds.VOLUME_QUARANTINE, feeds.POISON_SUBFOLDER, fileName)


def duplicatePath(catalog: str, spec: feeds.FeedSpec, fileName: str) -> str:
    return feeds.volumePath(catalog, feeds.VOLUME_QUARANTINE, spec.rejectSubfolder, feeds.DUPLICATE_SUBFOLDER, fileName)


def rejectFilePath(catalog: str, spec: feeds.FeedSpec, fileName: str) -> str:
    """@[User::RejectFilePath] = QuarantineFileRoot\\<feed>\\<file>.rej"""
    return feeds.volumePath(catalog, feeds.VOLUME_QUARANTINE, spec.rejectSubfolder, fileName + ".rej")
