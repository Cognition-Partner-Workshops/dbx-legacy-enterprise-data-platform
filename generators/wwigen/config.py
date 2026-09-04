"""Run configuration: scale modes, seed, history window and defect rates.

Row counts for every scale mode live in exactly one place,
``generators/config/scales.json``. This module loads that file and turns it
into a :class:`RunConfig` that the table generators read. Derived counts
(order lines, invoice lines, receipt lines) are computed here from the
declared drivers so a scale mode never has to restate the same number twice.
"""

from __future__ import annotations

import datetime
import json
import os
from dataclasses import dataclass, field

GENERATORS_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCALES_PATH = os.path.join(GENERATORS_ROOT, "config", "scales.json")
DEFAULT_OUTPUT_DIR = os.path.join(GENERATORS_ROOT, "output")

SCALE_MODES = ("small", "medium", "large")


def load_scales_document(path: str = SCALES_PATH) -> dict:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _parse_date(text: str) -> datetime.date:
    return datetime.date(*(int(part) for part in text.split("-")))


@dataclass(frozen=True)
class RunConfig:
    """Everything a table generator needs to know about the current run."""

    scale: str
    seed: int
    counts: dict
    quality: dict
    region_mix: dict
    history_start: datetime.date
    history_end: datetime.date
    snapshot_date: datetime.date
    output_dir: str
    config_version: int
    chunk_rows: int = 50000
    derived: dict = field(default_factory=dict)

    # -- counts ---------------------------------------------------------

    def count(self, key: str) -> int:
        if key in self.derived:
            return int(self.derived[key])
        if key not in self.counts:
            raise KeyError("scale %s does not declare a count for %r" % (self.scale, key))
        return int(self.counts[key])

    def ratio(self, key: str) -> float:
        return float(self.counts[key])

    def defect(self, key: str) -> float:
        return float(self.quality[key])

    # -- calendar -------------------------------------------------------

    @property
    def history_days(self) -> int:
        return (self.history_end - self.history_start).days

    # -- load identity ---------------------------------------------------

    @property
    def batch_id(self) -> int:
        """The batch every generated extract of this run belongs to.

        Derived from the run identity, so a regenerated run reproduces its
        own batch number and two scales never share one.
        """
        return int("%s%02d" % (self.snapshot_date.strftime("%Y%m%d"),
                               SCALE_MODES.index(self.scale) + 1))

    @property
    def loaded_at_utc(self) -> datetime.datetime:
        return datetime.datetime.combine(self.snapshot_date, datetime.time(2, 0, 0))

    def signature(self) -> dict:
        """Identity of this run, recorded in the manifest and resume markers."""
        return {
            "scale": self.scale,
            "seed": self.seed,
            "config_version": self.config_version,
            "history_start": self.history_start.isoformat(),
            "history_end": self.history_end.isoformat(),
        }


def build_run_config(scale: str,
                     seed: int = None,
                     output_dir: str = None,
                     scales_path: str = SCALES_PATH,
                     chunk_rows: int = 50000) -> RunConfig:
    document = load_scales_document(scales_path)
    if scale not in document["scales"]:
        raise ValueError("unknown scale %r; expected one of %s" % (scale, ", ".join(SCALE_MODES)))
    counts = dict(document["scales"][scale])
    history = document["history"]
    resolved_seed = int(document["default_seed"] if seed is None else seed)

    orders = int(counts["orders"])
    purchase_orders = int(counts["purchase_orders"])
    ap_invoices = int(counts["ap_invoices"])
    derived = {
        "order_lines": int(round(orders * float(counts["avg_order_lines"]))),
        "invoices": int(round(orders * float(counts["invoice_ratio"]))),
        "invoice_lines": int(round(orders * float(counts["invoice_ratio"])
                                   * float(counts["avg_order_lines"]))),
        "purchase_order_lines": int(round(purchase_orders * float(counts["avg_po_lines"]))),
        "receipts": int(round(purchase_orders * float(counts["receipt_ratio"]))),
        "receipt_lines": int(round(purchase_orders * float(counts["receipt_ratio"])
                                   * float(counts["avg_po_lines"]))),
        "ap_invoice_lines": int(round(ap_invoices * float(counts["avg_ap_invoice_lines"]))),
    }

    return RunConfig(
        scale=scale,
        seed=resolved_seed,
        counts=counts,
        quality=dict(document["quality"]),
        region_mix=dict(document["region_mix"]),
        history_start=_parse_date(history["start_date"]),
        history_end=_parse_date(history["end_date"]),
        snapshot_date=_parse_date(history["snapshot_date"]),
        output_dir=os.path.abspath(output_dir or os.path.join(DEFAULT_OUTPUT_DIR, scale)),
        config_version=int(document["config_version"]),
        chunk_rows=chunk_rows,
        derived=derived,
    )
