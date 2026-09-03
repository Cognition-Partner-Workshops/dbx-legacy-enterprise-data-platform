"""Legacy key formats, one per source system, plus the cross-system crosswalk.

The ERP and the OLTP database were never reconciled at the key level. The ERP
uses typed, zero-padded, human-readable character keys because it was
configured in 1998; the OLTP uses identity integers. Everything downstream
depends on ``WWI_MDM.PARTY_XREF`` and ``WWI_REF.CODE_TRANSLATION`` to join
them, and those crosswalks are not complete - which is the point.

``crosswalk_state`` decides, deterministically, whether a given entity is
cleanly mapped, missing from the crosswalk, mapped to a retired identifier, or
mapped with a stale code. The proportions come from the ``quality`` block of
the scale configuration, so the data-quality and reconciliation packages
always have a bounded, reproducible number of real problems to find.
"""

from __future__ import annotations

from . import rng

# Crosswalk outcomes and their share of the configured mismatch budget.
CROSSWALK_CLEAN = "CLEAN"
CROSSWALK_MISSING = "MISSING_XREF"
CROSSWALK_RETIRED = "RETIRED_TARGET"
CROSSWALK_STALE = "STALE_CODE"
CROSSWALK_DUPLICATE = "DUPLICATE_XREF"

_MISMATCH_KINDS = (CROSSWALK_MISSING, CROSSWALK_RETIRED, CROSSWALK_STALE, CROSSWALK_DUPLICATE)
_MISMATCH_WEIGHTS = (0.42, 0.23, 0.25, 0.10)


def erp_customer_code(ordinal: int) -> str:
    return "CUS-%07d" % (100000 + ordinal)


def erp_supplier_code(ordinal: int) -> str:
    return "SUP%06d" % (4000 + ordinal)


def erp_product_code(ordinal: int) -> str:
    return "ITM-%06d-A" % (500 + ordinal)


def erp_po_number(ordinal: int, year: int) -> str:
    return "PO%04d-%07d" % (year, ordinal)


def erp_receipt_number(ordinal: int) -> str:
    return "RCT%09d" % (7000000 + ordinal)


def erp_ap_invoice_number(ordinal: int, supplier_ordinal: int) -> str:
    return "AP%05d-%08d" % (supplier_ordinal % 100000, 20000000 + ordinal)


def erp_payment_number(ordinal: int) -> str:
    return "PMT-%08d" % (3100000 + ordinal)


def erp_journal_number(ordinal: int, year: int, month: int) -> str:
    return "JRN-%04d%02d-%06d" % (year, month, ordinal % 1000000)


def oltp_customer_id(ordinal: int) -> int:
    return 1000 + ordinal


def oltp_supplier_id(ordinal: int) -> int:
    return 200 + ordinal


def oltp_stock_item_id(ordinal: int) -> int:
    return 500 + ordinal


def oltp_order_id(ordinal: int) -> int:
    return 60000 + ordinal


def oltp_invoice_id(ordinal: int) -> int:
    return 70000 + ordinal


def oltp_shipment_id(ordinal: int) -> int:
    return 800000 + ordinal


def partner_customer_code(ordinal: int, region: str) -> str:
    """The code a third-party partner file uses. Shorter, and region-prefixed."""
    return "%s%05d" % (region[:2].upper(), ordinal % 100000)


def crosswalk_state(seed: int, entity_kind: str, ordinal: int, mismatch_rate: float) -> str:
    """Deterministically classify one entity's crosswalk health."""
    if not rng.chance(seed, mismatch_rate, "xwalk", entity_kind, ordinal):
        return CROSSWALK_CLEAN
    return rng.weighted_pick(seed, _MISMATCH_KINDS, _MISMATCH_WEIGHTS,
                             "xwalk-kind", entity_kind, ordinal)


def retired_variant(code: str) -> str:
    """The shape a retired ERP key takes after a master-data merge."""
    return code + "-X"


def stale_variant(code: str) -> str:
    """The pre-1999-renumbering form some crosswalk rows were never updated from."""
    digits = "".join(ch for ch in code if ch.isdigit())
    return "OLD%s" % digits[-6:] if digits else code + "-OLD"


def source_system_key(system_code: str, natural_key) -> str:
    """The composite key convention ``stg.fn_SourceSystemKey`` reproduces."""
    return "%s|%s" % (system_code, natural_key)
