"""Shared plumbing for the mock data generator: deterministic RNG, the in-memory
table store, CSV/Parquet writers and the manifest/edge-case registry.

Value formatting follows CONVENTIONS.md "Mock source data": UTF-8 RFC4180 CSV,
ISO dates/timestamps, empty string for NULL, ``0``/``1`` for SQL Server BIT and
``Y``/``N`` (emitted by the generators as plain strings) for Oracle CHAR(1) flags.
"""
from __future__ import annotations

import csv
import hashlib
import json
import random
import zlib
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import TypedDict

from .domain import Territory
from .schema import ORACLE, SQLSERVER, TABLE_COLUMNS

REGIONS = ("NA", "EU", "APAC")
REGION_WEIGHTS = {"NA": 0.40, "EU": 0.35, "APAC": 0.25}
REGION_CURRENCIES = {"NA": ("USD", "CAD"), "EU": ("EUR", "GBP"), "APAC": ("AUD", "SGD", "JPY")}
CURRENCIES = ("USD", "CAD", "EUR", "GBP", "AUD", "SGD", "JPY")
FISCAL_CALENDARS = {"NA": "NA445", "EU": "EUCAL", "APAC": "APACJUN"}
SPAN_MONTHS = 18

# Very large tables get Parquet twins when --parquet is passed.
PARQUET_TABLE_COUNT = 5
MANIFEST_KEY_SAMPLE = 500  # keys per edge case kept in manifest.json (keyCount is always the full total)

SCALES: dict[str, dict[str, int]] = {
    "small": {"customers": 200, "orders": 5_000, "stockItems": 150, "salespeople": 24},
    "medium": {"customers": 2_000, "orders": 50_000, "stockItems": 500, "salespeople": 120},
}

TWO_PLACES = Decimal("0.01")
THREE_PLACES = Decimal("0.001")
EIGHT_PLACES = Decimal("0.00000001")

Value = str | int | float | bool | Decimal | date | datetime | None
Row = dict[str, Value]
Numeric = Decimal | int | float | str


def money(value: Numeric, places: Decimal = TWO_PLACES) -> Decimal:
    return Decimal(str(value)).quantize(places, rounding=ROUND_HALF_UP)


def rate(value: Numeric) -> Decimal:
    return money(value, EIGHT_PLACES)


def qty(value: Numeric) -> Decimal:
    return money(value, THREE_PLACES)


def addMonths(day: date, months: int) -> date:
    monthIndex = day.month - 1 + months
    year = day.year + monthIndex // 12
    month = monthIndex % 12 + 1
    lastDay = (date(year + (month == 12), 1 if month == 12 else month + 1, 1) - timedelta(days=1)).day
    return date(year, month, min(day.day, lastDay))


def daysBetween(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def fmtValue(value: Value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


@dataclass
class EdgeCase:
    code: str
    description: str
    tables: list[str]
    keys: list[dict[str, Value]] = field(default_factory=list)

    def toJson(self) -> dict[str, object]:
        return {
            "code": self.code,
            "description": self.description,
            "tables": sorted(self.tables),
            "keyCount": len(self.keys),
            "keys": [{k: fmtValue(v) if not isinstance(v, (int, float)) or isinstance(v, bool) else v
                      for k, v in key.items()} for key in self.keys[:MANIFEST_KEY_SAMPLE]],
        }


class People:
    """Application.People rows plus the role lookups the other generators need."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.rows: list[Row] = []
        self.managers: dict[str, int] = {}
        self.salespeopleByTerritory: dict[str, list[int]] = {}
        self.insideSales: dict[str, int] = {}
        self.pickers: list[int] = []
        self.accounts: list[int] = []

    def addPerson(self, fullName: str, isEmployee: bool, isSalesperson: bool, validFrom: datetime, isSystemUser: bool = False) -> int:
        personId = len(self.rows) + 1
        first = fullName.split()[0]
        domain = "wideworldimporters.com" if isEmployee else "example.com"
        self.rows.append({
            "PersonID": personId, "FullName": fullName, "PreferredName": first if not isSystemUser else fullName,
            "IsPermittedToLogon": isEmployee, "LogonName": f"{first.lower()}{personId}@{domain}" if isEmployee else "NO LOGON",
            "IsExternalLogonProvider": False, "IsSystemUser": isSystemUser, "IsEmployee": isEmployee, "IsSalesperson": isSalesperson,
            "UserPreferences": None, "PhoneNumber": None if isSystemUser else f"({self.rng.randint(200, 999)}) 555-{self.rng.randint(1000, 9999)}",
            "FaxNumber": None, "EmailAddress": None if isSystemUser else f"{first.lower()}{personId}@{domain}", "CustomFields": None,
            "LastEditedBy": 1, "ValidFrom": validFrom, "ValidTo": datetime(9999, 12, 31, 23, 59, 59),
        })
        return personId


@dataclass
class Scratch:
    """Cross-module lookups handed from one generator to the next (not written out)."""

    fxRates: dict[tuple[str, date], Decimal] = field(default_factory=dict)
    fxGapMonth: tuple[str, date, date] | None = None
    planIdByCode: dict[str, int] = field(default_factory=dict)
    planByPerson: dict[int, str] = field(default_factory=dict)
    capPersonId: int | None = None
    people: People | None = None
    costByItem: dict[int, Decimal] = field(default_factory=dict)
    priceLists: dict[int, Row] = field(default_factory=dict)
    listIdByCode: dict[str, int] = field(default_factory=dict)
    priceByListAndItem: dict[tuple[int, int], Row] = field(default_factory=dict)
    promotionsByRegion: dict[str, list[Row]] = field(default_factory=dict)
    extractWatermark: datetime | None = None
    customerCountry: dict[int, str] = field(default_factory=dict)
    customerTerritory: dict[int, Territory] = field(default_factory=dict)
    lateArrivingCustomers: set[int] = field(default_factory=set)
    customersById: dict[int, Row] = field(default_factory=dict)
    partyByCustomer: dict[int, int] = field(default_factory=dict)


class TableEntry(TypedDict):
    system: str
    schema: str
    table: str
    path: str
    rowCount: int
    columns: list[str]
    sha256: str
    parquetPath: str | None


@dataclass
class GenContext:
    """Everything a generator module needs; also the in-memory table store."""

    seed: int
    scale: str
    asOf: date
    tables: dict[tuple[str, str, str], list[Row]] = field(default_factory=dict)
    edgeCases: dict[str, EdgeCase] = field(default_factory=dict)
    scratch: Scratch = field(default_factory=lambda: Scratch())

    @property
    def params(self) -> dict[str, int]:
        return SCALES[self.scale]

    @property
    def spanStart(self) -> date:
        return addMonths(self.asOf, -SPAN_MONTHS) + timedelta(days=1)

    @property
    def spanEnd(self) -> date:
        return self.asOf

    def rng(self, streamName: str) -> random.Random:
        """Independent deterministic stream per table/topic so that adding a generator
        does not perturb the values of an unrelated one."""
        streamSeed = (self.seed * 1_000_003 + zlib.crc32(streamName.encode("utf-8"))) % (2**63)
        return random.Random(streamSeed)

    def pickRegion(self, rng: random.Random) -> str:
        return rng.choices(REGIONS, weights=[REGION_WEIGHTS[r] for r in REGIONS], k=1)[0]

    def randomDate(self, rng: random.Random, start: date | None = None, end: date | None = None) -> date:
        start = start or self.spanStart
        end = end or self.spanEnd
        return start + timedelta(days=rng.randint(0, max(0, (end - start).days)))

    def randomTimestamp(self, rng: random.Random, day: date) -> datetime:
        return datetime(day.year, day.month, day.day, rng.randint(6, 19), rng.randint(0, 59), rng.randint(0, 59))

    # --- table store -----------------------------------------------------------
    def put(self, system: str, schema: str, table: str, rows: list[Row]) -> list[Row]:
        columns = TABLE_COLUMNS[(system, schema, table)]
        expected = set(columns)
        for row in rows:
            missing = expected - row.keys()
            extra = row.keys() - expected
            if missing or extra:
                raise ValueError(f"{schema}.{table}: missing={sorted(missing)} extra={sorted(extra)}")
        self.tables[(system, schema, table)] = rows
        return rows

    def get(self, system: str, schema: str, table: str) -> list[Row]:
        return self.tables[(system, schema, table)]

    def sql(self, schema: str, table: str) -> list[Row]:
        return self.get(SQLSERVER, schema, table)

    def ora(self, schema: str, table: str) -> list[Row]:
        return self.get(ORACLE, schema, table)

    # --- edge cases -------------------------------------------------------------
    def tag(self, code: str, description: str, table: str, **key: Value) -> None:
        edgeCase = self.edgeCases.get(code)
        if edgeCase is None:
            edgeCase = self.edgeCases[code] = EdgeCase(code, description, [])
        if table not in edgeCase.tables:
            edgeCase.tables.append(table)
        edgeCase.keys.append(dict(key))


# --- output ---------------------------------------------------------------------
def tableRelPath(system: str, schema: str, table: str) -> str:
    return f"{system}/{schema}/{table}.csv"


def writeCsv(path: Path, columns: list[str], rows: list[Row]) -> tuple[int, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow([fmtValue(row[c]) for c in columns])
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return len(rows), digest.hexdigest()


def writeParquet(path: Path, columns: list[str], rows: list[Row]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    # Same textual representation as the CSV so both twins round-trip identically.
    arrays = {c: pa.array([fmtValue(r[c]) if r[c] is not None else None for r in rows], type=pa.string())
              for c in columns}
    pq.write_table(pa.table(arrays), path)


class Manifest(TypedDict):
    manifestVersion: int
    generator: str
    seed: int
    scale: str
    asOfDate: str
    spanStart: str
    spanEnd: str
    regions: dict[str, object]
    tables: list[TableEntry]
    edgeCases: list[dict[str, object]]


def writeAll(ctx: GenContext, outDir: Path, parquet: bool = False) -> Manifest:
    outDir.mkdir(parents=True, exist_ok=True)
    tablesManifest: list[TableEntry] = []
    for (system, schema, table), rows in ctx.tables.items():
        columns = TABLE_COLUMNS[(system, schema, table)]
        rel = tableRelPath(system, schema, table)
        rowCount, sha = writeCsv(outDir / rel, columns, rows)
        tablesManifest.append({
            "system": system, "schema": schema, "table": table, "path": rel,
            "rowCount": rowCount, "columns": list(columns), "sha256": sha, "parquetPath": None,
        })
    if parquet:
        largest = sorted(tablesManifest, key=lambda t: (-t["rowCount"], t["path"]))[:PARQUET_TABLE_COUNT]
        for entry in largest:
            rows = ctx.tables[(entry["system"], entry["schema"], entry["table"])]
            rel = entry["path"][:-4] + ".parquet"
            writeParquet(outDir / rel, entry["columns"], rows)
            entry["parquetPath"] = rel
    manifest: Manifest = {
        "manifestVersion": 1,
        "generator": "sales_lakehouse.mock_data",
        "seed": ctx.seed,
        "scale": ctx.scale,
        "asOfDate": ctx.asOf.isoformat(),
        "spanStart": ctx.spanStart.isoformat(),
        "spanEnd": ctx.spanEnd.isoformat(),
        "regions": {"codes": list(REGIONS), "targetMix": REGION_WEIGHTS, "currencies": REGION_CURRENCIES,
                    "fiscalCalendars": FISCAL_CALENDARS},
        "tables": sorted(tablesManifest, key=lambda t: t["path"]),
        "edgeCases": [ctx.edgeCases[c].toJson() for c in sorted(ctx.edgeCases)],
    }
    (outDir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    return manifest
