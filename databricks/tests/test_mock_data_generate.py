"""Layout, manifest schema, determinism, column contract, encoding and Parquet output of
``sales_lakehouse.mock_data``. Edge cases / RI live in test_mock_data_edge_cases.py."""
from __future__ import annotations

import ast
import csv
import hashlib
import json
import re
from collections import Counter
from datetime import date
from pathlib import Path

import pytest

from sales_lakehouse.mock_data.common import PARQUET_TABLE_COUNT, SCALES
from sales_lakehouse.mock_data.generate import generate, parseArgs
from sales_lakehouse.mock_data.schema import ORACLE, SQLSERVER, TABLE_COLUMNS

REPO_ROOT = Path(__file__).resolve().parents[2]
AS_OF = date(2026, 9, 28)


def readCsv(root: Path, system: str, schema: str, table: str) -> list[dict[str, str]]:
    with (root / system / schema / f"{table}.csv").open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def fileHashes(root: Path) -> dict[str, str]:
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture(scope="session")
def outDir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("mock_small")
    generate(root, seed=42, scale="small", parquet=True, asOf=AS_OF)
    return root


@pytest.fixture(scope="session")
def manifest(outDir: Path) -> dict:
    return json.loads((outDir / "manifest.json").read_text(encoding="utf-8"))


def test_cli_defaults() -> None:
    args = parseArgs([])
    assert (args.seed, args.scale, str(args.out), args.parquet) == (42, "small", "mock_data/output", False)
    assert parseArgs(["--scale", "medium", "--parquet", "--seed", "7"]).scale == "medium"


def test_layout_matches_conventions(outDir: Path, manifest: dict) -> None:
    for (system, schema, table) in TABLE_COLUMNS:
        assert (outDir / system / schema / f"{table}.csv").is_file(), f"{system}/{schema}/{table}.csv missing"
    assert (outDir / "manifest.json").is_file()
    assert {t["path"] for t in manifest["tables"]} == {f"{s}/{sc}/{t}.csv" for (s, sc, t) in TABLE_COLUMNS}
    assert all(re.fullmatch(r"(sqlserver/[A-Z][A-Za-z]+/[A-Z][A-Za-z]+|oracle/WWI_[A-Z]+/[A-Z_0-9]+)\.csv", t["path"]) for t in manifest["tables"])


def test_manifest_schema(manifest: dict) -> None:
    assert manifest["manifestVersion"] == 1
    assert manifest["seed"] == 42 and manifest["scale"] == "small"
    assert manifest["asOfDate"] == AS_OF.isoformat() and manifest["spanEnd"] == manifest["asOfDate"]
    assert manifest["spanStart"] == "2025-03-29"  # 18 months ending as-of
    assert manifest["regions"]["codes"] == ["NA", "EU", "APAC"]
    assert manifest["regions"]["fiscalCalendars"] == {"NA": "NA445", "EU": "EUCAL", "APAC": "APACJUN"}
    for entry in manifest["tables"]:
        assert set(entry) == {"system", "schema", "table", "path", "rowCount", "columns", "sha256", "parquetPath"}
        assert entry["rowCount"] > 0 and re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])
    codes = [e["code"] for e in manifest["edgeCases"]]
    assert codes == sorted(codes) and len(set(codes)) == len(codes)
    for edge in manifest["edgeCases"]:
        assert set(edge) == {"code", "description", "tables", "keyCount", "keys"}
        assert re.fullmatch(r"[A-Z][A-Z0-9_]+", edge["code"]) and edge["description"]
        assert edge["tables"] and all(re.fullmatch(r"[A-Za-z_]+\.[A-Za-z_]+", t) for t in edge["tables"])
        assert edge["keyCount"] >= len(edge["keys"]) >= 1
        assert all(isinstance(k, dict) and k for k in edge["keys"])


def test_headers_match_ddl_column_contract(outDir: Path, manifest: dict) -> None:
    for entry in manifest["tables"]:
        with (outDir / entry["path"]).open(encoding="utf-8", newline="") as handle:
            header = next(csv.reader(handle))
        assert header == entry["columns"] == list(TABLE_COLUMNS[(entry["system"], entry["schema"], entry["table"])])


def _ddlText(paths: list[Path]) -> str:
    return "\n".join(p.read_text(encoding="utf-8", errors="replace") for path in paths for p in (path.rglob("*.sql") if path.is_dir() else [path]))


def test_schema_columns_exist_in_checked_in_ddl() -> None:
    sqlDdl = _ddlText([REPO_ROOT / "sqlserver" / "oltp" / "01_tables", REPO_ROOT / "sqlserver" / "oltp" / "02_extensions", REPO_ROOT / "wwi-ssdt"])
    oraDdl = _ddlText([REPO_ROOT / "oracle" / "tables"])
    sqlColumns = set(re.findall(r"\[([A-Za-z0-9_]+)\]", sqlDdl))
    oraColumns = set(re.findall(r"\b([A-Z][A-Z0-9_]+)\b", oraDdl))
    for (system, schema, table), columns in TABLE_COLUMNS.items():
        known = sqlColumns if system == SQLSERVER else oraColumns
        missing = [c for c in columns if c not in known]
        assert not missing, f"{schema}.{table}: not in DDL {missing}"


def test_deterministic_for_seed_and_scale(outDir: Path, tmp_path: Path) -> None:
    again = tmp_path / "again"
    generate(again, seed=42, scale="small", parquet=True, asOf=AS_OF)
    first = fileHashes(outDir)
    second = fileHashes(again)
    assert first.keys() == second.keys()
    assert [k for k in first if first[k] != second[k]] == []
    different = tmp_path / "seed7"
    generate(different, seed=7, scale="small", asOf=AS_OF)
    assert fileHashes(different)["sqlserver/Sales/Orders.csv"] != first["sqlserver/Sales/Orders.csv"]


def test_scale_small_sizes(manifest: dict) -> None:
    counts = {t["path"]: t["rowCount"] for t in manifest["tables"]}
    assert abs(counts["sqlserver/Sales/Customers.csv"] - SCALES["small"]["customers"]) <= 10  # + duplicate extract rows
    assert SCALES["small"]["orders"] * 0.98 <= counts["sqlserver/Sales/Orders.csv"] <= SCALES["small"]["orders"]
    assert counts["sqlserver/Sales/OrderLines.csv"] > counts["sqlserver/Sales/Orders.csv"]
    assert counts["oracle/WWI_REF/FX_RATE_DAILY.csv"] > 3000


def test_csv_encoding_rules(outDir: Path) -> None:
    orders = readCsv(outDir, SQLSERVER, "Sales", "Orders")
    assert {r["IsUndersupplyBackordered"] for r in orders} <= {"0", "1"}  # BIT
    assert {r["IsTaxInclusivePricing"] for r in orders} == {"0", "1"}
    assert all(r["Comments"] == "" for r in orders)  # SQL NULL -> empty string
    assert all(re.fullmatch(r"\d{4}-\d{2}-\d{2}", r["OrderDate"]) for r in orders)
    assert all(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", r["LastEditedWhen"]) for r in orders)
    master = readCsv(outDir, ORACLE, "WWI_MDM", "CUST_MASTER")
    assert {r["DELETED_FLG"] for r in master} <= {"Y", "N"} and {r["CREDIT_HOLD_FLG"] for r in master} <= {"Y", "N"}
    assert any(r["EDI_PARTNER_ID"] == "" for r in master)
    raw = (outDir / "sqlserver" / "Sales" / "Customers.csv").read_bytes()
    raw.decode("utf-8")
    assert b"\r\n" not in raw
    customers = readCsv(outDir, SQLSERVER, "Sales", "Customers")
    assert any("," in r["CustomerName"] or "(" in r["CustomerName"] for r in customers)  # quoting exercised


def test_parquet_for_five_largest_tables(outDir: Path, manifest: dict) -> None:
    pq = pytest.importorskip("pyarrow.parquet")
    withParquet = [t for t in manifest["tables"] if t["parquetPath"]]
    assert len(withParquet) == PARQUET_TABLE_COUNT
    largest = sorted(manifest["tables"], key=lambda t: -t["rowCount"])[:PARQUET_TABLE_COUNT]
    assert {t["path"] for t in withParquet} == {t["path"] for t in largest}
    for entry in withParquet:
        table = pq.read_table(outDir / entry["parquetPath"])
        assert table.num_rows == entry["rowCount"] and table.column_names == entry["columns"]
        csvRows = readCsv(outDir, entry["system"], entry["schema"], entry["table"])
        firstColumn = entry["columns"][0]
        assert table.column(firstColumn).to_pylist()[:50] == [r[firstColumn] for r in csvRows[:50]]


def test_no_parquet_without_flag(tmp_path: Path) -> None:
    manifest = generate(tmp_path, seed=42, scale="small", parquet=False, asOf=AS_OF)
    assert all(t["parquetPath"] is None for t in manifest["tables"])
    assert not list(tmp_path.rglob("*.parquet"))


def test_region_split(outDir: Path) -> None:
    customers = {r["CustomerID"]: r for r in readCsv(outDir, SQLSERVER, "Sales", "Customers")}
    share = Counter(r["RegionCode"] for r in customers.values())
    total = sum(share.values())
    for region, target in {"NA": 0.40, "EU": 0.35, "APAC": 0.25}.items():
        assert abs(share[region] / total - target) < 0.08, share
    territories = {r["SalesTerritoryID"]: r for r in readCsv(outDir, SQLSERVER, "Sales", "SalesTerritories")}
    assert {r["RegionCode"] for r in territories.values()} == {"NA", "EU", "APAC"}
    orders = readCsv(outDir, SQLSERVER, "Sales", "Orders")
    orderShare = Counter(territories[o["SalesTerritoryID"]]["RegionCode"] for o in orders)
    for region, target in {"NA": 0.40, "EU": 0.35, "APAC": 0.25}.items():
        assert abs(orderShare[region] / len(orders) - target) < 0.10, orderShare
    currencyByRegion = {"NA": {"USD", "CAD"}, "EU": {"EUR", "GBP"}, "APAC": {"AUD", "SGD", "JPY"}}
    for o in orders:
        region = territories[o["SalesTerritoryID"]]["RegionCode"]
        assert o["CurrencyCode"] in currencyByRegion[region]
        assert customers[o["CustomerID"]]["RegionCode"] == region


def test_notebook_is_valid_python_and_uses_widgets() -> None:
    source = (REPO_ROOT / "databricks" / "notebooks" / "00_mock_data" / "generate_mock_data.py").read_text(encoding="utf-8")
    assert source.startswith("# Databricks notebook source")
    ast.parse(source)
    assert 'dbutils.widgets.text("catalog"' in source and 'dbutils.widgets.text("mock_data_root"' in source
    assert "from sales_lakehouse.mock_data.generate import generate" in source


def _checkConstraints() -> dict[tuple[str, str], dict[str, set[str]]]:
    """{(Schema, Table): {Column: allowed values}} from the CHECK ([Col] IN (...)) constraints in the DDL."""
    allowed: dict[tuple[str, str], dict[str, set[str]]] = {}
    for folder in ("01_tables", "02_extensions"):
        for path in (REPO_ROOT / "sqlserver" / "oltp" / folder).glob("*.sql"):
            match = re.search(r"^\d+_([A-Z][A-Za-z]+)\.([A-Z][A-Za-z]+)(\.Extensions)?\.sql$", path.name)
            if not match:
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            for column, values in re.findall(r"CHECK \((?:\[\w+\] IS NULL OR )?\[(\w+)\] IN \(([^)]*)\)\)", text):
                allowed.setdefault((match.group(1), match.group(2)), {})[column] = set(re.findall(r"N'([^']*)'", values))
    return allowed


def test_code_columns_respect_ddl_check_constraints(outDir: Path) -> None:
    constraints = _checkConstraints()
    assert ("Sales", "Orders") in constraints and ("Sales", "CustomerPayments") in constraints
    violations = []
    for (schema, table), columns in constraints.items():
        if (SQLSERVER, schema, table) not in TABLE_COLUMNS:
            continue
        rows = readCsv(outDir, SQLSERVER, schema, table)
        for column, values in columns.items():
            if column not in TABLE_COLUMNS[(SQLSERVER, schema, table)]:
                continue
            bad = {r[column] for r in rows if r[column] != "" and r[column] not in values}
            if bad:
                violations.append(f"{schema}.{table}.{column}: {sorted(bad)} not in {sorted(values)}")
    assert not violations, "\n".join(violations)
