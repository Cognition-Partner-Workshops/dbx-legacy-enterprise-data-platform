"""CLI: ``python -m sales_lakehouse.mock_data.generate --scale small --out mock_data/output``.

Writes ``<out>/sqlserver/<Schema>/<Table>.csv``, ``<out>/oracle/<SCHEMA>/<TABLE>.csv`` and
``<out>/manifest.json`` (CONVENTIONS.md "Mock source data")."""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from . import oracle, sqlserver, sqlserver_transactions
from .common import SCALES, GenContext, Manifest, writeAll

DEFAULT_SEED = 42


def buildContext(seed: int = DEFAULT_SEED, scale: str = "small", asOf: date | None = None) -> GenContext:
    """Generate every table in memory. Order matters: Oracle reference (FX, calendars, tax,
    codes) -> SQL Server master data -> Oracle MDM/product (keyed off SQL Server customers
    and stock items) -> SQL Server transactions."""
    ctx = GenContext(seed=seed, scale=scale, asOf=asOf or date.today())
    oracle.generateReference(ctx)
    sqlserver.generateMasterData(ctx)
    oracle.generateMdm(ctx)
    oracle.generateProductMaster(ctx)
    sqlserver_transactions.generateTransactions(ctx)
    return ctx


def generate(outDir: Path, seed: int = DEFAULT_SEED, scale: str = "small", parquet: bool = False, asOf: date | None = None) -> Manifest:
    ctx = buildContext(seed=seed, scale=scale, asOf=asOf)
    return writeAll(ctx, outDir, parquet=parquet)


def parseArgs(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m sales_lakehouse.mock_data.generate", description=__doc__)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--scale", choices=sorted(SCALES), default="small")
    parser.add_argument("--out", type=Path, default=Path("mock_data/output"))
    parser.add_argument("--parquet", action="store_true", help="also write Parquet twins for the 5 largest tables")
    parser.add_argument("--as-of", type=date.fromisoformat, default=None, help="end of the 18-month span (default: today)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parseArgs(argv)
    manifest = generate(args.out, seed=args.seed, scale=args.scale, parquet=args.parquet, asOf=args.as_of)
    rows = sum(t["rowCount"] for t in manifest["tables"])
    print(f"wrote {len(manifest['tables'])} tables / {rows} rows / {len(manifest['edgeCases'])} edge cases to {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
