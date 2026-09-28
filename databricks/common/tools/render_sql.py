#!/usr/bin/env python3
"""Render sql/00_etl_control_tables.sql, sql/01_etl_seed_control_data.sql and views/00_etl_operational_views.sql
from the dbx_etl_common schema / seed / view definitions (keeps SQL and Python in sync).

    python databricks/common/tools/render_sql.py          # write files
    python databricks/common/tools/render_sql.py --check  # exit 1 when the committed files are stale
"""
from __future__ import annotations

import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
COMMON = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(COMMON, "dbx_etl_common", "src"))

from dbx_etl_common import bootstrap  # noqa: E402

OUTPUTS = {
    os.path.join(COMMON, "sql", "00_etl_control_tables.sql"): bootstrap.renderDdl,
    os.path.join(COMMON, "sql", "01_etl_seed_control_data.sql"): bootstrap.renderSeedSql,
    os.path.join(COMMON, "views", "00_etl_operational_views.sql"): bootstrap.renderViewSql,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    stale = []
    for path, render in OUTPUTS.items():
        content = render()
        if args.check:
            current = open(path, encoding="utf-8").read() if os.path.exists(path) else None
            if current != content:
                stale.append(path)
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(content)
            print(f"wrote {os.path.relpath(path, COMMON)}")
    if stale:
        print("stale rendered SQL (run tools/render_sql.py): " + ", ".join(os.path.relpath(p, COMMON) for p in stale))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
