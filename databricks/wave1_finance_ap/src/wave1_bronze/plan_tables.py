"""Turn the comma-separated ``source_tables`` job parameter into the JSON array
the ``for_each_task`` fans out over."""

import argparse
import json

from databricks.sdk.runtime import dbutils


def parse_tables(raw: str) -> list:
    return [t.strip().upper() for t in raw.replace("\n", ",").split(",") if t.strip()]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-tables", required=True)
    args = parser.parse_args()
    tables = parse_tables(args.source_tables)
    if not tables:
        raise SystemExit("source_tables resolved to an empty list")
    print("planned %d bronze loads: %s" % (len(tables), ", ".join(tables)))
    dbutils.jobs.taskValues.set(key="tables", value=json.dumps(tables))


if __name__ == "__main__":
    main()
