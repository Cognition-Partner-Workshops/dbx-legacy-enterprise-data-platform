"""Check that everything the estate writes into the etl control schema exists.

The control schema in `sqlserver/control/` is the one set of tables every work
package touches, so it is also the one place where a package can be quietly
wrong: an INSERT naming a column that was renamed years ago parses fine in a
generated `.dtsx` and only fails the night it runs.

This reads the CREATE TABLE statements under `sqlserver/control/`, collects the
columns of each `etl.*` table, and then checks every `INSERT INTO etl.<table>
(...)` and `EXEC etl.usp_*` found anywhere in the estate against them. Static
only - no database is contacted, and nothing here proves the statements would
succeed at run time.

Usage:
    python3 validation/checks/check_control_object_columns.py [--json]
"""

import json
import os
import re
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONTROL_DIR = os.path.join(REPO_ROOT, "sqlserver", "control")

SCAN_EXTENSIONS = (".sql", ".py", ".dtsx")
SKIP_DIRS = {
    ".git", "__pycache__", "wwi-ssis", "wwi-database-scripts", "wwi-ssrs",
    "wwi-ssas-tabular", "wwi-ssas-multidimensional", "power-bi-dashboards",
    "sample-scripts", "workload-drivers", "wwi-integration-etl",
}

CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+etl\.(\w+)\s*\((.*?)\n\s*\);", re.IGNORECASE | re.DOTALL)
COLUMN_RE = re.compile(r"^\s{4,}(\w+)\s+(?:AS\s|[A-Z])", re.MULTILINE)
CONSTRAINT_RE = re.compile(r"^\s*(CONSTRAINT|CREATE|PRIMARY|UNIQUE|CHECK|FOREIGN|INDEX)\b",
                           re.IGNORECASE)
INSERT_RE = re.compile(
    r"INSERT\s+INTO\s+\[?etl\]?\.\[?(\w+)\]?\s*\\?n?\s*\"?\s*\(([^;]*?)\)\s*\\?n?\s*\"?\s*SELECT",
    re.IGNORECASE | re.DOTALL)
CREATE_PROC_RE = re.compile(r"CREATE\s+(?:PROCEDURE|FUNCTION)\s+etl\.(\w+)", re.IGNORECASE)
EXEC_RE = re.compile(r"EXEC(?:UTE)?\s+etl\.(\w+)", re.IGNORECASE)


def strip_noise(text):
    """Remove the Python string-literal punctuation that wraps generated SQL."""
    text = text.replace('\\n', '\n').replace('\\t', ' ')
    text = re.sub(r'"\s*\n\s*"', ' ', text)
    text = text.replace('"', ' ').replace("'", "'")
    return text


def collect_control_objects():
    tables, routines = {}, set()
    for dirpath, _dirnames, filenames in os.walk(CONTROL_DIR):
        for filename in sorted(filenames):
            if not filename.endswith(".sql"):
                continue
            body = open(os.path.join(dirpath, filename), encoding="utf-8").read()
            for name, block in CREATE_TABLE_RE.findall(body):
                columns = set()
                for line in block.splitlines():
                    if CONSTRAINT_RE.match(line):
                        continue
                    match = re.match(r"\s{4,}(\w+)\s+\S", line)
                    if match:
                        columns.add(match.group(1).upper())
                tables.setdefault(name.upper(), set()).update(columns)
            routines.update(name.upper() for name in CREATE_PROC_RE.findall(body))
    return tables, routines


def scan_repository(tables, routines):
    failures = []
    checked_inserts = 0
    checked_execs = 0

    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        dirnames[:] = [d for d in sorted(dirnames) if d not in SKIP_DIRS]
        if os.path.abspath(dirpath).startswith(CONTROL_DIR):
            continue
        for filename in sorted(filenames):
            if not filename.endswith(SCAN_EXTENSIONS):
                continue
            path = os.path.join(dirpath, filename)
            relative = os.path.relpath(path, REPO_ROOT)
            # The checkers themselves quote control object names in patterns.
            if relative.startswith(os.path.join("validation", "checks")):
                continue
            try:
                body = strip_noise(open(path, encoding="utf-8").read())
            except (UnicodeDecodeError, OSError):
                continue

            for table, column_block in INSERT_RE.findall(body):
                key = table.upper()
                if key not in tables:
                    failures.append("%s: INSERT into unknown control table etl.%s"
                                    % (relative, table))
                    continue
                checked_inserts += 1
                for column in re.split(r"[,\s]+", column_block.strip()):
                    column = column.strip("[]() \n").upper()
                    if not column or not re.match(r"^\w+$", column):
                        continue
                    if column not in tables[key]:
                        failures.append("%s: etl.%s has no column %s"
                                        % (relative, table, column))

            for routine in EXEC_RE.findall(body):
                key = routine.upper()
                if key.startswith("USP_") or key.startswith("UFN_"):
                    checked_execs += 1
                    if key not in routines:
                        failures.append("%s: calls undefined control routine etl.%s"
                                        % (relative, routine))
                else:
                    failures.append("%s: calls etl.%s, which does not follow the "
                                    "usp_/ufn_ naming of the control schema"
                                    % (relative, routine))

    return failures, checked_inserts, checked_execs


def run(options):
    """Entry point used by validation/checks/run_deep_checks.py."""
    return report(as_json=options.json)


def report(as_json):
    tables, routines = collect_control_objects()
    failures, inserts, execs = scan_repository(tables, routines)
    failures = sorted(set(failures))

    if as_json:
        print(json.dumps({
            "control_tables": len(tables),
            "control_routines": len(routines),
            "inserts_checked": inserts,
            "exec_calls_checked": execs,
            "failures": failures,
        }, indent=2))
    else:
        for failure in failures:
            print("FAIL  [control-columns] %s" % failure)
        print()
        print("control tables       %d" % len(tables))
        print("control routines     %d" % len(routines))
        print("inserts checked      %d" % inserts)
        print("exec calls checked   %d" % execs)
        print()
        print("%d failure(s)" % len(failures))
        print("Static checks only - nothing here was executed against a live system.")

    return 1 if failures else 0


def main():
    return report(as_json="--json" in sys.argv)


if __name__ == "__main__":
    sys.exit(main())
