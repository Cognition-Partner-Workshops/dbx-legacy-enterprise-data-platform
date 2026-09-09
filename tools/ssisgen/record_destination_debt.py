"""Rebuild ssis/destination-metadata-debt.txt from the specs as they stand.

The register pins the destinations that still cannot honour their target table,
so a spec that regresses fails generation instead of failing in SSISDB. It is
maintained by hand as destinations are repaired; this script only exists to
re-derive the whole register when a schema or a generator change moves several
entries at once, and it never adds an entry the generator would not have
rejected on its own.

    python tools/ssisgen/record_destination_debt.py [--check]

--check rebuilds the register in memory and fails when it differs from the file
on disk, which is the form the offline checks run.
"""

from __future__ import annotations

import contextlib
import io
import os
import runpy
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import dbschema  # noqa: E402
import ssisgen  # noqa: E402

HEADER = [
    "# Destinations that cannot yet honour their target table contract.",
    "# Format: data flow|destination|table|why it is still broken",
    "#",
    "# Two kinds of entry, both a live defect:",
    "#  * a destination whose table no DDL under sqlserver/ deploys, or whose",
    "#    buffer column cannot be inserted into the column of that name, still",
    "#    publishes buffer-derived external metadata - the shape SSIS rejects",
    "#    with VS_NEEDSNEWMETADATA when it revalidates against the table;",
    "#  * a destination that leaves a NOT NULL column without a default unmapped",
    "#    validates and then fails its first insert.",
    "#",
    "# The list only shrinks: a destination that honours its contract while it is",
    "# still listed fails generation, and one that does not and is not listed",
    "# fails generation too. See tools/ssisgen/ssisgen.py.",
]


def collect():
    """Run every spec with the register empty and record what it rejects."""
    records = {}
    ssisgen._DEBT = {}
    original = ssisgen.DataFlow._destination_contract

    def patched(self, comp, lineage):
        try:
            return original(self, comp, lineage)
        except ssisgen.ContractError as exc:
            message = str(exc)
            if "does not honour its table contract" not in message:
                raise
            reason = message.split("table contract: ", 1)[1].split(". Fix the spec", 1)[0]
            records[(self.name, comp["name"], comp["table"])] = reason
            # Registering the destination is what the repaired register would
            # have done, so the rest of the package emits the metadata it will
            # ship with rather than a second failure.
            ssisgen._DEBT[ssisgen.debt_key(self.name, comp["name"], comp["table"])] = reason
            return original(self, comp, lineage)

    ssisgen.DataFlow._destination_contract = patched
    try:
        run_specs()
    finally:
        ssisgen.DataFlow._destination_contract = original
    return records


def run_specs():
    root = os.path.join(dbschema.REPO_ROOT, "ssis")
    for folder in sorted(os.listdir(root)):
        directory = os.path.join(root, folder)
        if not os.path.isdir(directory):
            continue
        for name in sorted(os.listdir(directory)):
            if not name.endswith(".py"):
                continue
            sys.path.insert(0, directory)
            cwd = os.getcwd()
            os.chdir(directory)
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    runpy.run_path(os.path.join(directory, name), run_name="__main__")
            except SystemExit:
                pass
            finally:
                os.chdir(cwd)
                sys.path.pop(0)


def render(records):
    lines = list(HEADER)
    for (flow, destination, table), reason in sorted(records.items()):
        lines.append("%s|%s|%s|%s" % (flow, destination, table, reason))
    return "\n".join(lines) + "\n"


def main(argv):
    text = render(collect())
    if "--check" in argv:
        with io.open(ssisgen.DEBT_REGISTER, encoding="utf-8") as handle:
            on_disk = handle.read()
        if on_disk != text:
            sys.stderr.write("ssis/destination-metadata-debt.txt is out of date; "
                             "run python tools/ssisgen/record_destination_debt.py\n")
            return 1
        print("destination debt register matches the specs")
        return 0
    with io.open(ssisgen.DEBT_REGISTER, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
    print("recorded %d destinations" % (len(text.splitlines()) - len(HEADER)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
