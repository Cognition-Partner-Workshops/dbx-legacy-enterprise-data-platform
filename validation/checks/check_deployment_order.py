#!/usr/bin/env python3
"""The order the SQL Server deployment drivers will actually run scripts in.

Both drivers order a directory by name and then pull forward whatever a
script's header names under "Deploy order : after ...", wildcards included.
This check reproduces that ordering offline and asks whether the result is
runnable:

  * every prerequisite a header names resolves to a script in the same stage;
  * the ordering has no cycle;
  * a script that writes a table created by another script in the same stage
    runs after it - the defect behind Msg 208 on etl.ReconciliationExemption
    and Msg 207 on [Region Code], both of which are ordering, not schema.

Static analysis only. Nothing here connects to SQL Server.

    python3 validation/checks/check_deployment_order.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import re
import sys

import estatelib as lib
import tsqllib

# The directories the drivers hand to sqlcmd, in stage order.
STAGE_DIRECTORIES = (
    "sqlserver/control",
    "sqlserver/oltp",
    "sqlserver/staging",
    "sqlserver/reference",
    "sqlserver/warehouse/dimensions",
    "sqlserver/warehouse/facts",
    "sqlserver/warehouse/aggregates",
    "sqlserver/procedures/dimensions",
    "sqlserver/procedures/facts",
    "sqlserver/views",
)

HEADER_LINES = 20
DEPLOY_ORDER_RE = re.compile(
    r"Deploy order\s*:\s*(.*?)(?:\n\s*\w[\w ]*:|\Z)", re.S)
WILDCARD_RE = re.compile(r"[\w.]*\*[\w.*]*\.sql")
UNRESOLVED_RE = re.compile(r"[A-Za-z0-9_.*]+\.sql")
BEFORE_RE = re.compile(r"\bbefore\b.*", re.I | re.S)


def stage_files(directory):
    """[(name, relative path)] the driver would find, in its own name order."""
    root = os.path.join(lib.REPO_ROOT, *directory.split("/"))
    found = []
    if not os.path.isdir(root):
        return found
    # The drivers use find -maxdepth 2, so one level of subdirectory is in.
    for dirpath, _dirnames, filenames in os.walk(root):
        relative = os.path.relpath(dirpath, root)
        if relative != "." and os.sep in relative:
            continue
        for filename in filenames:
            if not filename.lower().endswith(".sql"):
                continue
            rel = os.path.relpath(os.path.join(dirpath, filename),
                                  lib.REPO_ROOT).replace(os.sep, "/")
            found.append((filename, rel))
    return sorted(found)


def prerequisites(text, names, own_name):
    """The scripts a header names, matched the way both drivers match them."""
    header = "\n".join(text.splitlines()[:HEADER_LINES])
    field = DEPLOY_ORDER_RE.search(header)
    if not field:
        return []
    # Anything after "before" names a follower, not a prerequisite.
    after = BEFORE_RE.sub("", field.group(1))
    found = {name for name in names if name != own_name and name in after}
    for pattern in WILDCARD_RE.findall(after):
        found.update(name for name in names
                     if name != own_name and fnmatch.fnmatch(name, pattern))
    return sorted(found)


def check_unresolved(report, files):
    """A header naming a script no stage deploys is a stale dependency claim."""
    names = [name for name, _rel in files]
    for name, rel in files:
        header = "\n".join(stage_text(rel).splitlines()[:HEADER_LINES])
        field = DEPLOY_ORDER_RE.search(header)
        if not field:
            continue
        after = BEFORE_RE.sub("", field.group(1))
        resolved = prerequisites(stage_text(rel), names, name)
        for token in set(UNRESOLVED_RE.findall(after)):
            if token == name or "*" in token:
                continue
            if any(token in candidate for candidate in resolved):
                continue
            if any(token in candidate for candidate in names):
                continue
            report.warn(
                "sqlserver-deploy-order-unresolved", rel,
                "the header names prerequisite %s, which this stage does not "
                "deploy" % token)


def stage_text(rel):
    return lib.read_text(os.path.join(lib.REPO_ROOT, *rel.split("/")))


def order_stage(report, directory, files):
    """The order the drivers emit, reporting unresolved prerequisites and cycles."""
    names = [name for name, _rel in files]
    by_name = dict(files)
    emitted, visiting = [], []

    def emit(name):
        if name in emitted:
            return
        if name in visiting:
            report.error(
                "sqlserver-deploy-order-cycle", by_name[name],
                "prerequisite cycle: %s" % " -> ".join(visiting + [name]))
            return
        visiting.append(name)
        for candidate in prerequisites(stage_text(by_name[name]), names, name):
            emit(candidate)
        visiting.pop()
        emitted.append(name)

    for name in names:
        emit(name)
    return emitted


def run(args):
    report = lib.Report("check_deployment_order")
    tables = tsqllib.load_tables()

    stages = files_checked = writes_checked = 0
    for directory in STAGE_DIRECTORIES:
        files = stage_files(directory)
        if not files:
            continue
        stages += 1
        files_checked += len(files)
        check_unresolved(report, files)
        order = order_stage(report, directory, files)
        position = {name: index for index, name in enumerate(order)}
        by_name = dict(files)

        for name, rel in files:
            text = tsqllib.strip_comments(stage_text(rel))
            targets = set()
            for match in tsqllib.INSERT_RE.finditer(text):
                targets.add("%s.%s" % (tsqllib.unbracket(match.group(1)),
                                       tsqllib.unbracket(match.group(2))))
            for key in sorted(targets):
                table = tables.get(key)
                if table is None or not table.created:
                    continue
                creators = [os.path.basename(path) for path in table.paths
                            if os.path.basename(path) in by_name]
                if not creators or name in creators:
                    continue
                writes_checked += 1
                late = [creator for creator in creators
                        if position.get(creator, len(order)) > position[name]]
                if late and len(late) == len(creators):
                    report.error(
                        "sqlserver-deploy-order", rel,
                        "writes %s, which %s creates, but the drivers run %s first "
                        "(Msg 207/208 at deploy time)"
                        % (key, ", ".join(sorted(late)), name))

    report.count("deployment_stages_checked", stages)
    report.count("deployment_scripts_ordered", files_checked)
    report.count("cross_script_writes_checked", writes_checked)
    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
