#!/usr/bin/env python3
"""Compare built .ispac archives against the canonical package ownership.

An .ispac is a flat zip of the project manifest, the project parameters, the
connection managers and one entry per package. A package that silently fails to
make it into the archive - or lands in the wrong project - only shows up at run
time as "package not found", so the build is checked here instead: every
package in config/estate-catalog.yaml must appear exactly once, in the archive
of the project the catalog gives it.

Usage:
    python3 validation/checks/check_ispac_contents.py <directory of .ispac> [--json]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import zipfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import orchestration  # noqa: E402  (path set above)


def packages_in(archive_path):
    with zipfile.ZipFile(archive_path) as archive:
        return {os.path.basename(name)[: -len(".dtsx")]
                for name in archive.namelist() if name.lower().endswith(".dtsx")}


def analyse(directory):
    owner = orchestration.project_by_package()
    expected = {}
    for package, project in owner.items():
        expected.setdefault(project, set()).add(package)

    failures = []
    built = {}
    for name in sorted(os.listdir(directory)):
        if name.endswith(".ispac"):
            built[name[: -len(".ispac")]] = packages_in(os.path.join(directory, name))

    for project in sorted(expected):
        if project not in built:
            failures.append("%s.ispac was not built" % project)
            continue
        missing = sorted(expected[project] - built[project])
        extra = sorted(built[project] - expected[project])
        for package in missing:
            failures.append("%s.ispac does not hold %s" % (project, package))
        for package in extra:
            failures.append("%s.ispac holds %s, which the catalog does not give it"
                            % (project, package))
    for project in sorted(set(built) - set(expected)):
        failures.append("%s.ispac is not a catalog project" % project)

    seen = {}
    for project, packages in built.items():
        for package in packages:
            seen.setdefault(package, []).append(project)
    for package, projects in sorted(seen.items()):
        if len(projects) > 1:
            failures.append("%s is built into more than one project: %s"
                            % (package, ", ".join(sorted(projects))))

    return {
        "directory": directory,
        "projects_expected": len(expected),
        "projects_built": len(built),
        "packages_expected": len(owner),
        "packages_built": sum(len(packages) for packages in built.values()),
        "failures": failures,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("directory")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = analyse(args.directory)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        for key in ("projects_expected", "projects_built",
                    "packages_expected", "packages_built"):
            print("%-24s %s" % (key, report[key]))
        for failure in report["failures"]:
            print("FAIL  %s" % failure)
        print("%d failure(s)" % len(report["failures"]))
    return 1 if report["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
