"""Carve a migration wave out of the estate and check it is closed.

Reads a wave definition (config/waves/*.yaml), the generated static lineage
(docs/inventories/ssis-sql-lineage.json) and ssis/orchestration-plan.json, and
reports every place the slice reaches outside itself: objects it reads that
something else writes, objects it writes that something else reads, plan
phases it shares with other packages, and the shared procedures it executes.
Static only; nothing is executed against a database.
"""
import argparse
import json
import os
from collections import Counter, defaultdict

import estatelib as lib

LINEAGE_PATH = os.path.join(lib.REPO_ROOT, "docs", "inventories",
                            "ssis-sql-lineage.json")
PLAN_PATH = os.path.join(lib.REPO_ROOT, "ssis", "orchestration-plan.json")
SOURCE_LAYERS = {"oracle", "oltp"}
CHECK = "wave-closure"


def load_wave(path):
    import yaml
    with open(path) as handle:
        wave = yaml.safe_load(handle)
    wave["packages"] = list(wave["packages"])
    wave.setdefault("shared_layers", ["reference", "dw-dimension"])
    wave.setdefault("plumbing_prefixes", ["ETL.", "ERR."])
    return wave


def is_plumbing(obj, prefixes):
    return any(obj.startswith(prefix) for prefix in prefixes)


def index_lineage(lineage, prefixes):
    """Return per-object writers/readers and per-package exec sets."""
    writers = defaultdict(set)
    readers = defaultdict(set)
    layers = {}
    execs = {}
    for name, data in lineage.items():
        for item in data["reads"] + data["reads_via_procs"]:
            layers[item["object"]] = item["layer"]
            if not is_plumbing(item["object"], prefixes):
                readers[item["object"]].add(name)
        for item in data["writes"] + data["writes_via_procs"]:
            layers[item["object"]] = item["layer"]
            if not is_plumbing(item["object"], prefixes):
                writers[item["object"]].add(name)
        execs[name] = {
            (item["object"] if isinstance(item, dict) else item)
            for item in data["execs"]
        }
    return writers, readers, layers, execs


def plan_membership(plan):
    """Return package -> list of (root, phase) it is invoked from."""
    membership = defaultdict(list)
    phases = {}
    for root in plan["roots"]:
        for node in root["nodes"]:
            if node.get("kind") != "phase":
                continue
            key = (root["root"], node["name"])
            members = [child["package"] for child in node.get("children", [])]
            phases[key] = members
            for member in members:
                membership[member].append(key)
    return membership, phases


def upstream_closure(seed, lineage, writers, layers, stop_layers, prefixes):
    """Packages needed if every non-shared upstream writer were pulled in."""
    closure = set(seed)
    frontier = list(seed)
    while frontier:
        name = frontier.pop()
        data = lineage[name]
        for item in data["reads"] + data["reads_via_procs"]:
            obj = item["object"]
            if is_plumbing(obj, prefixes) or layers.get(obj) in stop_layers:
                continue
            for writer in writers.get(obj, ()):
                if writer not in closure:
                    closure.add(writer)
                    frontier.append(writer)
    return closure


def classify_inputs(slice_set, lineage, writers, layers, shared_layers,
                    prefixes):
    inputs = {}
    for name in slice_set:
        data = lineage[name]
        for item in data["reads"] + data["reads_via_procs"]:
            obj = item["object"]
            if is_plumbing(obj, prefixes) or obj in inputs:
                continue
            layer = item["layer"]
            outside = sorted(writers.get(obj, set()) - slice_set)
            inside = sorted(writers.get(obj, set()) & slice_set)
            if layer in SOURCE_LAYERS:
                kind = "source"
            elif not writers.get(obj):
                kind = "orphan-input"
            elif not outside:
                kind = "internal"
            elif layer in shared_layers:
                kind = "shared-conformed"
            elif inside:
                kind = "co-written"
            else:
                kind = "upstream-leak"
            inputs[obj] = {
                "object": obj, "layer": layer, "kind": kind,
                "writers_in_slice": inside, "writers_outside": outside,
                "readers_in_slice": sorted(
                    n for n in slice_set
                    if any(x["object"] == obj for x in
                           lineage[n]["reads"] + lineage[n]["reads_via_procs"])),
            }
    return inputs


def classify_outputs(slice_set, lineage, readers, writers, prefixes):
    outputs = {}
    for name in slice_set:
        data = lineage[name]
        for item in data["writes"] + data["writes_via_procs"]:
            obj = item["object"]
            if is_plumbing(obj, prefixes) or obj in outputs:
                continue
            outside_readers = sorted(readers.get(obj, set()) - slice_set)
            outside_writers = sorted(writers.get(obj, set()) - slice_set)
            if outside_writers:
                kind = "co-written"
            elif outside_readers:
                kind = "downstream-consumed"
            else:
                kind = "internal"
            outputs[obj] = {
                "object": obj, "layer": item["layer"], "kind": kind,
                "writers_in_slice": sorted(writers.get(obj, set()) & slice_set),
                "writers_outside": outside_writers,
                "readers_outside": outside_readers,
            }
    return outputs


def table(lines, header, rows):
    lines.append("| " + " | ".join(header) + " |")
    lines.append("| " + " | ".join("---" for _ in header) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    lines.append("")


def fmt(items, limit=6):
    items = list(items)
    if not items:
        return "-"
    shown = ", ".join("`%s`" % i for i in items[:limit])
    if len(items) > limit:
        shown += " (+%d)" % (len(items) - limit)
    return shown


INPUT_ACTION = {
    "source": "Ingest directly (Lakehouse Federation / JDBC extract into bronze); "
              "legacy source stays read-only.",
    "internal": "No action; produced inside the wave.",
    "shared-conformed": "Consume as a shared conformed input: mirror the legacy "
                        "table into the shared `ref`/`dim` schema (read-only "
                        "sync from SQL Server DW) until its owning wave migrates.",
    "upstream-leak": "Closure gap: either pull the writer package into wave 1 or "
                     "freeze the table as a synced input and record the "
                     "dependency in the wave manifest.",
    "co-written": "Contention: the object is written both inside and outside the "
                  "slice. Split ownership before cutover or migrate both writers.",
    "orphan-input": "Read by the slice but written by no package in the estate. "
                    "Verify on the live server (view? manual load? dead code?) "
                    "before design freeze.",
}
OUTPUT_ACTION = {
    "internal": "Owned outright; publish only in the migration catalog.",
    "downstream-consumed": "Keep the legacy table populated during coexistence "
                           "(reverse-sync Delta -> SQL Server after each run) "
                           "until every listed reader has migrated.",
    "co-written": "Another package also writes this object; the wave cannot own "
                  "it. Reassign or split the writers.",
}


def run(args):
    report = lib.Report("carve_wave_slice")
    wave = load_wave(args.wave)
    with open(LINEAGE_PATH) as handle:
        lineage = json.load(handle)["packages"]
    with open(PLAN_PATH) as handle:
        plan = json.load(handle)
    prefixes = tuple(wave["plumbing_prefixes"])
    shared_layers = set(wave["shared_layers"])
    slice_set = set(wave["packages"])

    missing = sorted(slice_set - set(lineage))
    for name in missing:
        report.error(CHECK, name, "wave package has no lineage entry")
    slice_set -= set(missing)

    writers, readers, layers, execs = index_lineage(lineage, prefixes)
    membership, phases = plan_membership(plan)
    inputs = classify_inputs(slice_set, lineage, writers, layers,
                             shared_layers, prefixes)
    outputs = classify_outputs(slice_set, lineage, readers, writers, prefixes)
    full_closure = upstream_closure(slice_set, lineage, writers, layers,
                                    shared_layers | {"etl-control"}, prefixes)
    pull_in = sorted(full_closure - slice_set)

    input_kinds = Counter(item["kind"] for item in inputs.values())
    output_kinds = Counter(item["kind"] for item in outputs.values())
    for item in inputs.values():
        if item["kind"] in ("upstream-leak", "co-written"):
            report.warn(CHECK, item["object"],
                        "%s: written outside the slice by %s" % (
                            item["kind"], ", ".join(item["writers_outside"])))
        elif item["kind"] == "orphan-input":
            report.warn(CHECK, item["object"], "read by the slice, written by no package")
    for item in outputs.values():
        if item["kind"] == "co-written":
            report.warn(CHECK, item["object"], "also written by %s" %
                        ", ".join(item["writers_outside"]))

    shared_execs = Counter()
    unresolved = Counter()
    for name in slice_set:
        for proc in execs[name]:
            shared_execs[proc] += 1
        for proc in lineage[name]["unresolved_procs"]:
            unresolved[proc] += 1
    for proc in sorted(unresolved):
        report.warn(CHECK, proc, "procedure body not in repo; called by %d slice package(s)"
                    % unresolved[proc])

    phase_rows = []
    touched_phases = set()
    for name in sorted(slice_set):
        for key in membership.get(name, []):
            touched_phases.add(key)
    for key in sorted(touched_phases):
        members = phases[key]
        inside = [m for m in members if m in slice_set]
        outside = [m for m in members if m not in slice_set]
        phase_rows.append((key[0], key[1], fmt(inside), fmt(outside)))
    uninvoked = sorted(n for n in slice_set if not membership.get(n))

    report.count("slice_packages", len(slice_set))
    report.count("inputs", len(inputs))
    for kind, number in sorted(input_kinds.items()):
        report.count("inputs_%s" % kind.replace("-", "_"), number)
    report.count("outputs", len(outputs))
    for kind, number in sorted(output_kinds.items()):
        report.count("outputs_%s" % kind.replace("-", "_"), number)
    report.count("pull_in_candidates", len(pull_in))
    report.count("plan_phases_touched", len(touched_phases))
    report.count("shared_procs", len(shared_execs))
    report.count("unresolved_procs", len(unresolved))
    report.detail("pull_in_candidates", pull_in)
    report.detail("upstream_leaks", sorted(
        o for o, i in inputs.items() if i["kind"] in ("upstream-leak", "co-written")))
    report.detail("orphan_inputs", sorted(
        o for o, i in inputs.items() if i["kind"] == "orphan-input"))
    report.detail("downstream_consumers", sorted({
        r for i in outputs.values() for r in i["readers_outside"]}))

    if not args.no_write:
        write_report(args.output, wave, slice_set, lineage, inputs, outputs,
                     pull_in, phase_rows, uninvoked, shared_execs, unresolved,
                     report)
    return report.emit(as_json=args.json, strict=args.strict,
                       show_warnings=not args.quiet)


def write_report(path, wave, slice_set, lineage, inputs, outputs, pull_in,
                 phase_rows, uninvoked, shared_execs, unresolved, report):
    lines = [
        "# Wave %s closure check: %s" % (wave["wave"], wave["title"]),
        "",
        "Generated by `validation/checks/carve_wave_slice.py` from "
        "`docs/inventories/ssis-sql-lineage.json` and `ssis/orchestration-plan.json`. "
        "Static only. Control-framework (`etl.*`) and reject (`err.*`) objects are "
        "treated as plumbing and listed separately.",
        "",
        wave["description"].strip(),
        "",
        "## Slice packages (%d)" % len(slice_set),
        "",
    ]
    rows = []
    for name in sorted(slice_set):
        data = lineage[name]
        rows.append((name, data["folder"], data["load_type"],
                     fmt([x["object"] for x in data["writes"] + data["writes_via_procs"]
                          if not x["object"].startswith(("ETL.", "ERR."))], 4)))
    table(lines, ("Package", "Folder", "Load type", "Writes"), rows)

    lines.extend(["## Verdict", ""])
    leaks = [i for i in inputs.values() if i["kind"] in ("upstream-leak", "co-written")]
    orphans = [i for i in inputs.values() if i["kind"] == "orphan-input"]
    cowritten_out = [o for o in outputs.values() if o["kind"] == "co-written"]
    consumed = [o for o in outputs.values() if o["kind"] == "downstream-consumed"]
    lines.append("- Upstream leaks (non-shared inputs written or co-written outside the slice): **%d**" % len(leaks))
    lines.append("- Orphan inputs (read, never written by any package): **%d**" % len(orphans))
    lines.append("- Outputs co-written from outside the slice: **%d**" % len(cowritten_out))
    lines.append("- Outputs consumed outside the slice (need coexistence sync): **%d**" % len(consumed))
    lines.append("- Packages to pull in for a fully closed upstream: **%d** (%s)" % (
        len(pull_in), fmt(pull_in, 12)))
    lines.append("- Unresolved procedures: **%d**" % len(unresolved))
    lines.append("")

    for kind in ("upstream-leak", "co-written", "orphan-input", "shared-conformed",
                 "source", "internal"):
        items = sorted((i for i in inputs.values() if i["kind"] == kind),
                       key=lambda i: i["object"])
        if not items:
            continue
        lines.extend(["## Inputs: %s (%d)" % (kind, len(items)), "",
                      INPUT_ACTION[kind], ""])
        table(lines, ("Object", "Layer", "Written by (outside)", "Read by (in slice)"),
              [(i["object"], i["layer"], fmt(i["writers_outside"]),
                fmt(i["readers_in_slice"])) for i in items])

    for kind in ("co-written", "downstream-consumed", "internal"):
        items = sorted((o for o in outputs.values() if o["kind"] == kind),
                       key=lambda o: o["object"])
        if not items:
            continue
        lines.extend(["## Outputs: %s (%d)" % (kind, len(items)), "",
                      OUTPUT_ACTION[kind], ""])
        table(lines, ("Object", "Layer", "Written by (in slice)", "Other writers",
                      "Read by (outside)"),
              [(o["object"], o["layer"], fmt(o["writers_in_slice"]),
                fmt(o["writers_outside"]), fmt(o["readers_outside"]))
               for o in items])

    lines.extend(["## Orchestration phases touched", "",
                  "Legacy plan phases that contain a slice package. Out-of-slice "
                  "packages in the same phase keep running under the legacy master "
                  "during coexistence; the slice's steps are removed from the phase "
                  "at cutover.", ""])
    table(lines, ("Root", "Phase", "Slice packages", "Other packages"), phase_rows)
    if uninvoked:
        lines.append("Slice packages not invoked by any plan phase: %s" % fmt(uninvoked, 20))
        lines.append("")

    lines.extend(["## Shared procedures executed by the slice", ""])
    table(lines, ("Procedure", "Slice packages calling", "Body in repo"),
          [(proc, number, "no" if proc in unresolved else "yes")
           for proc, number in sorted(shared_execs.items())])

    lines.extend(["## Summary counts", "", "```json",
                  json.dumps(report.counts, indent=2, sort_keys=True), "```", ""])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as handle:
        handle.write("\n".join(lines))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    parser.add_argument("--wave", required=True, help="wave definition yaml")
    parser.add_argument("--output", default=None,
                        help="markdown report path (default docs/waves/<name>-closure.md)")
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args(argv)
    if args.output is None:
        wave = load_wave(args.wave)
        args.output = os.path.join(
            lib.REPO_ROOT, "docs", "waves",
            "wave-%02d-%s-closure.md" % (int(wave["wave"]), wave["name"]))
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
