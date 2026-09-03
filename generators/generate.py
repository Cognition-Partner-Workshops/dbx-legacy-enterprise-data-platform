#!/usr/bin/env python3
"""Generate the synthetic estate: Oracle ERP, SQL Server OLTP and file feeds.

Everything is written to files. Nothing in this suite opens a database
connection, and nothing in it needs one to run.

    python3 generators/generate.py --scale small
    python3 generators/generate.py --scale medium --group sqlserver_sales
    python3 generators/generate.py --scale large --resume
    python3 generators/generate.py --self-check

The run is fully determined by ``(seed, scale, config_version)``: the same
triple produces byte-identical files, which is what ``--self-check`` asserts
by generating a slice twice into scratch directories and comparing checksums.

Row generation is streamed. A table's producer is a generator function, the
writer flushes in chunks, and nothing accumulates the estate in memory - so
``large`` is bounded by the chunk size rather than the row count.

Each table writes a resume marker on completion, so ``--resume`` skips tables
already produced by an identical run signature and a partially finished run
can be continued.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from wwigen import config, context, manifest, schema, tables, writers  # noqa: E402
from wwigen.loaders import bulkinsert, sqlloader  # noqa: E402

SELF_CHECK_TABLES = (
    "oracle.WWI_MDM.CUST_MASTER",
    "oracle.WWI_FIN.AP_INVOICE_HDR",
    "sqlserver.Sales.OrderLines",
    "file.landing.partner_sales_eu",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="generate.py",
        description="Deterministic synthetic data generator for the legacy estate.")
    parser.add_argument("--scale", choices=config.SCALE_MODES, default="small",
                        help="row-count profile declared in generators/config/scales.json")
    parser.add_argument("--seed", type=int, default=None,
                        help="override the default seed; changing it changes every value")
    parser.add_argument("--output", default=None,
                        help="output directory (default generators/output/<scale>)")
    parser.add_argument("--only", action="append", default=[], metavar="TABLE",
                        help="generate only this table key or Schema.Name; repeatable")
    parser.add_argument("--group", action="append", default=[], metavar="GROUP",
                        help="generate only this group; repeatable")
    parser.add_argument("--system", action="append", default=[],
                        choices=[schema.ORACLE, schema.SQLSERVER, schema.FILE_FEED],
                        help="restrict to one source system; repeatable")
    parser.add_argument("--resume", action="store_true",
                        help="skip tables already completed by an identical run signature")
    parser.add_argument("--no-loaders", action="store_true",
                        help="skip the SQL*Loader / bcp / BULK INSERT artefacts")
    parser.add_argument("--chunk-rows", type=int, default=50000,
                        help="rows buffered before each flush")
    parser.add_argument("--progress-every", type=int, default=250000,
                        help="progress reporting interval in rows")
    parser.add_argument("--quiet", action="store_true", help="suppress progress output")
    parser.add_argument("--list-tables", action="store_true",
                        help="print the registry and exit without generating")
    parser.add_argument("--self-check", action="store_true",
                        help="generate a small slice twice and compare the checksums")
    return parser


def spec_fingerprint(spec) -> str:
    """Identity of a table's contract, so a column change invalidates a marker."""
    import hashlib
    material = "|".join("%s:%s:%d:%d" % (c.name, c.type, c.length, c.scale)
                        for c in spec.columns)
    material = "%s#%s#%s#%s" % (spec.key, spec.delimiter, spec.encoding, material)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


def write_table(cfg, ctx, spec, stream, progress_every: int, quiet: bool) -> dict:
    """Stream one table to its extract file and return its manifest entry."""
    path = os.path.join(cfg.output_dir, "data", spec.relative_path())
    reporter = writers.ProgressReporter(stream, spec.qualified_name,
                                        every=progress_every, quiet=quiet)
    column_count = len(spec.columns)
    with writers.DelimitedWriter(path, spec, chunk_rows=cfg.chunk_rows) as writer:
        for row in spec.produce(cfg, ctx):
            if isinstance(row, bytes):
                # A feed row with a deliberately broken encoding.
                writer.write_raw(row)
            elif spec.allows_defects and len(row) != column_count:
                # A feed row with the wrong field count; write it as it arrived.
                writer.write_raw(spec.delimiter.join(
                    "" if value is None else str(value) for value in row))
            else:
                writer.write(row)
            reporter.tick(writer.rows_written)
        result = writer.close()
    reporter.done(result["rows"])
    return {
        "table": spec.key,
        "qualified_name": spec.qualified_name,
        "system": spec.system,
        "schema": spec.schema,
        "name": spec.name,
        "group": spec.group,
        "target_object": spec.target_object,
        "file": "data/" + spec.relative_path(),
        "delimiter": spec.delimiter,
        "encoding": spec.encoding,
        "header": spec.header,
        "rows": result["rows"],
        "bytes": result["bytes"],
        "sha256": result["sha256"],
        "scale": cfg.scale,
        "seed": cfg.seed,
        "status": "complete",
        "spec_fingerprint": spec_fingerprint(spec),
        "signature": cfg.signature(),
    }


def generate(cfg, specs, stream, resume: bool = False, emit_loaders: bool = True,
             progress_every: int = 250000, quiet: bool = False) -> dict:
    signature = cfg.signature()
    if not quiet:
        stream.write("scale=%s seed=%d tables=%d output=%s\n"
                     % (cfg.scale, cfg.seed, len(specs), cfg.output_dir))
    for index, spec in enumerate(specs, start=1):
        fingerprint = spec_fingerprint(spec)
        marker = manifest.read_marker(cfg.output_dir, spec.key)
        data_path = os.path.join(cfg.output_dir, "data", spec.relative_path())
        if resume and manifest.marker_is_current(marker, signature, fingerprint) \
                and os.path.exists(data_path):
            if not quiet:
                stream.write("[%3d/%3d] %-44s skipped (resume)\n"
                             % (index, len(specs), spec.qualified_name))
            continue
        if not quiet:
            stream.write("[%3d/%3d] %s\n" % (index, len(specs), spec.qualified_name))
        manifest.write_marker(cfg.output_dir, spec.key, {
            "table": spec.key, "status": "running", "signature": signature,
            "spec_fingerprint": fingerprint,
        })
        entry = write_table(cfg, ctx_for(cfg), spec, stream, progress_every, quiet)
        manifest.write_marker(cfg.output_dir, spec.key, entry)

    loader_files = {"oracle": [], "sqlserver": []}
    if emit_loaders:
        all_specs = tables.all_specs()
        loader_files["oracle"] = sqlloader.emit(cfg.output_dir, all_specs)
        loader_files["sqlserver"] = bulkinsert.emit(cfg.output_dir, all_specs)

    entries = manifest.collect(cfg.output_dir)
    path = manifest.write_manifest(cfg.output_dir, signature, entries, loader_files)
    if not quiet:
        total = sum(int(entry.get("rows", 0)) for entry in entries)
        stream.write("manifest: %s (%d tables, %d rows)\n" % (path, len(entries), total))
    return {"manifest": path, "tables": len(entries),
            "rows": sum(int(entry.get("rows", 0)) for entry in entries)}


_CONTEXTS = {}


def ctx_for(cfg):
    """One RunContext per configuration; it is read-only and safe to share."""
    key = (cfg.scale, cfg.seed, cfg.config_version)
    if key not in _CONTEXTS:
        _CONTEXTS[key] = context.RunContext(cfg)
    return _CONTEXTS[key]


def self_check(stream, seed=None) -> int:
    """Generate the same slice twice and compare every file checksum."""
    specs = tables.select(only=SELF_CHECK_TABLES)
    digests = []
    root = tempfile.mkdtemp(prefix="wwigen-selfcheck-")
    try:
        for attempt in ("a", "b"):
            cfg = config.build_run_config("small", seed=seed,
                                          output_dir=os.path.join(root, attempt))
            generate(cfg, specs, stream, emit_loaders=False, quiet=True)
            digests.append({entry["table"]: entry["sha256"]
                            for entry in manifest.collect(cfg.output_dir)})
    finally:
        shutil.rmtree(root, ignore_errors=True)

    mismatches = sorted(table for table in digests[0]
                        if digests[0][table] != digests[1].get(table))
    for table in sorted(digests[0]):
        stream.write("  %-46s %s %s\n"
                     % (table, digests[0][table][:16],
                        "OK" if table not in mismatches else "DIFFERS"))
    if mismatches:
        stream.write("self-check FAILED for: %s\n" % ", ".join(mismatches))
        return 1
    stream.write("self-check passed: %d tables byte-identical across runs\n" % len(digests[0]))
    return 0


def list_tables(specs, stream) -> None:
    stream.write("%-46s %-10s %-22s %s\n" % ("TABLE", "SYSTEM", "GROUP", "ROW COUNT KEY"))
    for spec in specs:
        stream.write("%-46s %-10s %-22s %s\n"
                     % (spec.key, spec.system, spec.group, spec.row_count_key or "-"))
    stream.write("%d tables\n" % len(specs))


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    stream = sys.stdout

    if args.self_check:
        return self_check(stream, seed=args.seed)

    specs = tables.select(only=tuple(args.only), groups_wanted=tuple(args.group),
                          systems=tuple(args.system))
    if not specs:
        stream.write("no tables matched the selection\n")
        return 2
    if args.list_tables:
        list_tables(specs, stream)
        return 0

    cfg = config.build_run_config(args.scale, seed=args.seed, output_dir=args.output,
                                  chunk_rows=args.chunk_rows)
    generate(cfg, specs, stream, resume=args.resume,
             emit_loaders=not args.no_loaders,
             progress_every=args.progress_every, quiet=args.quiet)
    return 0


if __name__ == "__main__":
    sys.exit(main())
