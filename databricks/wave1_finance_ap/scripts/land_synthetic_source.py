#!/usr/bin/env python3
"""Land the synthetic Oracle extracts for wave 1 in the UC landing volume.

Runs the repo's deterministic generator (``generators/generate.py``) for the
Oracle tables wave 1 reads, writes one column contract per table from the same
registry the generator used, and copies both into the volume with the Databricks
CLI. Standard library only; auth comes from DATABRICKS_HOST / DATABRICKS_TOKEN
in the environment.

    python3 scripts/land_synthetic_source.py --scale medium \
        --volume-path /Volumes/de_demo_workspace/wave1_landing/oracle_extract

Volume layout::

    oracle/_contracts/<SCHEMA>.<TABLE>.json
    oracle/<SCHEMA>/<TABLE>/<TABLE>.dat
    oracle/_manifest/manifest.json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
GENERATORS = os.path.join(REPO, "generators")
sys.path.insert(0, GENERATORS)

from wwigen import manifest as wwimanifest  # noqa: E402
from wwigen import tables  # noqa: E402

# WWI_REF.CURRENCY_CODE is deliberately absent: the generator emits it empty at
# every scale, and wave 1 only needs FX_RATE_DAILY from the currency reference.
WAVE1_TABLES = (
    "WWI_REF.PAYMENT_METHOD_REF", "WWI_REF.FX_RATE_DAILY",
    "WWI_MDM.SUPP_MASTER", "WWI_MDM.SUPP_ADDRESS", "WWI_MDM.SUPP_BANK_ACCOUNT",
    "WWI_FIN.PAYMENT_TERMS", "WWI_FIN.TAX_RATE", "WWI_FIN.COST_CENTER",
    "WWI_FIN.AP_INVOICE_HDR", "WWI_FIN.AP_INVOICE_LINE", "WWI_FIN.AP_INVOICE_HOLD",
    "WWI_FIN.AP_PAYMENT", "WWI_FIN.AP_PAYMENT_APPLY", "WWI_FIN.AP_AGING_SNAPSHOT",
)


def run(cmd, **kw):
    print("+", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True, **kw)


def generate(scale: str, output: str, resume: bool, parallel: int) -> None:
    """Run the generator, one process per table when ``parallel`` > 1.

    Every producer re-harvests its parents' keyspaces (a lines table replays the
    whole header table, and the PO tables it references), so the AP tables are
    minutes each at medium scale and embarrassingly parallel. Parts are
    generated into ``<output>/parts/<TABLE>`` and merged afterwards.
    """
    base = [sys.executable, os.path.join(GENERATORS, "generate.py"),
            "--scale", scale, "--no-loaders", "--quiet"] + (["--resume"] if resume else [])
    if parallel <= 1:
        cmd = base + ["--output", output]
        for t in WAVE1_TABLES:
            cmd += ["--only", t]
        run(cmd)
        return
    procs = {}
    pending = list(WAVE1_TABLES)
    while pending or procs:
        while pending and len(procs) < parallel:
            t = pending.pop(0)
            part = os.path.join(output, "parts", t)
            cmd = base + ["--output", part, "--only", t]
            print("+", " ".join(cmd), flush=True)
            procs[t] = subprocess.Popen(cmd)
        for t, p in list(procs.items()):
            if p.poll() is not None:
                if p.returncode:
                    raise SystemExit("generator failed for %s (rc=%d)" % (t, p.returncode))
                del procs[t]
        if procs:
            time.sleep(5)
    merge_parts(output)


def merge_parts(output: str) -> None:
    """Fold ``<output>/parts/*`` into ``<output>`` and rebuild manifest.json."""
    parts_dir = os.path.join(output, "parts")
    for part in sorted(os.listdir(parts_dir)):
        root = os.path.join(parts_dir, part)
        for dirpath, _dirs, files in os.walk(os.path.join(root, "data")):
            for f in files:
                src = os.path.join(dirpath, f)
                dst = os.path.join(output, os.path.relpath(src, root))
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
        markers = wwimanifest.marker_dir(root)
        target_markers = wwimanifest.marker_dir(output)
        os.makedirs(target_markers, exist_ok=True)
        for f in os.listdir(markers):
            shutil.copy2(os.path.join(markers, f), os.path.join(target_markers, f))
    entries = [e for e in wwimanifest.collect(output)
               if e.get("status") == "complete" and e["table"].split(".", 1)[1] in WAVE1_TABLES]
    if not entries:
        raise SystemExit("no completed tables under %s" % parts_dir)
    signatures = {json.dumps(e["signature"], sort_keys=True) for e in entries}
    if len(signatures) != 1:
        raise SystemExit("parts were generated with different seeds/scales: %s" % sorted(signatures))
    wwimanifest.write_manifest(output, entries[0]["signature"], entries, {})


def write_contracts(output: str, scale: str, manifest: dict) -> str:
    contract_dir = os.path.join(output, "contracts")
    os.makedirs(contract_dir, exist_ok=True)
    rows_by_key = {e["table"]: e.get("rows", 0) for e in manifest.get("tables", [])}
    specs = tables.by_key()
    for qualified in WAVE1_TABLES:
        spec = specs["oracle." + qualified]
        contract = {
            "table": qualified,
            "system": "oracle",
            "target_object": spec.target_object,
            "delimiter": spec.delimiter,
            "header": spec.header,
            "encoding": spec.encoding,
            "extension": spec.extension,
            "generator_scale": scale,
            "rows": rows_by_key.get(spec.key, 0),
            "columns": [
                {"name": c.name, "type": c.type, "length": c.length,
                 "precision": c.precision, "scale": c.scale, "nullable": c.nullable}
                for c in spec.columns
            ],
        }
        with open(os.path.join(contract_dir, qualified + ".json"), "w") as fh:
            json.dump(contract, fh, indent=2)
    return contract_dir


def upload(output: str, contract_dir: str, volume_path: str) -> None:
    run(["databricks", "fs", "cp", "--overwrite", "--recursive", contract_dir,
         os.path.join("dbfs:" + volume_path, "oracle", "_contracts")])
    data_root = os.path.join(output, "data", "oracle")
    for schema in sorted(os.listdir(data_root)):
        for fname in sorted(os.listdir(os.path.join(data_root, schema))):
            table = os.path.splitext(fname)[0]
            if "%s.%s" % (schema, table) not in WAVE1_TABLES:
                continue
            table_dir = os.path.join("dbfs:" + volume_path, "oracle", schema, table)
            run(["databricks", "fs", "mkdir", table_dir])
            run(["databricks", "fs", "cp", "--overwrite",
                 os.path.join(data_root, schema, fname), os.path.join(table_dir, fname)])
    manifest_dir = os.path.join("dbfs:" + volume_path, "oracle", "_manifest")
    run(["databricks", "fs", "mkdir", manifest_dir])
    run(["databricks", "fs", "cp", "--overwrite", os.path.join(output, "manifest.json"),
         os.path.join(manifest_dir, "manifest.json")])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--scale", default="medium", choices=("small", "medium", "large"))
    parser.add_argument("--output", default=os.path.join(os.path.expanduser("~"), "wave1_landing", "medium"),
                        help="local generator output dir (git-ignored; never inside the repo)")
    parser.add_argument("--volume-path", required=True,
                        help="/Volumes/<catalog>/<schema>/<volume> the bundle's landing_path points at")
    parser.add_argument("--skip-generate", action="store_true", help="reuse files already in --output")
    parser.add_argument("--resume", action="store_true", help="pass --resume to the generator")
    parser.add_argument("--parallel", type=int, default=max(1, (os.cpu_count() or 2) - 1),
                        help="generator processes to run at once (1 = single run)")
    parser.add_argument("--merge-only", action="store_true",
                        help="only merge existing <output>/parts into <output>")
    args = parser.parse_args()

    if args.merge_only:
        merge_parts(args.output)
    elif not args.skip_generate:
        generate(args.scale, args.output, args.resume, args.parallel)
    with open(os.path.join(args.output, "manifest.json")) as fh:
        manifest = json.load(fh)
    contract_dir = write_contracts(args.output, args.scale, manifest)
    upload(args.output, contract_dir, args.volume_path)
    print("landed %d tables at %s" % (len(WAVE1_TABLES), args.volume_path))


if __name__ == "__main__":
    main()
