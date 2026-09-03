"""Run manifest and resume markers.

Every generated table writes a per-table marker recording the run signature
(scale, seed, configuration version, history window), the output file, the row
count and the file checksum. Two things are built on those markers:

* the run manifest, a single JSON document describing everything produced,
  which is what a downstream loader reads to know what to load;
* resumability - a table whose marker matches the current run signature is
  skipped, so an interrupted ``large`` run continues where it stopped instead
  of restarting.
"""

from __future__ import annotations

import json
import os

MARKER_DIRNAME = "_markers"
MANIFEST_FILENAME = "manifest.json"


def marker_dir(output_dir: str) -> str:
    return os.path.join(output_dir, MARKER_DIRNAME)


def marker_path(output_dir: str, table_key: str) -> str:
    return os.path.join(marker_dir(output_dir), table_key.replace("/", "_") + ".json")


def read_marker(output_dir: str, table_key: str):
    path = marker_path(output_dir, table_key)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (ValueError, OSError):
        return None


def write_marker(output_dir: str, table_key: str, payload: dict) -> None:
    os.makedirs(marker_dir(output_dir), exist_ok=True)
    with open(marker_path(output_dir, table_key), "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def marker_is_current(marker, signature: dict, spec_fingerprint: str) -> bool:
    if not marker:
        return False
    if marker.get("status") != "complete":
        return False
    if marker.get("signature") != signature:
        return False
    return marker.get("spec_fingerprint") == spec_fingerprint


def collect(output_dir: str) -> list:
    directory = marker_dir(output_dir)
    if not os.path.isdir(directory):
        return []
    entries = []
    for filename in sorted(os.listdir(directory)):
        if not filename.endswith(".json"):
            continue
        with open(os.path.join(directory, filename), encoding="utf-8") as handle:
            entries.append(json.load(handle))
    return sorted(entries, key=lambda entry: entry["table"])


def write_manifest(output_dir: str, signature: dict, entries: list,
                   loader_files: dict) -> str:
    total_rows = sum(int(entry.get("rows", 0)) for entry in entries)
    total_bytes = sum(int(entry.get("bytes", 0)) for entry in entries)
    document = {
        "manifest_version": 2,
        "signature": signature,
        "totals": {
            "tables": len(entries),
            "rows": total_rows,
            "bytes": total_bytes,
        },
        "by_system": _totals_by(entries, "system"),
        "by_group": _totals_by(entries, "group"),
        "loader_artifacts": loader_files,
        "tables": entries,
    }
    path = os.path.join(output_dir, MANIFEST_FILENAME)
    os.makedirs(output_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(document, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return path


def _totals_by(entries: list, field: str) -> dict:
    totals = {}
    for entry in entries:
        key = entry.get(field) or "unknown"
        bucket = totals.setdefault(key, {"tables": 0, "rows": 0})
        bucket["tables"] += 1
        bucket["rows"] += int(entry.get("rows", 0))
    return dict(sorted(totals.items()))
