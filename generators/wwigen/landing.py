"""The file-feed contract, read from ``config/landing-zone.yaml``.

A file feed has no table to conform to: what makes it correct is the directory
it is dropped in, the name it is dropped under, and the encoding and delimiter
the ingestion package reads it with. That contract is the landing-zone
document, so the generator reads it rather than restating it.

The document is also what resolves the landing root: everything is relative to
``WWI_LANDING_ROOT``, and the generated files are staged beneath it - never
beside the loaders and never at an absolute path baked in at generation time.
"""

from __future__ import annotations

import os

import yaml

from . import canon

DOCUMENT = os.path.join(canon.REPO_ROOT, "config", "landing-zone.yaml")

DEFAULT_ROOT = "C:\\WWI\\DEV"
ROOT_VARIABLE = "WWI_LANDING_ROOT"

# The encodings the document names, as Python codecs.
CODECS = {
    "windows-1252": "cp1252",
    "iso-8859-1": "latin-1",
    "latin-1": "latin-1",
    "utf-8": "utf-8",
    "utf-8-bom": "utf-8-sig",
}


class Feed:
    """One feed as the landing zone declares it."""

    def __init__(self, name, directory, pattern, encoding, delimiter,
                 header, raw_table, consumed_by):
        self.name = name
        self.directory = directory          # relative to the landing root
        self.pattern = pattern
        self.encoding = encoding            # as written in the document
        self.delimiter = delimiter
        self.header = header
        self.raw_table = raw_table
        self.consumed_by = consumed_by

    @property
    def codec(self) -> str:
        codec = CODECS.get(self.encoding)
        if codec is None:
            raise ValueError("%s: unknown encoding %r" % (self.name, self.encoding))
        return codec

    @property
    def extension(self) -> str:
        return self.pattern.rsplit(".", 1)[-1]

    def filename(self, snapshot_date, sequence: int = 1) -> str:
        """The name a file of this feed arrives under, for one drop."""
        return (self.pattern
                .replace("{yyyyMMdd}", snapshot_date.strftime("%Y%m%d"))
                .replace("{yyyyMM}", snapshot_date.strftime("%Y%m"))
                .replace("{seq3}", "%03d" % sequence))

    def relative_path(self, snapshot_date, sequence: int = 1) -> str:
        """The path beneath the landing root, in Windows form."""
        return "%s\\%s" % (self.directory.replace("/", "\\"),
                           self.filename(snapshot_date, sequence))


_CACHE = {}


def feeds() -> dict:
    """{feed name: Feed} for every feed the landing zone declares."""
    if _CACHE.get("root") == canon.REPO_ROOT:
        return _CACHE["feeds"]
    with open(DOCUMENT, encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    found = {}
    for directory in document.get("directories", {}).values():
        for entry in directory.get("subdirectories", []) or []:
            for name in entry.get("feeds", []):
                found[name] = Feed(
                    name=name,
                    directory=entry["path"],
                    pattern=entry["pattern"],
                    encoding=str(entry.get("encoding", "utf-8")).lower(),
                    delimiter=entry.get("delimiter", "|"),
                    header=bool(entry.get("header", False)),
                    raw_table=entry.get("raw_table", ""),
                    consumed_by=entry.get("consumed_by", ""))
    _CACHE.update(root=canon.REPO_ROOT, feeds=found)
    return found


def feed(name: str):
    return feeds().get(name)


def reset_cache():
    _CACHE.clear()
