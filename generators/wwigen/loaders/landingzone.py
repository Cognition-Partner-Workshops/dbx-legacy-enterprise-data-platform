"""Staging of the generated file feeds into the landing zone.

A feed is not loaded by the generator's own loaders: the ingestion packages
pick it up from the landing zone, so what the generator has to get right is the
directory, the filename and the encoding. Those come from
``config/landing-zone.yaml`` through :mod:`wwigen.landing`.

The generated files stay in the run's output directory, which is where the
manifest and the determinism check see them. This module emits the copy step
that puts them where the ingestion packages look, under ``WWI_LANDING_ROOT``
(``C:\\WWI\\DEV`` in DEV), together with a manifest of what goes where.
"""

from __future__ import annotations

import os

from .. import landing, schema


def placements(cfg, specs) -> list:
    """[(spec, feed, path beneath the landing root)] for the file feeds."""
    placed = []
    for spec in sorted(specs, key=lambda item: item.key):
        if spec.system != schema.FILE_FEED:
            continue
        feed = landing.feed(spec.name)
        if feed is None:
            raise ValueError("%s is not a feed in config/landing-zone.yaml" % spec.name)
        placed.append((spec, feed, feed.relative_path(cfg.snapshot_date)))
    return placed


SCRIPT_HEADER = '''#Requires -Version 5.1
<#
.SYNOPSIS
    Stage the generated file feeds into the landing zone.

.DESCRIPTION
    Copies each generated feed to the directory and filename
    config/landing-zone.yaml declares for it, beneath the landing root:

        WWI_LANDING_ROOT   (DEV: {default_root})

    The feeds are copied, not moved, so a run can be staged again without
    regenerating. Nothing here processes a file; the ingestion packages do
    that, and they are not run by the generator.
#>
[CmdletBinding()]
param(
    # Landing root. Defaults to $env:{root_variable}, then to the DEV root.
    [string] $LandingRoot,
    # Print the placements without copying.
    [switch] $ListOnly
)

$ErrorActionPreference = 'Stop'

if (-not $LandingRoot) {{ $LandingRoot = $env:{root_variable} }}
if (-not $LandingRoot) {{ $LandingRoot = '{default_root}' }}

$DataRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '{data_root}')).Path

$Feeds = @(
'''

SCRIPT_FOOTER = ''')

foreach ($feed in $Feeds) {
    $source = Join-Path $DataRoot $feed.Source
    if (-not (Test-Path -LiteralPath $source)) {
        throw "generated feed not found: $source"
    }
    $destination = Join-Path $LandingRoot $feed.Target
    if ($ListOnly) {
        '{0,-22} {1}' -f $feed.Name, $destination
        continue
    }
    $directory = Split-Path -Parent $destination
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    Copy-Item -LiteralPath $source -Destination $destination -Force
    Write-Host ("staged {0} -> {1}" -f $feed.Name, $destination)
}

Write-Host 'landing zone staged'
'''

DATA_ROOT_RELATIVE = "..\\..\\data"


def script_text(cfg, specs) -> str:
    header = SCRIPT_HEADER.format(default_root=landing.DEFAULT_ROOT,
                                  root_variable=landing.ROOT_VARIABLE,
                                  data_root=DATA_ROOT_RELATIVE)
    entries = ["    @{ Name = '%s'; Source = '%s'; Target = '%s' }"
               % (spec.name, spec.relative_path().replace("/", "\\"), target)
               for spec, _feed, target in placements(cfg, specs)]
    return header + "\n".join(entries) + "\n" + SCRIPT_FOOTER


def manifest_text(cfg, specs) -> str:
    lines = ["feed,generated_file,landing_path,encoding,delimiter,raw_table,consumed_by"]
    for spec, feed, target in placements(cfg, specs):
        delimiter = {"\t": "\\t"}.get(feed.delimiter, feed.delimiter)
        lines.append(",".join([
            spec.name, "data/" + spec.relative_path(), target.replace("\\", "/"),
            feed.encoding, '"%s"' % delimiter, feed.raw_table, feed.consumed_by]))
    return "\n".join(lines) + "\n"


def emit(cfg, specs) -> list:
    """Write the staging script and the feed manifest."""
    feeds = [spec for spec in specs if spec.system == schema.FILE_FEED]
    if not feeds:
        return []
    root = os.path.join(cfg.output_dir, "loaders", "landing")
    os.makedirs(root, exist_ok=True)
    written = []
    for name, text in (("Stage-LandingZone.ps1", script_text(cfg, specs)),
                       ("feeds.csv", manifest_text(cfg, specs))):
        path = os.path.join(root, name)
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
        written.append(os.path.relpath(path, cfg.output_dir).replace(os.sep, "/"))
    return sorted(written)
