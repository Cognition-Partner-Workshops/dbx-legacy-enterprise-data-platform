"""The table registry: every object the generator can produce.

Each module in this package declares its own :class:`TableSpec` tuple. The
registry is the concatenation, ordered so that a full run writes masters
before the transactions that reference them - useful for a human reading the
output, and required by the load order in the emitted loader scripts.
"""

from __future__ import annotations

from . import (file_feeds, oracle_fin, oracle_mdm, oracle_proc, oracle_ref,
               sqlserver_ops, sqlserver_sales)

MODULES = (oracle_ref, oracle_mdm, oracle_proc, oracle_fin,
           sqlserver_sales, sqlserver_ops, file_feeds)


def all_specs() -> tuple:
    specs = []
    for module in MODULES:
        specs.extend(module.SPECS)
    keys = [spec.key for spec in specs]
    duplicates = sorted({key for key in keys if keys.count(key) > 1})
    if duplicates:
        raise RuntimeError("duplicate table keys in the registry: %s" % ", ".join(duplicates))
    return tuple(specs)


def by_key() -> dict:
    return {spec.key: spec for spec in all_specs()}


def groups() -> tuple:
    return tuple(sorted({spec.group for spec in all_specs()}))


def select(only=(), groups_wanted=(), systems=()) -> tuple:
    """Filter the registry for ``--only`` / ``--group`` / ``--system``."""
    specs = all_specs()
    if systems:
        specs = tuple(spec for spec in specs if spec.system in systems)
    if groups_wanted:
        specs = tuple(spec for spec in specs if spec.group in groups_wanted)
    if only:
        wanted = set(only)
        matched = tuple(spec for spec in specs
                        if spec.key in wanted or spec.qualified_name in wanted
                        or spec.name in wanted)
        unknown = wanted - {spec.key for spec in matched} \
            - {spec.qualified_name for spec in matched} - {spec.name for spec in matched}
        if unknown:
            raise KeyError("unknown table(s): %s" % ", ".join(sorted(unknown)))
        specs = matched
    return specs
