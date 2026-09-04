"""Declared bindings between the producers and the deployed schema.

:func:`conform_spec` is the single place a raw producer spec becomes the spec
the estate can actually load: Oracle extracts are projected onto the tables in
``oracle/tables``, SQL Server extracts onto the raw landing tables in
``sqlserver/staging/tables``, and file feeds onto the feed declared in
``config/landing-zone.yaml``, whose contract is a filename, an encoding and a
delimiter rather than a column list.
"""

from __future__ import annotations

import dataclasses

from .. import conform, landing, schema
from . import oracle_map, sql_map

UNLANDED_TAG = "unlanded"


def conform_spec(spec: schema.TableSpec) -> schema.TableSpec:
    """The loadable form of one producer spec."""
    if spec.system == schema.ORACLE:
        key = "%s.%s" % (spec.schema, spec.name)
        return conform.conform_oracle_spec(spec, oracle_map.RULES.get(key))
    if spec.system == schema.SQLSERVER:
        target = sql_map.TARGETS.get(spec.key)
        if target is None:
            return dataclasses.replace(
                spec, target_object="",
                tags=tuple(spec.tags) + (UNLANDED_TAG,))
        return conform.conform_sql_spec(spec, target, sql_map.RULES.get(spec.key))
    if spec.system == schema.FILE_FEED:
        return conform_feed(spec)
    return spec


def conform_feed(spec: schema.TableSpec) -> schema.TableSpec:
    """The feed as the landing zone declares it, not as the producer assumed."""
    feed = landing.feed(spec.name)
    if feed is None:
        raise conform.ContractError(
            "%s is not a feed in config/landing-zone.yaml" % spec.name)
    return dataclasses.replace(
        spec, delimiter=feed.delimiter, encoding=feed.codec,
        header=feed.header, extension=feed.extension,
        target_object=feed.raw_table or spec.target_object)


def is_landed(spec: schema.TableSpec) -> bool:
    """Whether the extract has a table to be loaded into."""
    return UNLANDED_TAG not in tuple(spec.tags) and bool(spec.target_object)


def conform_all(specs) -> tuple:
    return tuple(conform_spec(spec) for spec in specs)
