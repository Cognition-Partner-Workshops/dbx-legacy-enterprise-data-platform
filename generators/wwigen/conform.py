"""Project a producer's rows onto the columns the target table actually has.

A producer knows the business content of a row; it does not know the column
vocabulary of the table the row has to land in. This module binds the two:
it reads the canonical contract from :mod:`wwigen.canon`, applies the column
map declared in :mod:`wwigen.contracts`, and returns a spec whose columns are
the target's own columns, in the target's own order, with the target's widths.

Three rules decide whether a canonical column appears in an extract:

* a column the producer supplies a value for is always written;
* a column the table requires - NOT NULL and no DEFAULT - is always written,
  filled deterministically when the producer has nothing to say about it;
* everything else is left out, so the column's DEFAULT applies on load.

A required column that ends up with no value is a generation failure, not a
row to be written and rejected later by the engine.
"""

from __future__ import annotations

import dataclasses
import datetime

from . import canon, regions, rng, schema
from .schema import Column, TableSpec

TECHNICAL_ORACLE = ("SOURCE_SYS", "CREATED_BY", "CREATED_DT",
                    "UPDATED_BY", "UPDATED_DT")

GENERATOR_USER = "WWI_GEN"

# Suffix vocabulary drift between the producers (written against the 2007
# column names) and the schema as deployed. Applied only when the target
# column exists and the producer's own name does not.
SUFFIX_ALIASES = (
    ("_NM", "_NAME"),
    ("_NO", "_NBR"),
    ("_CODE", "_CD"),
    ("_CODE", "_NBR"),
    ("_TS", "_DT"),
    ("_TX", "_TXT"),
    ("_QTY", "_CNT"),
    ("_AMT", ""),
    ("_NM", "_TXT"),
)

# Whole-name drift, in priority order. Same rule as the suffixes: only used
# when the target column exists and nothing else has claimed it.
NAME_ALIASES = {
    "LAST_UPD_BY": ("UPDATED_BY",),
    "LAST_UPD_TS": ("UPDATED_DT",),
    "LAST_UPD_DT": ("UPDATED_DT",),
    "SRC_SYSTEM_CD": ("SOURCE_SYS", "SOURCE_SYS_CD"),
    "EFF_FROM_DT": ("EFFECTIVE_FROM_DT", "EFFECTIVE_DT", "VALID_FROM_DT"),
    "EFF_TO_DT": ("EFFECTIVE_TO_DT", "EXPIRY_DT", "END_DT"),
    "VALID_FROM_DT": ("EFFECTIVE_FROM_DT", "EFFECTIVE_DT"),
    "VALID_TO_DT": ("EFFECTIVE_TO_DT", "EXPIRY_DT", "END_DT"),
    "CITY_NM": ("CITY_TXT", "CITY_NAME"),
}


class ContractError(RuntimeError):
    """A generated extract cannot satisfy its target table."""


def oracle_column(column: canon.OracleColumn) -> Column:
    """The generator's view of one canonical Oracle column."""
    if column.is_date:
        kind = schema.DATE if column.type_name == "DATE" else schema.TIMESTAMP
        return Column(column.name, kind, nullable=column.nullable)
    if column.is_numeric:
        if column.scale:
            return Column(column.name, schema.DECIMAL, precision=column.precision,
                          scale=column.scale, nullable=column.nullable)
        return Column(column.name, schema.INTEGER, length=column.precision or 12,
                      nullable=column.nullable)
    width = column.width or 1
    if width == 1 and column.name.endswith("_FLG"):
        return Column(column.name, schema.FLAG, length=1, nullable=column.nullable)
    return Column(column.name, schema.STRING, length=width, nullable=column.nullable)


def sql_column(column: canon.SqlColumn) -> Column:
    """The generator's view of one canonical SQL Server landing column."""
    name = column.type_name
    if name in ("date",):
        return Column(column.name, schema.DATE, nullable=column.nullable)
    if name in ("datetime", "datetime2", "smalldatetime"):
        return Column(column.name, schema.TIMESTAMP, nullable=column.nullable)
    if name in ("bigint", "int", "smallint", "tinyint"):
        return Column(column.name, schema.INTEGER, length=19, nullable=column.nullable)
    if name in ("decimal", "numeric", "money"):
        return Column(column.name, schema.DECIMAL, precision=19, scale=4,
                      nullable=column.nullable)
    return Column(column.name, schema.STRING, length=column.width or 4000,
                  nullable=column.nullable)


def digits_of(value, fallback: int = 0) -> int:
    """The numeric part of a business code: CUS-0001234 -> 1234."""
    if value is None:
        return fallback
    text = "".join(character for character in str(value) if character.isdigit())
    return int(text) if text else fallback


def surrogate_id(code, occurrence: int) -> int:
    """A surrogate key for a business code, unique per repeat of that code.

    Children derive the same value from the same code, so a foreign key
    resolves without the child having to see the parent's rows.
    """
    base = digits_of(code)
    return base if occurrence == 0 else 900000000 + occurrence * 1000000 + base


class Rule:
    """How one producer's rows map onto one canonical table."""

    def __init__(self, rename=None, drop=(), fill=None, key=None,
                 parents=None, include=(), target=None):
        self.rename = dict(rename or {})
        self.drop = frozenset(drop)
        self.fill = dict(fill or {})
        self.key = key                  # producer column holding the business code
        self.parents = dict(parents or {})  # canonical FK -> producer code column
        self.include = tuple(include)   # optional columns to write anyway
        self.target = target


def code_id(column: str):
    """A surrogate key derived from the business code in ``column``.

    The parent derives its own key the same way, so a child resolves a
    foreign key without ever having to see the parent's rows.
    """
    def resolve(cfg, values, index):
        code = values.get(column)
        return None if code is None else digits_of(code)
    return resolve


def line_id(parent_column: str, line_column: str, factor: int = 1000):
    """A surrogate line key derived from the parent code and the line number."""
    def resolve(cfg, values, index):
        code = values.get(parent_column)
        if code is None:
            return None
        return digits_of(code) * factor + int(values.get(line_column) or 0)
    return resolve


def copy_of(column: str, default=None):
    def resolve(cfg, values, index):
        value = values.get(column)
        return default if value is None else value
    return resolve


def region_of_country(column: str = "COUNTRY_CD", default: str = "NA"):
    """The region a country belongs to, per :mod:`wwigen.regions`.

    Several tables carry the region the row is administered in even though
    the producer only knows the address country; the estate's own country
    catalogue is what resolves one to the other.
    """
    lookup = {profile.code: region
              for region, profiles in regions.COUNTRIES.items()
              for profile in profiles}

    def resolve(cfg, values, index):
        return lookup.get(values.get(column), default)
    return resolve


def by_region(mapping, default=None, column: str = "REGION_CD"):
    """A per-region constant: the regional divergence the estate is built on."""
    def resolve(cfg, values, index):
        return mapping.get(values.get(column), default)
    return resolve


def _alias_candidates(name: str):
    yield name
    for alias in NAME_ALIASES.get(name, ()):
        yield alias
    for suffix, replacement in SUFFIX_ALIASES:
        if name.endswith(suffix) and len(name) > len(suffix):
            yield name[: -len(suffix)] + replacement
    if "CURRENCY" in name:
        yield name.replace("CURRENCY", "CURR")


def resolve_sources(generated, canonical_names, rule: Rule):
    """{canonical column: producer column} plus the producer columns dropped."""
    taken, sources, unmapped = set(), {}, []
    canonical = {name.upper(): name for name in canonical_names}
    for name in generated:
        if name in rule.drop:
            continue
        target = rule.rename.get(name)
        if target is None:
            for candidate in _alias_candidates(name):
                actual = canonical.get(candidate.upper())
                if actual and actual not in taken:
                    target = actual
                    break
        if target is None:
            unmapped.append(name)
            continue
        if target not in canonical.values():
            raise ContractError("mapped column %s does not exist in the target" % target)
        if target in taken:
            raise ContractError("two producer columns map onto %s" % target)
        taken.add(target)
        sources[target] = name
    return sources, unmapped


def _fill_value(column, table, spec_key, index, cfg, values, rule):
    """A deterministic value for a required column the producer does not set."""
    filler = rule.fill.get(column.name)
    if filler is not None:
        return filler(cfg, values, index) if callable(filler) else filler
    name = column.name
    if name in table.primary_key and column.is_numeric:
        return 1 + index
    if name == "SOURCE_SYS":
        return "ORA_ERP"
    if name in ("CREATED_BY", "UPDATED_BY"):
        return GENERATOR_USER
    if name in ("CREATED_DT", "UPDATED_DT"):
        return cfg.history_end
    allowed = table.allowed_values.get(name)
    if allowed:
        return rng.pick(cfg.seed, allowed, spec_key, name, index)
    if column.is_numeric:
        if column.scale:
            return round(1 + rng.unit(cfg.seed, spec_key, name, index) * 100, column.scale)
        return 1 + rng.stable_hash(cfg.seed, spec_key, name, index) % 1000
    if column.is_date:
        span = max((cfg.history_end - cfg.history_start).days, 1)
        return cfg.history_start + datetime.timedelta(
            days=rng.stable_hash(cfg.seed, spec_key, name, index) % span)
    width = column.width or 1
    if width == 1:
        return "Y" if name.endswith("_FLG") else "X"
    token = "%s%04d" % (name.split("_")[0][:3].upper(),
                        rng.stable_hash(cfg.seed, spec_key, name, index) % 10000)
    return token[:width]


def _coerce(value, canonical, where=""):
    """The value as the column can hold it, or a contract failure.

    Truncating to the declared width is a legitimate projection; handing a
    business code to a numeric column is not - that is a mapping defect and
    has to fail at generation rather than at load.
    """
    if value is None:
        return None
    if canonical.is_character:
        text = value if isinstance(value, str) else str(value)
        width = canonical.width
        return text[:width] if width else text
    if getattr(canonical, "is_numeric", False):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            try:
                return int(str(value)) if str(value).lstrip("-").isdigit() \
                    else float(str(value))
            except ValueError:
                raise ContractError(
                    "%s is %s but the extract supplies %r"
                    % (where or canonical.name, canonical.type_name, value))
    if getattr(canonical, "is_date", False) and not isinstance(
            value, (datetime.date, datetime.datetime)):
        raise ContractError("%s is %s but the extract supplies %r"
                            % (where or canonical.name, canonical.type_name, value))
    return value


def conform_oracle_spec(spec: TableSpec, rule: Rule = None) -> TableSpec:
    """Rebuild one Oracle spec against the table the estate actually deploys."""
    rule = rule or Rule()
    tables = canon.oracle_tables()
    key = "%s.%s" % (spec.schema, spec.name)
    table = tables.get(key)
    if table is None:
        raise ContractError("no canonical Oracle table for %s" % key)

    generated = tuple(column.name for column in spec.columns)
    canonical_names = tuple(column.name for column in table.columns)
    sources, _unmapped = resolve_sources(generated, canonical_names, rule)

    written = []
    for column in table.columns:
        if column.name in sources or column.required \
                or column.name in TECHNICAL_ORACLE and table.has(column.name) \
                or column.name in rule.include:
            written.append(column)

    columns = tuple(oracle_column(column) for column in written)
    original = spec.produce
    spec_key = spec.key
    occurrences = {}

    def produce(cfg, ctx):
        occurrences.clear()
        for index, row in enumerate(original(cfg, ctx)):
            values = dict(zip(generated, row))
            code = values.get(rule.key) if rule.key else None
            occurrence = 0
            if code is not None:
                occurrence = occurrences.get(code, 0)
                occurrences[code] = occurrence + 1
            out = []
            for column in written:
                source = sources.get(column.name)
                if source is not None:
                    value = values.get(source)
                elif column.name in rule.parents:
                    value = code_id(rule.parents[column.name])(cfg, values, index)
                elif rule.key and column.name in table.primary_key:
                    value = surrogate_id(code, occurrence)
                else:
                    value = _fill_value(column, table, spec_key, index, cfg, values, rule)
                if value is None and column.required:
                    value = _fill_value(column, table, spec_key, index, cfg, values, rule)
                if value is None and column.required:
                    raise ContractError(
                        "%s.%s is required by %s and the extract has no value"
                        % (spec_key, column.name, key))
                out.append(_coerce(value, column, "%s.%s" % (spec_key, column.name)))
            yield tuple(out)

    return _replace(spec, columns=columns, produce=produce,
                    target_object=rule.target or spec.target_object)


def conform_sql_spec(spec: TableSpec, target: str, rule: Rule = None) -> TableSpec:
    """Rebuild one SQL Server extract against its raw landing table."""
    rule = rule or Rule()
    table = canon.sqlserver_landing_tables().get(target)
    if table is None:
        raise ContractError("no canonical landing table %s" % target)

    generated = tuple(column.name for column in spec.columns)
    canonical_names = tuple(column.name for column in table.columns)
    sources, _unmapped = resolve_sources(generated, canonical_names, rule)

    required = set(table.required_columns)
    written = [column for column in table.columns
               if column.name in sources or column.name in required
               or column.name in rule.include]
    columns = tuple(sql_column(column) for column in written)
    original = spec.produce
    spec_key = spec.key

    def produce(cfg, ctx):
        for index, row in enumerate(original(cfg, ctx)):
            values = dict(zip(generated, row))
            out = []
            for column in written:
                source = sources.get(column.name)
                if source is not None:
                    value = values.get(source)
                else:
                    filler = rule.fill.get(column.name)
                    if callable(filler):
                        value = filler(cfg, values, index)
                    elif filler is not None:
                        value = filler
                    elif column.name == "BatchId":
                        value = cfg.batch_id
                    elif column.name == "SourceRowNumber":
                        value = index + 1
                    elif column.name == "LoadedAtUtc":
                        value = cfg.loaded_at_utc
                    elif column.name == "SourceSystemCode":
                        value = rule.fill.get("SourceSystemCode", "WWI_OLTP")
                    else:
                        value = None
                if value is None and column.name in required:
                    raise ContractError(
                        "%s.%s is required by %s and the extract has no value"
                        % (spec_key, column.name, target))
                out.append(_coerce(value, column, "%s.%s" % (spec_key, column.name)))
            yield tuple(out)

    return _replace(spec, columns=columns, produce=produce, target_object=target)


def _replace(spec: TableSpec, **changes) -> TableSpec:
    return dataclasses.replace(spec, **changes)
