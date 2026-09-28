"""Unity Catalog naming contract.

Legacy object -> Delta table mapping (snake_case of the legacy object, spaces -> ``_``)::

    raw.X         -> bronze.raw_x          Dimension.X  -> gold.dim_x
    stg.X         -> silver.stg_x          Fact.X       -> gold.fact_x
    work.X        -> silver.work_x         Aggregate.X  -> gold.agg_x
    err.X         -> silver.err_x          Report.X     -> gold.rpt_x
    ref.X         -> silver.ref_x          Integration.X -> silver.int_x
    etl.X         -> etl.x   (control tables keep PascalCase *columns*)

Everything is qualified with the ``catalog`` job parameter / bundle variable; nothing is
hard-coded.
"""
from __future__ import annotations

import re
from typing import Dict, Tuple

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
ETL = "etl"

# legacy schema (lower-case) -> (target schema, table prefix)
LEGACY_SCHEMA_MAP: Dict[str, Tuple[str, str]] = {
    "raw": (BRONZE, "raw_"),
    "stg": (SILVER, "stg_"),
    "work": (SILVER, "work_"),
    "err": (SILVER, "err_"),
    "ref": (SILVER, "ref_"),
    "integration": (SILVER, "int_"),
    "dimension": (GOLD, "dim_"),
    "fact": (GOLD, "fact_"),
    "aggregate": (GOLD, "agg_"),
    "report": (GOLD, "rpt_"),
    "etl": (ETL, ""),
}

_LEGACY_REF = re.compile(
    r"(?<![\w.\[])\[?(raw|stg|work|err|ref|Integration|Dimension|Fact|Aggregate|Report|etl)\]?\."
    r"(\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*(?: [A-Za-z_][A-Za-z0-9_]*)*)",
    re.IGNORECASE,
)
# Legacy object names may contain spaces ("Fact.Stock Holding"); inside SQL the run of words after the
# object must stop at the first SQL keyword ("stg.Customer GROUP BY ..." names stg.Customer only).
_SQL_KEYWORDS = frozenset("""
    select from where group by having order join inner left right full outer cross on as and or not in is
    null exists between like union all except intersect limit when then else end case with using values
    insert update delete set into distinct top over partition asc desc offset fetch coalesce
""".split())


def table(catalog: str, schema: str, name: str) -> str:
    """``naming.table("wwi_dev", "gold", "dim_customer") -> "wwi_dev.gold.dim_customer"``."""
    return f"{catalog}.{schema}.{name}"


def controlTable(catalog: str, name: str) -> str:
    """Fully qualified control table, e.g. ``controlTable("wwi_dev", "batch") -> "wwi_dev.etl.batch"``."""
    return table(catalog, ETL, name)


def snake(name: str) -> str:
    """``"Stock Item" -> "stock_item"``, ``"CustomerMaster" -> "customer_master"``, ``"FxRateDaily" -> "fx_rate_daily"``."""
    s = name.strip().strip("[]").replace(" ", "_").replace("-", "_")
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.lower()


def splitLegacy(legacyName: str) -> Tuple[str, str]:
    """``"Dimension.Stock Item" -> ("Dimension", "Stock Item")``; tolerates ``[schema].[object]``."""
    parts = [p.strip().strip("[]") for p in re.split(r"\.(?![^\[]*\])", legacyName.strip())]
    if len(parts) == 1:
        return "", parts[0]
    return parts[-2], parts[-1]


def deltaName(legacyName: str) -> Tuple[str, str]:
    """Return ``(schema, table)`` for a legacy ``schema.object`` name (no catalog)."""
    schema, obj = splitLegacy(legacyName)
    key = schema.lower()
    if key not in LEGACY_SCHEMA_MAP:
        raise ValueError(f"Unknown legacy schema '{schema}' in '{legacyName}'.")
    targetSchema, prefix = LEGACY_SCHEMA_MAP[key]
    if key == "etl":
        targetSchema = ETL  # resolved at call time so a relocated control schema is honoured
    return targetSchema, prefix + snake(obj)


def legacyToDelta(catalog: str, legacyName: str) -> str:
    """``legacyToDelta("wwi_dev", "stg.Customer") -> "wwi_dev.silver.stg_customer"``.

    Names that already carry three parts (``catalog.schema.table``) are returned unchanged.
    """
    if legacyName.count(".") >= 2 and "[" not in legacyName and " " not in legacyName.strip():
        return legacyName
    schema, tbl = deltaName(legacyName)
    return table(catalog, schema, tbl)


_SQL_STRING_LITERAL = re.compile(r"N?'(?:[^']|'')*'")


def translateLegacyReferences(catalog: str, sql: str) -> str:
    """Rewrite legacy ``schema.Object`` references inside a SQL fragment to Delta names.

    Used by the data-quality evaluator and the orchestration control nodes so seeded
    ``RuleExpression`` / T-SQL values may keep referring to ``stg.Customer`` etc. Only the schema
    prefixes in :data:`LEGACY_SCHEMA_MAP` are rewritten, string literals are left untouched and a
    multi-word object name ends at the first SQL keyword.
    """
    def _repl(m: "re.Match[str]") -> str:
        obj = m.group(2)
        if not obj.startswith("["):
            words = obj.split(" ")
            keep = [words[0]]
            for w in words[1:]:
                if w.lower() in _SQL_KEYWORDS:
                    break
                keep.append(w)
            obj = " ".join(keep)
        rest = m.group(0)[m.start(2) - m.start(0) + len(obj):]
        return legacyToDelta(catalog, f"{m.group(1)}.{obj}") + rest

    out, pos = [], 0
    for lit in _SQL_STRING_LITERAL.finditer(sql):
        out.append(_LEGACY_REF.sub(_repl, sql[pos:lit.start()]))
        out.append(lit.group(0))
        pos = lit.end()
    out.append(_LEGACY_REF.sub(_repl, sql[pos:]))
    return "".join(out)


def aliasFor(legacyName: str) -> str:
    """Stable SQL alias for the legacy object (``"stg.Stock Item" -> "Stock_Item"``)."""
    _, obj = splitLegacy(legacyName)
    return re.sub(r"\W", "_", obj)
