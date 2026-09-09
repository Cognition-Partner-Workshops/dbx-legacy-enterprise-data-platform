"""ssisgen - emit SSIS 2016 (.dtsx) control flow / data flow XML from Python specs.

The estate contains ~200 packages. Hand-writing SSIS XML at that scale produces
either copy-paste clones or broken XML, so every package in ssis/ is emitted from
a Python spec module through this library. The XML shape follows the existing
wwi-ssis/DailyETLMain.dtsx package that ships with WideWorldImporters.

Design rules:

* Deterministic. Every DTSID/GUID is derived from an MD5 of the object's refId,
  so regenerating a package produces a byte-identical file and diffs stay
  readable.
* No live-connection metadata. Data flow components carry cached column
  metadata supplied by the spec, exactly as SSIS persists it; nothing here
  contacts a database.
* Structural only. This library guarantees well-formed, schema-shaped XML - it
  does NOT guarantee the package executes. Runtime validation happens in a
  later phase once SQL Server and Oracle are provisioned.

Usage:

    from ssisgen import Package, DataFlow, Column

    pkg = Package("EXT_ORA_CustomerMaster", description="...")
    pkg.add_parameter("SourceQueryTimeout", 3600, dtype="int")
    pkg.add_variable("BatchId", 0, dtype="int")
    ...
    pkg.write("ssis/01_oracle_extract/EXT_ORA_CustomerMaster.dtsx")
"""

from __future__ import annotations

import hashlib
import os
import re
from xml.sax.saxutils import escape, quoteattr

import dbschema

DTS_NS = "www.microsoft.com/SqlServer/Dts"

# SSIS runtime variable type codes (System.TypeCode)
VAR_TYPES = {
    "int": "3",
    "long": "20",
    "string": "8",
    "datetime": "7",
    "bool": "11",
    "decimal": "14",
}

# Package parameters are persisted with the same variant type codes as
# variables; the runtime parses ParameterValue with that code, so a mismatch
# fails the package load rather than the build.
PARAM_TYPES = {"int": "3", "string": "8", "bool": "11", "datetime": "7", "decimal": "14"}

# Execute SQL Task parameter bindings are persisted as OLE DB type codes; the
# task parses DataType as an integer, so symbolic names fail the package load.
SQLTASK_TYPES = {"LONG": 3, "NVARCHAR": 130, "BYTE": 17}
SQLTASK_SIZES = {"LONG": -1, "NVARCHAR": 4000, "BYTE": -1}

# Flat file columns are persisted with OLE DB type codes. Every feed column is
# read as text and converted downstream, which is what the estate's packages
# do, so DT_WSTR is the default.
FLAT_FILE_TYPES = {"wstr": 130, "str": 129, "i4": 3, "i8": 20}


def param_value(value, dtype):
    """Serialise a parameter value the way the SSIS runtime parses it back."""
    if dtype == "bool":
        if isinstance(value, str):
            truthy = value.strip().lower() in ("1", "true", "-1")
        else:
            truthy = bool(value)
        return "True" if truthy else "False"
    return str(value)

# Pipeline buffer column data types
DT = {
    "i4": ("i4", None, None, None),
    "i8": ("i8", None, None, None),
    "bool": ("bool", None, None, None),
    "date": ("date", None, None, None),
    "dbDate": ("dbDate", None, None, None),
    "dbTimeStamp": ("dbTimeStamp", None, None, None),
    "guid": ("guid", None, None, None),
}


# The SSIS runtime rejects these characters in any named object, so a spec that
# names a task after a qualified table ("Truncate raw.Foo") has to be folded to a
# legal name before it reaches the XML.
INVALID_NAME_CHARS = "/\\:[].="


def unique_columns(columns):
    """Drop repeat column names; a buffer column name is unique within a path."""
    seen = set()
    out = []
    for col in columns:
        if col.name in seen:
            continue
        seen.add(col.name)
        out.append(col)
    return out


def safe_name(name):
    """Fold a spec-supplied object name into one the SSIS runtime accepts."""
    out = name
    for char in INVALID_NAME_CHARS:
        out = out.replace(char, "_")
    return out


class ContractError(ValueError):
    """A spec asks for XML the SSIS runtime would reject at validation."""


# Destinations the estate has never been able to describe from their target
# table: the table is not deployed by any DDL under sqlserver/, or the buffer
# carries a column the destination cannot insert into the column of that name.
# They keep the buffer-derived metadata they have always had, which is exactly
# the metadata SSIS rejects with VS_NEEDSNEWMETADATA, so every entry is a known
# defect waiting for its package to be repaired - not a licence to write more.
# The register only shrinks: a destination that can honour its table contract
# fails generation while it is still listed here, and one that cannot and is not
# listed fails generation too.
DEBT_REGISTER = os.path.join(dbschema.REPO_ROOT, "ssis", "destination-metadata-debt.txt")

_DEBT = None


def destination_debt():
    """The registered destinations, keyed 'data flow|destination|table'."""
    global _DEBT
    if _DEBT is None:
        entries = {}
        if os.path.exists(DEBT_REGISTER):
            with open(DEBT_REGISTER, encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    flow, destination, table, reason = (line.split("|") + ["", "", "", ""])[:4]
                    entries[debt_key(flow.strip(), destination.strip(), table.strip())] = reason.strip()
        _DEBT = entries
    return _DEBT


def debt_key(flow, destination, table):
    return "%s|%s|%s" % (flow, destination, dbschema.normalise_table(table))


def sql_parameter_count(sql):
    """Number of ``?`` placeholders the OLE DB provider binds in *sql*.

    Question marks inside string literals belong to the data, not to the
    parameter list, so they are skipped.
    """
    count = 0
    in_literal = False
    for char in sql:
        if char == "'":
            in_literal = not in_literal
        elif char == "?" and not in_literal:
            count += 1
    return count


def expression_statement_count(expression):
    """Number of statements the expression evaluator would read in *expression*.

    The Expression Task evaluates one expression, so a semicolon that is not
    inside a string literal is a second statement the parser refuses with 'The
    token ";" ... was not recognized'.
    """
    count = 1
    in_literal = False
    escaped = False
    for char in expression:
        if escaped:
            escaped = False
            continue
        if char == "\\" and in_literal:
            escaped = True
        elif char == '"':
            in_literal = not in_literal
        elif char == ";" and not in_literal:
            count += 1
    return count


def guid(seed: str) -> str:
    """Deterministic {GUID} derived from a seed string."""
    h = hashlib.md5(seed.encode("utf-8")).hexdigest().upper()
    return "{%s-%s-%s-%s-%s}" % (h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])


def connection_manager_ref_id(name, scope="Project"):
    """refId of a connection manager as the runtime addresses it."""
    return "%s.ConnectionManagers[%s]" % (scope, name)


def connection_manager_id(name, scope="Project"):
    """ID a pipeline component's <connection> element must carry.

    A package-scoped manager is addressed by its refId path: the DTSID is not in
    the package's ID map that CPackage::LoadFromXML resolves pipeline connection
    references against, so emitting the GUID there fails the load with
    0xC001001C. A project-scoped manager lives outside the package and is
    addressed by its DTSID with the ``:external`` suffix.
    """
    if scope == "Package":
        return connection_manager_ref_id(name, scope)
    return guid("cm:" + name) + ":external"


def attr(name, value):
    return "%s=%s" % (name, quoteattr(str(value)))


class Column:
    """A pipeline buffer column."""

    def __init__(self, name, dtype="wstr", length=None, precision=None, scale=None, codepage=None):
        self.name = name
        self.dtype = dtype
        self.length = length
        self.precision = precision
        self.scale = scale
        self.codepage = codepage

    def cached_attrs(self, prefix="cached"):
        parts = ['%sDataType="%s"' % (prefix, self.dtype)]
        if self.length is not None:
            parts.append('%sLength="%d"' % (prefix, self.length))
        if self.precision is not None:
            parts.append('%sPrecision="%d"' % (prefix, self.precision))
        if self.scale is not None:
            parts.append('%sScale="%d"' % (prefix, self.scale))
        if self.codepage is not None:
            parts.append('%sCodePage="%d"' % (prefix, self.codepage))
        parts.append('%sName=%s' % (prefix, quoteattr(self.name)))
        return " ".join(parts)

    def metadata_attrs(self):
        parts = ['dataType="%s"' % self.dtype]
        if self.length is not None:
            parts.append('length="%d"' % self.length)
        if self.precision is not None:
            parts.append('precision="%d"' % self.precision)
        if self.scale is not None:
            parts.append('scale="%d"' % self.scale)
        if self.codepage is not None:
            parts.append('codePage="%d"' % self.codepage)
        return " ".join(parts)


def str_col(name, length=50):
    return Column(name, "wstr", length=length)


def int_col(name):
    return Column(name, "i4")


def bigint_col(name):
    return Column(name, "i8")


def money_col(name):
    return Column(name, "numeric", precision=18, scale=2)


def date_col(name):
    return Column(name, "dbTimeStamp")


def day_col(name):
    """A whole-day column, which is what a SQL DATE hands the buffer.

    Declaring one as a timestamp leaves the source's external metadata out of
    synchronisation with the column the server describes.
    """
    return Column(name, "dbDate")


# The aggregate names its summaries by number, and a summary applied to a type
# it does not produce - a count landing in a decimal column, say - is rejected.
AGGREGATION_TYPES = {
    "group": 0,
    "count": 1,
    "countdistinct": 2,
    "sum": 3,
    "avg": 4,
    "min": 5,
    "minimum": 5,
    "max": 6,
    "maximum": 6,
    "average": 4,
    "count distinct": 2,
}


def aggregation_type(op):
    """The number the aggregate gives the summary *op*."""
    try:
        return AGGREGATION_TYPES[op.lower()]
    except KeyError:
        raise ContractError("unknown aggregation %r, expected one of %s"
                            % (op, ", ".join(sorted(AGGREGATION_TYPES))))


def aggregate_output_column(name, op, source):
    """The column *op* over *source* produces, named *name*.

    A count is an unsigned four byte counter whatever it counted - the wider
    counter is the component's big-count shape, which no expression downstream
    can compare against a plain integer. Every other summary carries the type
    of the column it summarises, which is the only type the component accepts.
    """
    op = op.lower()
    if op in ("count", "countdistinct", "count distinct"):
        return Column(name, "ui4")
    return Column(name, source.dtype, length=source.length,
                  precision=source.precision, scale=source.scale,
                  codepage=source.codepage)


# The single column a flat file source puts its unparsable line into.
FLAT_FILE_ERROR_COLUMN = "Flat File Source Error Output Column"

LOCALE_ID = 1033

# The columns every error output carries to describe the failure.
ERROR_COLUMNS = ("ErrorCode", "ErrorColumn")


def text_col(name, codepage=1252):
    return Column(name, "text", codepage=codepage)


def ntext_col(name):
    return Column(name, "nText")


def plain_table(table):
    """'[stg].[Currency]' as the estate writes it in a log: 'stg.Currency'."""
    return table.replace("[", "").replace("]", "").strip()


# Words that name the shape of a pipeline output rather than the reason rows
# were routed down it.
OUTPUT_NOISE = ("ole", "db", "output", "input", "flat", "file", "1")


def reason_code(output_name):
    """The reject reason an output's name states, as a code.

    'Invalid Currency' rejects for INVALID_CURRENCY, and the source error
    output for SOURCE_ERROR: the routing decision is the reason, so the
    generator does not invent a second vocabulary for it.
    """
    words = [word for word in re.findall(r"[A-Za-z0-9]+", output_name)
             if word.lower() not in OUTPUT_NOISE]
    return "_".join(word.upper() for word in words) or "REJECTED"


def lookup_subject(component_name):
    """'Lookup Sales Territory (Full Cache)' looks up 'Sales Territory'."""
    subject = re.sub(r"\([^)]*\)", " ", component_name)
    subject = re.sub(r"(?i)^\s*lookup\s+", "", subject.strip())
    return " ".join(subject.split()) or component_name


def literal_expression(column, value):
    """*value* as an expression of *column*'s type, or None when it has none."""
    text = value.replace('"', "'")
    if column.dtype == "wstr":
        return '(DT_WSTR,%d)"%s"' % (column.length or len(text), text[:column.length or len(text)])
    if column.dtype == "str":
        return '(DT_STR,%d,%d)"%s"' % (column.length or len(text), column.codepage or 1252,
                                       text[:column.length or len(text)])
    return None


def envelope_expression(target, object_name, from_component, from_output):
    """What the generator can honestly derive for error column *target*."""
    name = target.name.lower()
    if name == "batchid":
        return "(DT_I8) @[$Package::BatchId]"
    if name in ("targetobjectname", "sourceobjectname", "objectname"):
        return literal_expression(target, plain_table(object_name))
    if name == "rejectreasoncode":
        return literal_expression(target, reason_code(from_output))
    if name == "lookupname":
        return literal_expression(target, lookup_subject(from_component))
    return None


VARIABLE_REFERENCE = re.compile(r"[@$]\[[^\]]*\]")


def referenced_columns(expression, candidates):
    """The column names *expression* reads, in *candidates* order.

    A transform only sees the input columns it asks for, so an expression over
    a column the component never claimed fails to parse at validation time. A
    column reads either bare or bracketed; variable and parameter references are
    dropped first so a name inside one is not read as a column.
    """
    text = VARIABLE_REFERENCE.sub(" ", expression)
    return [name for name in candidates
            if re.search(r"(?<![\w:])\[?%s\]?(?![\w])" % re.escape(name), text)]


# ---------------------------------------------------------------------------
# Data flow
# ---------------------------------------------------------------------------


class _Component:
    def __init__(self, name, class_id, description, columns, version=4):
        self.name = name
        self.class_id = class_id
        self.description = description
        self.columns = columns
        self.version = version


class DataFlow:
    """A Microsoft.Pipeline task built as a linear chain of components.

    The chain is intentionally simple - source, optional transforms, destination -
    which is what the vast majority of the legacy estate's data flows look like.
    Error outputs from the source and destination are redirected to a reject
    destination when ``reject_table`` is supplied.
    """

    def __init__(self, name, description="Data Flow Task"):
        self.name = safe_name(name)
        self.description = description
        self._components = []  # list of dicts describing xml emission
        self._paths = []
        self._last_output = None
        self._columns = []
        self._primary_table = None
        self._envelope_columns = set()

    # -- sources ------------------------------------------------------------

    def oledb_source(self, name, connection, sql, columns, timeout=0, parameters=()):
        """Read a rowset with an SQL command.

        ``parameters`` names the variables or package parameters bound to the
        ``?`` placeholders of *sql*, in placeholder order. A parameterised
        command with no bindings cannot be executed, so the count is enforced
        when the package emits (see :meth:`bind`).
        """
        self._columns = list(columns)
        self._components.append(
            dict(kind="oledb_source", name=name, connection=connection, sql=sql, columns=list(columns),
                 timeout=timeout, parameters=list(parameters), parameter_mapping="")
        )
        self._last_output = (name, "OLE DB Source Output")
        return self

    def bind(self, package):
        """Resolve variable-addressed component metadata against *package*.

        An OLE DB source persists its parameter list as ``ParameterMapping``:
        placeholder name plus the DTSID of the variable feeding it. The DTSID
        is derived from the owning package, so it can only be resolved once the
        data flow is attached to one.
        """
        for comp in self._components:
            if comp["kind"] != "oledb_source":
                continue
            expected = sql_parameter_count(comp["sql"])
            bound = comp["parameters"]
            if len(bound) != expected:
                raise ContractError(
                    "OLE DB source %r in %r has %d '?' placeholder(s) but %d bound "
                    "parameter(s); an unbound placeholder fails component validation "
                    "at run time" % (comp["name"], self.name, expected, len(bound)))
            comp["parameter_mapping"] = "".join(
                '"Parameter%d:Input",%s;' % (index, package.variable_dtsid(variable))
                for index, variable in enumerate(bound))
        return self

    def flatfile_source(self, name, connection, columns, connection_scope="Project", code_page=1252):
        """Read a delimited file through a flat file connection manager.

        ``connection_scope`` is ``Package`` for a per-feed flat file connection
        manager whose ConnectionString is an expression over a package variable
        (the file the Foreach loop is currently on), which is the only scope
        that can see such a variable.
        """
        self._columns = list(columns)
        self._components.append(dict(kind="flatfile_source", name=name, connection=connection,
                                     columns=list(columns), connection_scope=connection_scope,
                                     code_page=code_page))
        self._last_output = (name, "Flat File Source Output")
        return self

    # -- transforms ---------------------------------------------------------

    def derived_column(self, name, derivations, source=None):
        """derivations: list of (column_name, expression, Column).

        ``source`` attaches the derivation to a named upstream output - a split
        case or an error output - instead of continuing the main path, which is
        how a branch gets the columns only its own destination needs.
        """
        for stage, group in enumerate(self._stage_derivations(name, list(derivations))):
            stage_name = name if stage == 0 else "%s %d" % (name, stage + 1)
            self._components.append(dict(kind="derived", name=stage_name, derivations=group))
            for _, _, col in group:
                self._columns.append(col)
            upstream = source if (stage == 0 and source) else self._last_output
            self._paths.append((upstream, (stage_name, "Derived Column Input")))
            self._last_output = (stage_name, "Derived Column Output")
        return self

    def _stage_derivations(self, name, derivations):
        """*derivations* grouped into the components that can evaluate them.

        A derived column evaluates every expression against its input row, so a
        column the same component adds is not there to be read: a derivation
        over one has to run in a later component. A derivation that replaces an
        upstream column is not such a case - the input still carries the value
        the upstream component produced - so it stays with its readers.
        """
        upstream = set(col.name for col in self._columns)
        added = [col_name for col_name, _expr, _col in derivations if col_name not in upstream]
        pending = list(derivations)
        available = set(upstream)
        stages = []
        while pending:
            ready, deferred = [], []
            for derivation in pending:
                col_name, expr, _col = derivation
                waiting = [other for other in referenced_columns(expr, added)
                           if other != col_name and other not in available]
                (deferred if waiting else ready).append(derivation)
            if not ready:
                raise ContractError(
                    "derived column %r in %r has derivations that read each other: %s"
                    % (name, self.name, ", ".join(sorted(col for col, _e, _c in deferred))))
            stages.append(ready)
            available.update(col_name for col_name, _e, _c in ready)
            pending = deferred
        return stages

    def lookup(self, name, connection, sql, join_columns, output_columns, no_match="RD"):
        """no_match: 'RD' redirect rows to no-match output, 'FC' fail component,
        'IG' ignore failure (null-extend)."""
        self._components.append(
            dict(
                kind="lookup",
                name=name,
                connection=connection,
                sql=sql,
                join_columns=list(join_columns),
                output_columns=list(output_columns),
                no_match=no_match,
            )
        )
        self._columns.extend(output_columns)
        self._paths.append((self._last_output, (name, "Lookup Input")))
        self._last_output = (name, "Lookup Match Output")
        return self

    def conditional_split(self, name, cases, default_output="Default"):
        """cases: list of (output_name, expression). Continues on the first case."""
        self._components.append(dict(kind="split", name=name, cases=list(cases), default=default_output))
        self._paths.append((self._last_output, (name, "Conditional Split Input")))
        self._last_output = (name, cases[0][0])
        return self

    def multicast(self, name, outputs):
        """Fan one input out to several identical outputs; continues on the first."""
        self._components.append(dict(kind="multicast", name=name, outputs=list(outputs)))
        self._paths.append((self._last_output, (name, "Multicast Input 1")))
        self._last_output = (name, outputs[0])
        return self

    def row_count(self, name, variable):
        self._components.append(dict(kind="rowcount", name=name, variable=variable))
        self._paths.append((self._last_output, (name, "Row Count Input")))
        self._last_output = (name, "Row Count Output")
        return self

    def aggregate(self, name, group_by, aggregations):
        """aggregations: list of (source_column, output_column, operation)."""
        self._components.append(
            dict(kind="aggregate", name=name, group_by=list(group_by), aggregations=list(aggregations))
        )
        self._paths.append((self._last_output, (name, "Aggregate Input 1")))
        self._last_output = (name, "Aggregate Output 1")
        return self

    def sort(self, name, sort_columns, eliminate_duplicates=False):
        self._components.append(
            dict(kind="sort", name=name, sort_columns=list(sort_columns), dedupe=eliminate_duplicates)
        )
        self._paths.append((self._last_output, (name, "Sort Input")))
        self._last_output = (name, "Sort Output")
        return self

    def union_all(self, name):
        self._components.append(dict(kind="union", name=name))
        self._paths.append((self._last_output, (name, "Union All Input 1")))
        self._last_output = (name, "Union All Output 1")
        return self

    def data_conversion(self, name, conversions, source=None):
        """conversions: list of (source_column, output_column, Column).

        ``source`` names the (component, output) the conversion reads when it
        starts a branch off an earlier output rather than continuing the chain.
        """
        self._components.append(dict(kind="convert", name=name, conversions=list(conversions)))
        for _, _, col in conversions:
            self._columns.append(col)
        self._paths.append((source or self._last_output, (name, "Data Conversion Input")))
        self._last_output = (name, "Data Conversion Output")
        return self

    # -- destinations -------------------------------------------------------

    def _destination_mapping(self, name, table, mapping):
        """Validate a spec-supplied ``{target column: pipeline column}`` mapping.

        A mapping naming a column the table does not have, or a buffer column
        the flow never produces, describes an insert the destination cannot
        make, so it fails generation instead of the run.
        """
        mapping = dict(mapping or {})
        if not mapping or not dbschema.has_table(table):
            return mapping
        table_columns = set(col.name.lower() for col in dbschema.table_columns(table))
        buffer_columns = set(col.name.lower() for col in self._columns)
        for target, source in sorted(mapping.items()):
            if target.lower() not in table_columns:
                raise ContractError(
                    "destination %r in %r maps %r to %s, which has no such column"
                    % (name, self.name, source, table))
            if source.lower() not in buffer_columns:
                raise ContractError(
                    "destination %r in %r maps %s.%s from buffer column %r, which "
                    "the data flow does not produce" % (name, self.name, table, target, source))
        return mapping

    def _destination_contract(self, comp, lineage):
        """The ``(external column, buffer column or None)`` pairs to emit.

        The external columns are the target table's, because that is what the
        destination revalidates against when the package starts. A destination
        that cannot be described that way - no DDL deploys its table, or the
        buffer holds a column the table cannot accept under that name - keeps
        the buffer-derived metadata the estate has always given it, and only
        while :data:`DEBT_REGISTER` still records it as unrepaired.

        A destination that describes its table but leaves a NOT NULL column
        without a default unmapped would validate and then fail its first
        insert, so it needs a register entry too - it keeps the table-derived
        metadata, because reverting it to the buffer's would only move the
        failure back to validation.
        """
        key = debt_key(self.name, comp["name"], comp["table"])
        registered = key in destination_debt()
        buffer_columns = dict((col.name.lower(), col) for col in comp["columns"])
        sources = dict((target.lower(), source) for target, source in comp["mapping"].items())
        broken = None
        pairs = []
        if dbschema.has_table(comp["table"]):
            for target in dbschema.table_columns(comp["table"]):
                col = buffer_columns.get(sources.get(target.name.lower(), target.name).lower())
                if col is None or col.name not in lineage:
                    pairs.append((target, None))
                    continue
                if not dbschema.compatible(col.dtype, target.dtype):
                    broken = ("buffer column %s (%s) cannot be inserted into %s.%s (%s)"
                              % (col.name, col.dtype, comp["table"], target.name, target.dtype))
                    break
                pairs.append((target, col))
        else:
            broken = ("no CREATE TABLE under sqlserver/ deploys %s" % comp["table"])
        unmapped = None
        if not broken:
            if not any(col is not None for _target, col in pairs):
                # SSIS answers VS_ISBROKEN to an input with no columns: "The
                # number of input columns for ... cannot be zero."
                unmapped = ("the data flow maps no column into %s, so the destination "
                            "input is empty" % comp["table"])
            missing = [target.name for target, col in pairs
                       if col is None and not target.nullable
                       and not target.identity and not target.has_default]
            if missing and not unmapped:
                unmapped = ("the data flow supplies no value for NOT NULL column(s) %s"
                            % ", ".join(missing))
        broken = broken or unmapped
        if broken and not registered:
            raise ContractError(
                "destination %r in %r does not honour its table contract: %s. Fix the "
                "spec, or record the destination in %s as '%s|%s|%s|<reason>'"
                % (comp["name"], self.name, broken,
                   os.path.relpath(DEBT_REGISTER, dbschema.REPO_ROOT).replace(os.sep, "/"),
                   self.name, comp["name"], comp["table"]))
        if registered and not broken:
            raise ContractError(
                "destination %r in %r now describes %s from its table, so its entry in "
                "%s is stale and must be deleted"
                % (comp["name"], self.name, comp["table"],
                   os.path.relpath(DEBT_REGISTER, dbschema.REPO_ROOT).replace(os.sep, "/")))
        if broken and not unmapped:
            return [(col, col) for col in comp["columns"] if col.name in lineage]
        return pairs

    def oledb_destination(self, name, connection, table, fast_load=True, keep_identity=False,
                          batch_size=100000, error_disposition="FailComponent", mapping=None):
        """Insert the buffer into *table*.

        Buffer columns are mapped to the columns of the same name in the target
        table; ``mapping`` ({target column: buffer column}) carries the pairs
        whose names differ.
        """
        if self._primary_table is None:
            self._primary_table = table
        self._components.append(
            dict(
                kind="oledb_dest",
                name=name,
                connection=connection,
                table=table,
                fast_load=fast_load,
                keep_identity=keep_identity,
                batch_size=batch_size,
                error_disposition=error_disposition,
                columns=list(self._columns),
                mapping=self._destination_mapping(name, table, mapping),
            )
        )
        self._paths.append((self._last_output, (name, "OLE DB Destination Input")))
        return self

    def _reject_envelope(self, name, table, from_component, from_output):
        """The bookkeeping columns an error table asks of the branch itself.

        An error table records why a row was routed away, which is a property
        of the branch rather than of the row: the pipeline carries no BatchId,
        no object name and no reason code, so a branch destination that only
        maps the row's own columns leaves the table's mandatory columns unfed -
        and where none of the row's columns share a name with the table, the
        destination has no input column at all and fails validation with
        VS_ISBROKEN. The generator derives those columns onto the branch from
        what it already knows: the batch parameter, the table the flow loads,
        the output the rows were routed down and the lookup that missed.
        """
        if not dbschema.has_table(table):
            return []
        carried = set(col.name.lower() for col in self._columns) - self._envelope_columns
        derivations = []
        for target in dbschema.table_columns(table):
            if target.supplied_by_the_server or target.name.lower() in carried:
                continue
            expression = envelope_expression(
                target, self._primary_table or self.name, from_component, from_output)
            if expression is None:
                continue
            derivations.append((target.name, expression,
                                Column(target.name, target.dtype, length=target.length,
                                       precision=target.precision, scale=target.scale,
                                       codepage=target.codepage)))
        return derivations

    def _reject_source(self, name, table, from_component, from_output):
        """The output a reject destination reads, enveloped where it must be."""
        derivations = self._reject_envelope(name, table, from_component, from_output)
        if not derivations:
            return (from_component, from_output)
        envelope = safe_name("Envelope %s" % name)
        self._components.append(dict(kind="derived", name=envelope, derivations=derivations))
        self._paths.append(((from_component, from_output), (envelope, "Derived Column Input")))
        for col_name, _expr, col in derivations:
            self._columns.append(col)
            self._envelope_columns.add(col_name.lower())
        return (envelope, "Derived Column Output")

    def reject_destination(self, name, connection, table, from_component,
                           from_output="OLE DB Source Error Output", mapping=None):
        source = self._reject_source(name, table, from_component, from_output)
        self._components.append(
            dict(kind="oledb_dest", name=name, connection=connection, table=table, fast_load=False,
                 keep_identity=False, batch_size=0, error_disposition="FailComponent",
                 columns=list(self._columns), mapping=self._destination_mapping(name, table, mapping))
        )
        self._paths.append((source, (name, "OLE DB Destination Input")))
        return self

    def branch_destination(self, name, connection, table, from_component, from_output, mapping=None):
        """Attach an extra destination to a named upstream output (e.g. a split case)."""
        source = self._reject_source(name, table, from_component, from_output)
        self._components.append(
            dict(kind="oledb_dest", name=name, connection=connection, table=table, fast_load=True,
                 keep_identity=False, batch_size=10000, error_disposition="FailComponent",
                 columns=list(self._columns), mapping=self._destination_mapping(name, table, mapping))
        )
        self._paths.append((source, (name, "OLE DB Destination Input")))
        return self

    # -- emission -----------------------------------------------------------

    def _ref(self, base, component=None):
        return base if component is None else "%s\\%s" % (base, component)

    def _normalise_names(self):
        """Fold component and path endpoint names to runtime-legal names."""
        for comp in self._components:
            comp["name"] = safe_name(comp["name"])
            if "columns" in comp:
                comp["columns"] = unique_columns(comp["columns"])
        self._paths = [((safe_name(start[0]), start[1]), (safe_name(end[0]), end[1]))
                       for start, end in self._paths]

    def _path_names(self):
        """Unique, deterministic path names, one per path in declaration order."""
        names = []
        used = set()
        starts = set()
        for start, _end in self._paths:
            if start in starts:
                raise ValueError(
                    "output %s.%s feeds more than one path; fan out with a multicast"
                    % (start[0], start[1]))
            starts.add(start)
            candidate = start[1]
            suffix = 1
            while candidate in used:
                candidate = "%s %d" % (start[1], suffix)
                suffix += 1
            used.add(candidate)
            names.append(candidate)
        return names

    def _lineage(self, base_ref):
        """Map every component output to the columns it exposes downstream.

        SSIS resolves an input column through the ``lineageId`` of the output
        column that produced it, so every input column has to name the refId
        minted by its upstream output rather than repeat its own refId.
        """
        upstream_of = {}
        for start, end in self._paths:
            upstream_of[end] = start
        outputs = {}

        def cols_of(comp, output_name, columns):
            ref = "%s\\%s" % (base_ref, comp["name"])
            return [(col.name, "%s.Outputs[%s].Columns[%s]" % (ref, output_name, col.name), col)
                    for col in columns]

        def inherited(comp, input_name):
            source = upstream_of.get((comp["name"], input_name))
            return list(outputs.get(source, []))

        for comp in self._components:
            name = comp["name"]
            ref = "%s\\%s" % (base_ref, name)
            kind = comp["kind"]
            if kind in ("oledb_source", "flatfile_source"):
                is_flat = kind == "flatfile_source"
                main = "Flat File Source Output" if is_flat else "OLE DB Source Output"
                error = "Flat File Source Error Output" if is_flat else "OLE DB Source Error Output"
                outputs[(name, main)] = cols_of(comp, main, comp["columns"])
                # A flat file source cannot describe the row it failed to parse
                # column by column, so its error output carries the whole line
                # as one blob instead of a copy of the source columns.
                error_columns = ([text_col(FLAT_FILE_ERROR_COLUMN, comp["code_page"])] if is_flat
                                 else list(comp["columns"]))
                outputs[(name, error)] = cols_of(
                    comp, error, error_columns + [int_col("ErrorCode"), int_col("ErrorColumn")])
            elif kind == "derived":
                base = inherited(comp, "Derived Column Input")
                existing = set(col_name for col_name, _rid, _col in base)
                # A derivation that reuses an upstream column name replaces that
                # column in place, so it keeps the upstream lineage id.
                added = [(col_name, "%s.Outputs[Derived Column Output].Columns[%s]" % (ref, col_name), col)
                         for col_name, _expr, col in comp["derivations"] if col_name not in existing]
                outputs[(name, "Derived Column Output")] = base + added
            elif kind == "lookup":
                base = inherited(comp, "Lookup Input")
                existing = set(col_name for col_name, _rid, _col in base)
                added = [(col.name, "%s.Outputs[Lookup Match Output].Columns[%s]" % (ref, col.name), col)
                         for col in comp["output_columns"] if col.name not in existing]
                outputs[(name, "Lookup Match Output")] = base + added
                outputs[(name, "Lookup No Match Output")] = base
                outputs[(name, "Lookup Error Output")] = base
            elif kind == "split":
                base = inherited(comp, "Conditional Split Input")
                for out_name, _expr in comp["cases"]:
                    outputs[(name, out_name)] = base
                outputs[(name, comp["default"])] = base
            elif kind == "rowcount":
                outputs[(name, "Row Count Output")] = inherited(comp, "Row Count Input")
            elif kind == "multicast":
                base = inherited(comp, "Multicast Input 1")
                for out_name in comp["outputs"]:
                    outputs[(name, out_name)] = base
            elif kind == "convert":
                base = inherited(comp, "Data Conversion Input")
                existing = set(col_name for col_name, _rid, _col in base)
                added = [(dest, "%s.Outputs[Data Conversion Output].Columns[%s]" % (ref, dest), col)
                         for _src, dest, col in comp["conversions"] if dest not in existing]
                outputs[(name, "Data Conversion Output")] = base + added
                # The error output is synchronous, so the passthrough columns keep
                # their upstream lineage ids. Its own error columns replace any
                # the upstream error output already carried, which would
                # otherwise be declared twice under two different lineage ids.
                outputs[(name, "Data Conversion Error Output")] = [
                    entry for entry in base if entry[0] not in ERROR_COLUMNS] + [
                    (col.name, "%s.Outputs[Data Conversion Error Output].Columns[%s]" % (ref, col.name), col)
                    for col in (int_col("ErrorCode"), int_col("ErrorColumn"))]
            elif kind == "aggregate":
                base = dict((col_name, col) for col_name, _rid, col in inherited(comp, "Aggregate Input 1"))
                produced = [(gb, base.get(gb, str_col(gb))) for gb in comp["group_by"]]
                # A summary carries the type of the column it summarises, except
                # a count, which the component always produces as an unsigned
                # eight byte counter whatever it counted.
                produced += [(dest, aggregate_output_column(dest, op, base.get(src, int_col(dest))))
                             for src, dest, op in comp["aggregations"]]
                seen = set()
                outputs[(name, "Aggregate Output 1")] = [
                    (col_name, "%s.Outputs[Aggregate Output 1].Columns[%s]" % (ref, col_name), col)
                    for col_name, col in produced
                    if not (col_name in seen or seen.add(col_name))]
            elif kind == "sort":
                outputs[(name, "Sort Output")] = [
                    (col_name, "%s.Outputs[Sort Output].Columns[%s]" % (ref, col_name), col)
                    for col_name, _rid, col in inherited(comp, "Sort Input")]
            elif kind == "union":
                outputs[(name, "Union All Output 1")] = [
                    (col_name, "%s.Outputs[Union All Output 1].Columns[%s]" % (ref, col_name), col)
                    for col_name, _rid, col in inherited(comp, "Union All Input 1")]
        return outputs, upstream_of

    def to_xml(self, base_ref, indent):
        pad = " " * indent
        self._normalise_names()
        outputs, upstream_of = self._lineage(base_ref)
        out = []
        out.append('%s<pipeline version="1">' % pad)
        out.append("%s  <components>" % pad)
        for comp in self._components:
            out.extend(self._component_xml(comp, base_ref, indent + 4, outputs, upstream_of))
        out.append("%s  </components>" % pad)
        out.append("%s  <paths>" % pad)
        path_names = self._path_names()
        for (start, end), path_name in zip(self._paths, path_names):
            start_ref = "%s\\%s.Outputs[%s]" % (base_ref, start[0], start[1])
            end_ref = "%s\\%s.Inputs[%s]" % (base_ref, end[0], end[1])
            out.append(
                '%s    <path refId="%s.Paths[%s]" endId="%s" name="%s" startId="%s" />'
                % (pad, base_ref, escape(path_name), escape(end_ref), escape(path_name), escape(start_ref))
            )
        out.append("%s  </paths>" % pad)
        out.append("%s</pipeline>" % pad)
        return out

    # component emitters ----------------------------------------------------

    def _prop(self, pad, dtype, name, value, description=""):
        return '%s<property dataType="%s" description=%s name="%s">%s</property>' % (
            pad,
            dtype,
            quoteattr(description),
            name,
            escape(str(value)),
        )

    def _read_columns(self, expressions, upstream, exclude):
        """Upstream columns the expressions read, as (name, Column) pairs."""
        names = set()
        candidates = [col_name for col_name, _rid, _col in upstream if col_name not in exclude]
        for expression in expressions:
            names.update(referenced_columns(expression, candidates))
        return [(col_name, col) for col_name, _rid, col in upstream
                if col_name in names]

    def _error_output_xml(self, pad, ref, component):
        """The error output a synchronous transform is required to declare.

        A transform that redirects nothing still has to carry the error output
        the component creates for itself; otherwise the output count is wrong
        and the component fails validation with VS_ISCORRUPT.
        """
        name = "%s Error Output" % component
        input_ref = "%s.Inputs[%s Input]" % (ref, component)
        lines = ['%s    <output refId=%s exclusionGroup="1" isErrorOut="true" name=%s synchronousInputId=%s>'
                 % (pad, quoteattr("%s.Outputs[%s]" % (ref, name)), quoteattr(name), quoteattr(input_ref)),
                 "%s      <outputColumns>" % pad]
        for extra, flags in (("ErrorCode", 1), ("ErrorColumn", 2)):
            col_ref = "%s.Outputs[%s].Columns[%s]" % (ref, name, extra)
            lines.append('%s        <outputColumn refId=%s dataType="i4" lineageId=%s name="%s" specialFlags="%d" />'
                         % (pad, quoteattr(col_ref), quoteattr(col_ref), extra, flags))
        lines.append("%s      </outputColumns>" % pad)
        lines.append("%s      <externalMetadataColumns />" % pad)
        lines.append("%s    </output>" % pad)
        return "\n".join(lines)

    def _component_xml(self, comp, base_ref, indent, outputs, upstream_of):
        pad = " " * indent
        ref = "%s\\%s" % (base_ref, comp["name"])
        kind = comp["kind"]
        out = []

        def upstream_cols(input_name):
            return outputs.get(upstream_of.get((comp["name"], input_name)), [])

        def lineage_of(input_name):
            return dict((col_name, rid) for col_name, rid, _col in upstream_cols(input_name))

        def column_of(input_name):
            return dict((col_name, col) for col_name, _rid, col in upstream_cols(input_name))
        if kind in ("oledb_source", "flatfile_source"):
            is_flat = kind == "flatfile_source"
            class_id = "Microsoft.FlatFileSource" if is_flat else "Microsoft.OLEDBSource"
            # A flat file adapter parses text against a locale, and it reads that
            # locale from the component rather than from the connection manager.
            out.append(
                '%s<component refId=%s componentClassID="%s" contactInfo="%s" description="%s"%s name=%s usesDispositions="true" version="%d">'
                % (pad, quoteattr(ref), class_id,
                   "Flat File Source" if is_flat else "OLE DB Source",
                   "Flat File Source" if is_flat else "OLE DB Source",
                   ' localeId="%d"' % LOCALE_ID if is_flat else "",
                   quoteattr(comp["name"]), 1 if is_flat else 7)
            )
            out.append("%s  <properties>" % pad)
            if not is_flat:
                out.append(self._prop(pad + "    ", "System.Int32", "CommandTimeout", comp["timeout"],
                                      "The number of seconds before a command times out."))
                # The OLE DB source declares the whole custom property set it
                # supports, not just the ones the chosen access mode reads. The
                # component asks the runtime for every property by name during
                # validation, and a missing one - OpenRowsetVariable is the
                # first it looks for - fails the component with VS_ISCORRUPT
                # before the access mode is ever consulted.
                out.append(self._prop(pad + "    ", "System.String", "OpenRowset", "",
                                      "Specifies the name of the database object used to open a rowset."))
                out.append(self._prop(pad + "    ", "System.String", "OpenRowsetVariable", "",
                                      "Specifies the variable that contains the name of the database object used to open a rowset."))
                out.append(self._prop(pad + "    ", "System.String", "SqlCommand", comp["sql"],
                                      "The SQL command to be executed."))
                out.append(self._prop(pad + "    ", "System.String", "SqlCommandVariable", "",
                                      "The variable that contains the SQL command to be executed."))
                out.append(self._prop(pad + "    ", "System.Int32", "DefaultCodePage", 1252,
                                      "Specifies the column code page to use when code page information is unavailable."))
                out.append(self._prop(pad + "    ", "System.Boolean", "AlwaysUseDefaultCodePage", "false",
                                      "Forces the use of the DefaultCodePage property value when describing character data."))
                out.append(self._prop(pad + "    ", "System.Int32", "AccessMode", 2,
                                      "Specifies the mode used to access the database."))
                out.append(self._prop(pad + "    ", "System.String", "ParameterMapping",
                                      comp.get("parameter_mapping", ""),
                                      "The mapping from parameter to variables."))
            else:
                out.append(self._prop(pad + "    ", "System.Boolean", "RetainNulls", "false",
                                      "Specifies whether zero-length strings are converted to nulls."))
                out.append(self._prop(pad + "    ", "System.String", "FileNameColumnName", "SourceFileName",
                                      "Specifies the name of the output column containing the file name."))
            out.append("%s  </properties>" % pad)
            out.append("%s  <connections>" % pad)
            scope = comp.get("connection_scope", "Project")
            out.append(
                '%s    <connection refId=%s connectionManagerID=%s connectionManagerRefId=%s description="The connection used to access the source." name="%s" />'
                % (pad, quoteattr(ref + ".Connections[%s]" % ("FlatFileConnection" if is_flat else "OleDbConnection")),
                   quoteattr(connection_manager_id(comp["connection"], scope)),
                   quoteattr(connection_manager_ref_id(comp["connection"], scope)),
                   "FlatFileConnection" if is_flat else "OleDbConnection")
            )
            out.append("%s  </connections>" % pad)
            out.append("%s  <outputs>" % pad)
            output_name = "Flat File Source Output" if is_flat else "OLE DB Source Output"
            error_name = "Flat File Source Error Output" if is_flat else "OLE DB Source Error Output"
            out.append('%s    <output refId=%s name="%s">' % (pad, quoteattr("%s.Outputs[%s]" % (ref, output_name)), output_name))
            out.append("%s      <outputColumns>" % pad)
            for col in comp["columns"]:
                out.append(
                    '%s        <outputColumn refId=%s %s errorOrTruncationOperation="Conversion" errorRowDisposition="RedirectRow" externalMetadataColumnId=%s lineageId=%s name=%s truncationRowDisposition="RedirectRow"%s'
                    % (pad, quoteattr("%s.Outputs[%s].Columns[%s]" % (ref, output_name, col.name)),
                       col.metadata_attrs(),
                       quoteattr("%s.Outputs[%s].ExternalColumns[%s]" % (ref, output_name, col.name)),
                       quoteattr("%s.Outputs[%s].Columns[%s]" % (ref, output_name, col.name)),
                       quoteattr(col.name),
                       ">" if is_flat else " />")
                )
                if is_flat:
                    # The flat file source asks each of its output columns for
                    # the per-column parsing properties by name and fails the
                    # whole component with VS_ISCORRUPT when one is absent.
                    out.append("%s          <properties>" % pad)
                    out.append(self._prop(pad + "            ", "System.Boolean", "FastParse", "false",
                                          "Indicates whether the column uses the faster, locale-neutral parsing routines."))
                    out.append(self._prop(pad + "            ", "System.Boolean", "UseBinaryFormat", "false",
                                          "Indicates whether the data is in binary format."))
                    out.append("%s          </properties>" % pad)
                    out.append("%s        </outputColumn>" % pad)
            out.append("%s      </outputColumns>" % pad)
            out.append("%s      <externalMetadataColumns isUsed=\"True\">" % pad)
            for col in comp["columns"]:
                out.append(
                    '%s        <externalMetadataColumn refId=%s %s name=%s />'
                    % (pad, quoteattr("%s.Outputs[%s].ExternalColumns[%s]" % (ref, output_name, col.name)),
                       col.metadata_attrs(), quoteattr(col.name))
                )
            out.append("%s      </externalMetadataColumns>" % pad)
            out.append("%s    </output>" % pad)
            out.append('%s    <output refId=%s isErrorOut="true" name="%s">' % (pad, quoteattr("%s.Outputs[%s]" % (ref, error_name)), error_name))
            out.append("%s      <outputColumns>" % pad)
            error_columns = ([text_col(FLAT_FILE_ERROR_COLUMN, comp["code_page"])] if is_flat
                             else list(comp["columns"]))
            for col in error_columns:
                out.append(
                    '%s        <outputColumn refId=%s %s%s lineageId=%s name=%s />'
                    % (pad, quoteattr("%s.Outputs[%s].Columns[%s]" % (ref, error_name, col.name)),
                       col.metadata_attrs(),
                       ' description=%s' % quoteattr(col.name) if is_flat else "",
                       quoteattr("%s.Outputs[%s].Columns[%s]" % (ref, error_name, col.name)),
                       quoteattr(col.name))
                )
            for extra in ("ErrorCode", "ErrorColumn"):
                out.append(
                    '%s        <outputColumn refId=%s dataType="i4" lineageId=%s name="%s" specialFlags="1" />'
                    % (pad, quoteattr("%s.Outputs[%s].Columns[%s]" % (ref, error_name, extra)),
                       quoteattr("%s.Outputs[%s].Columns[%s]" % (ref, error_name, extra)), extra)
                )
            out.append("%s      </outputColumns>" % pad)
            out.append("%s      <externalMetadataColumns />" % pad)
            out.append("%s    </output>" % pad)
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "derived":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.DerivedColumn" description="Derived Column" name=%s usesDispositions="true">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            lineage = lineage_of("Derived Column Input")
            upstream = upstream_cols("Derived Column Input")
            replacements = [(col_name, expr) for col_name, expr, _col in comp["derivations"] if col_name in lineage]
            replaced = set(col_name for col_name, _expr in replacements)
            read = self._read_columns([expr for _name, expr, _col in comp["derivations"]], upstream, replaced)
            # A replaced column is written in place, so its cache describes the
            # column arriving on the input - the upstream type, not the type the
            # derivation names. An input column carrying only a cachedName has no
            # cache the component can compare against the buffer, which fails
            # validation with VS_NEEDSNEWMETADATA before a row moves.
            upstream_by_name = dict((col_name, col) for col_name, _rid, col in upstream)
            out.append("%s  <inputs>" % pad)
            if replacements or read:
                out.append('%s    <input refId=%s name="Derived Column Input">' % (pad, quoteattr("%s.Inputs[Derived Column Input]" % ref)))
                out.append("%s      <inputColumns>" % pad)
                for col_name, col in read:
                    out.append('%s        <inputColumn refId=%s %s lineageId=%s name=%s usageType="readOnly" />'
                               % (pad, quoteattr("%s.Inputs[Derived Column Input].Columns[%s]" % (ref, col_name)),
                                  col.cached_attrs(), quoteattr(lineage[col_name]), quoteattr(col_name)))
                for col_name, expr in replacements:
                    # A written column computes, so it needs the dispositions
                    # that say what a computation error does; without them the
                    # column validates as VS_ISCORRUPT: "has an invalid error
                    # or truncation row disposition".
                    out.append('%s        <inputColumn refId=%s %s errorOrTruncationOperation="Computation"'
                               ' errorRowDisposition="FailComponent" lineageId=%s name=%s'
                               ' truncationRowDisposition="FailComponent" usageType="readWrite">'
                               % (pad, quoteattr("%s.Inputs[Derived Column Input].Columns[%s]" % (ref, col_name)),
                                  upstream_by_name[col_name].cached_attrs(),
                                  quoteattr(lineage[col_name]), quoteattr(col_name)))
                    out.append("%s          <properties>" % pad)
                    out.append(self._prop(pad + "            ", "System.String", "Expression", expr,
                                          "The expression used to compute this column."))
                    out.append(self._prop(pad + "            ", "System.String", "FriendlyExpression", expr,
                                          "The expression as displayed in the designer."))
                    out.append("%s          </properties>" % pad)
                    out.append("%s        </inputColumn>" % pad)
                out.append("%s      </inputColumns>" % pad)
                out.append("%s    </input>" % pad)
            else:
                out.append('%s    <input refId=%s name="Derived Column Input" />' % (pad, quoteattr("%s.Inputs[Derived Column Input]" % ref)))
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s exclusionGroup="1" name="Derived Column Output" synchronousInputId=%s>'
                       % (pad, quoteattr("%s.Outputs[Derived Column Output]" % ref), quoteattr("%s.Inputs[Derived Column Input]" % ref)))
            out.append("%s      <outputColumns>" % pad)
            for col_name, expr, col in comp["derivations"]:
                if col_name in lineage:
                    continue
                col_ref = "%s.Outputs[Derived Column Output].Columns[%s]" % (ref, col_name)
                out.append('%s        <outputColumn refId=%s %s errorOrTruncationOperation="Computation" errorRowDisposition="FailComponent" lineageId=%s name=%s truncationRowDisposition="FailComponent">'
                           % (pad, quoteattr(col_ref), col.metadata_attrs(), quoteattr(col_ref), quoteattr(col_name)))
                out.append("%s          <properties>" % pad)
                out.append(self._prop(pad + "            ", "System.String", "Expression", expr,
                                      "The expression used to compute this column."))
                out.append(self._prop(pad + "            ", "System.String", "FriendlyExpression", expr,
                                      "The expression as displayed in the designer."))
                out.append("%s          </properties>" % pad)
                out.append("%s        </outputColumn>" % pad)
            out.append("%s      </outputColumns>" % pad)
            out.append("%s      <externalMetadataColumns />" % pad)
            out.append("%s    </output>" % pad)
            out.append(self._error_output_xml(pad, ref, "Derived Column"))
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "lookup":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.Lookup" description="Lookup" name=%s usesDispositions="true" version="6">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <properties>" % pad)
            # Like the OLE DB source, the lookup reads its whole property set by
            # name while it validates, so every property the component gives
            # itself has to be persisted even where the default is wanted: the
            # first one missing fails the component with DTS_E_ELEMENTNOTFOUND
            # (0xC0010009) before the reference query is looked at.
            out.append(self._prop(pad + "    ", "System.String", "SqlCommand", comp["sql"],
                                  "The SQL command used to populate the lookup cache."))
            out.append(self._prop(pad + "    ", "System.String", "SqlCommandParam", "",
                                  "The parameterized SQL command used to populate the lookup cache."))
            out.append(self._prop(pad + "    ", "System.Int32", "ConnectionType", 0,
                                  "Specifies whether the reference rows come from a database or a cache."))
            # The component knows two behaviours: 0 treats a miss as an error
            # row - what the match output's disposition then decides the fate of
            # - and 1 sends it to the no match output, which leaves nothing for
            # that disposition to say.
            redirects = comp["no_match"] == "RD"
            out.append(self._prop(pad + "    ", "System.Int32", "NoMatchBehavior",
                                  1 if redirects else 0,
                                  "Determines the behaviour when a lookup finds no match."))
            out.append(self._prop(pad + "    ", "System.Int32", "NoMatchCachePercentage", 0,
                                  "The share of the cache held for rows with no matching entry."))
            out.append(self._prop(pad + "    ", "System.Int32", "CacheType", 0, "Full cache."))
            out.append(self._prop(pad + "    ", "System.Int32", "MaxMemoryUsage", 25,
                                  "The maximum cache size, in megabytes, of the 32-bit runtime."))
            out.append(self._prop(pad + "    ", "System.Int64", "MaxMemoryUsage64", 25,
                                  "The maximum cache size, in megabytes, of the 64-bit runtime."))
            out.append(self._prop(pad + "    ", "System.String", "ReferenceMetadataXml", "",
                                  "The cached description of the reference query's columns."))
            out.append(self._prop(pad + "    ", "System.String", "ParameterMap", "",
                                  "The input columns bound to the parameters of the reference query."))
            out.append(self._prop(pad + "    ", "System.Int32", "DefaultCodePage", 1252,
                                  "Specifies the code page to use when code page information is unavailable."))
            out.append(self._prop(pad + "    ", "System.Boolean", "TreatDuplicateKeysAsError", "false",
                                  "Whether duplicate keys in the reference rows fail the component."))
            out.append("%s  </properties>" % pad)
            out.append("%s  <connections>" % pad)
            out.append(
                '%s    <connection refId=%s connectionManagerID=%s connectionManagerRefId=%s name="OleDbConnection" />'
                % (pad, quoteattr(ref + ".Connections[OleDbConnection]"),
                   quoteattr(connection_manager_id(comp["connection"])),
                   quoteattr(connection_manager_ref_id(comp["connection"])))
            )
            out.append("%s  </connections>" % pad)
            out.append("%s  <inputs>" % pad)
            # The lookup carries its dispositions on the match output, not on
            # the input: a disposition on the input is rejected outright.
            out.append('%s    <input refId=%s name="Lookup Input">'
                       % (pad, quoteattr("%s.Inputs[Lookup Input]" % ref)))
            out.append("%s      <inputColumns>" % pad)
            lineage = lineage_of("Lookup Input")
            join_columns = column_of("Lookup Input")
            for jc in comp["join_columns"]:
                if jc not in lineage:
                    continue
                # joinToReferenceColumn names the reference column the key is
                # matched against; without it the component has no join at all
                # and answers 0xC0010009 at validation.
                out.append('%s        <inputColumn refId=%s %s externalMetadataColumnId=%s joinToReferenceColumn=%s lineageId=%s name=%s />'
                           % (pad, quoteattr("%s.Inputs[Lookup Input].Columns[%s]" % (ref, jc)),
                              join_columns[jc].cached_attrs(),
                              quoteattr("%s.Inputs[Lookup Input].ExternalColumns[%s]" % (ref, jc)),
                              quoteattr(jc),
                              quoteattr(lineage[jc]), quoteattr(jc)))
            out.append("%s      </inputColumns>" % pad)
            # The reference set the join and the copies name has to be described
            # on the input, or the component resolves them against an empty
            # collection and returns 0xC0010009.
            out.append('%s      <externalMetadataColumns isUsed="True">' % pad)
            for jc in comp["join_columns"]:
                if jc not in lineage:
                    continue
                out.append('%s        <externalMetadataColumn refId=%s %s name=%s />'
                           % (pad, quoteattr("%s.Inputs[Lookup Input].ExternalColumns[%s]" % (ref, jc)),
                              join_columns[jc].metadata_attrs(), quoteattr(jc)))
            for col in comp["output_columns"]:
                out.append('%s        <externalMetadataColumn refId=%s %s name=%s />'
                           % (pad, quoteattr("%s.Inputs[Lookup Input].ExternalColumns[%s]"
                                             % (ref, col.name)),
                              col.metadata_attrs(), quoteattr(col.name)))
            out.append("%s      </externalMetadataColumns>" % pad)
            out.append("%s    </input>" % pad)
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s errorOrTruncationOperation="Lookup" errorRowDisposition="%s" exclusionGroup="1" name="Lookup Match Output" synchronousInputId=%s>'
                       % (pad, quoteattr("%s.Outputs[Lookup Match Output]" % ref),
                          "NotUsed" if redirects
                          else ("IgnoreFailure" if comp["no_match"] == "IG" else "FailComponent"),
                          quoteattr("%s.Inputs[Lookup Input]" % ref)))
            out.append("%s      <outputColumns>" % pad)
            for col in comp["output_columns"]:
                col_ref = "%s.Outputs[Lookup Match Output].Columns[%s]" % (ref, col.name)
                # The lookup names the reference column it copies in a property
                # of the output column, not in an attribute of it, and treats a
                # copied column that carries no such property as corrupt. The
                # copy can only truncate, so the column carries a truncation
                # disposition and no error disposition.
                out.append('%s        <outputColumn refId=%s %s '
                           'errorOrTruncationOperation="Copy Column" '
                           'lineageId=%s name=%s truncationRowDisposition="FailComponent">'
                           % (pad, quoteattr(col_ref), col.metadata_attrs(),
                              quoteattr(col_ref), quoteattr(col.name)))
                out.append("%s          <properties>" % pad)
                out.append(self._prop(pad + "            ", "System.String",
                                      "CopyFromReferenceColumn", col.name,
                                      "Specifies the column in the reference table "
                                      "from which a column is copied."))
                out.append("%s          </properties>" % pad)
                out.append("%s        </outputColumn>" % pad)
            out.append("%s      </outputColumns>" % pad)
            out.append("%s      <externalMetadataColumns />" % pad)
            out.append("%s    </output>" % pad)
            out.append('%s    <output refId=%s exclusionGroup="1" name="Lookup No Match Output" synchronousInputId=%s />'
                       % (pad, quoteattr("%s.Outputs[Lookup No Match Output]" % ref), quoteattr("%s.Inputs[Lookup Input]" % ref)))
            out.append('%s    <output refId=%s exclusionGroup="1" isErrorOut="true" name="Lookup Error Output" synchronousInputId=%s>'
                       % (pad, quoteattr("%s.Outputs[Lookup Error Output]" % ref), quoteattr("%s.Inputs[Lookup Input]" % ref)))
            out.append("%s      <outputColumns>" % pad)
            for col, flag in ((int_col("ErrorCode"), 1), (int_col("ErrorColumn"), 2)):
                col_ref = "%s.Outputs[Lookup Error Output].Columns[%s]" % (ref, col.name)
                out.append('%s        <outputColumn refId=%s %s lineageId=%s name=%s specialFlags="%d" />'
                           % (pad, quoteattr(col_ref), col.metadata_attrs(), quoteattr(col_ref),
                              quoteattr(col.name), flag))
            out.append("%s      </outputColumns>" % pad)
            out.append("%s    </output>" % pad)
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "split":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.ConditionalSplit" description="Conditional Split" name=%s usesDispositions="true">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            lineage = lineage_of("Conditional Split Input")
            read = self._read_columns([expr for _name, expr in comp["cases"]],
                                      upstream_cols("Conditional Split Input"), set())
            out.append("%s  <inputs>" % pad)
            if read:
                out.append('%s    <input refId=%s name="Conditional Split Input">' % (pad, quoteattr("%s.Inputs[Conditional Split Input]" % ref)))
                out.append("%s      <inputColumns>" % pad)
                for col_name, col in read:
                    out.append('%s        <inputColumn refId=%s %s lineageId=%s name=%s usageType="readOnly" />'
                               % (pad, quoteattr("%s.Inputs[Conditional Split Input].Columns[%s]" % (ref, col_name)),
                                  col.cached_attrs(), quoteattr(lineage[col_name]), quoteattr(col_name)))
                out.append("%s      </inputColumns>" % pad)
                out.append("%s    </input>" % pad)
            else:
                out.append('%s    <input refId=%s name="Conditional Split Input" />' % (pad, quoteattr("%s.Inputs[Conditional Split Input]" % ref)))
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            for order, (out_name, expr) in enumerate(comp["cases"]):
                out.append('%s    <output refId=%s errorOrTruncationOperation="Computation" errorRowDisposition="FailComponent" exclusionGroup="1" name=%s synchronousInputId=%s truncationRowDisposition="FailComponent">'
                           % (pad, quoteattr("%s.Outputs[%s]" % (ref, out_name)), quoteattr(out_name),
                              quoteattr("%s.Inputs[Conditional Split Input]" % ref)))
                out.append("%s      <properties>" % pad)
                out.append(self._prop(pad + "        ", "System.String", "Expression", expr, "The condition for this output."))
                out.append(self._prop(pad + "        ", "System.String", "FriendlyExpression", expr, "The condition as displayed."))
                out.append(self._prop(pad + "        ", "System.Int32", "EvaluationOrder", order, "Evaluation order."))
                out.append("%s      </properties>" % pad)
                out.append("%s    </output>" % pad)
            # The component recognises its default output by the IsDefaultOut
            # custom property, not by an attribute; without it the output is
            # read as another condition and fails for having no Expression.
            out.append('%s    <output refId=%s exclusionGroup="1" name=%s synchronousInputId=%s>'
                       % (pad, quoteattr("%s.Outputs[%s]" % (ref, comp["default"])), quoteattr(comp["default"]),
                          quoteattr("%s.Inputs[Conditional Split Input]" % ref)))
            out.append("%s      <properties>" % pad)
            out.append(self._prop(pad + "        ", "System.Boolean", "IsDefaultOut", "true",
                                  "Specifies the default output."))
            out.append("%s      </properties>" % pad)
            out.append("%s    </output>" % pad)
            out.append(self._error_output_xml(pad, ref, "Conditional Split"))
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "rowcount":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.RowCount" description="Row Count" name=%s>'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <properties>" % pad)
            out.append(self._prop(pad + "    ", "System.String", "VariableName", comp["variable"],
                                  "The variable that receives the row count."))
            out.append("%s  </properties>" % pad)
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s name="Row Count Input" />' % (pad, quoteattr("%s.Inputs[Row Count Input]" % ref)))
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s name="Row Count Output" synchronousInputId=%s />'
                       % (pad, quoteattr("%s.Outputs[Row Count Output]" % ref), quoteattr("%s.Inputs[Row Count Input]" % ref)))
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "multicast":
            out.append('%s<component refId=%s componentClassID="Microsoft.Multicast" description="Multicast" name=%s>'
                       % (pad, quoteattr(ref), quoteattr(comp["name"])))
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s name="Multicast Input 1" />'
                       % (pad, quoteattr("%s.Inputs[Multicast Input 1]" % ref)))
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            for out_name in comp["outputs"]:
                out.append('%s    <output refId=%s name=%s synchronousInputId=%s />'
                           % (pad, quoteattr("%s.Outputs[%s]" % (ref, out_name)), quoteattr(out_name),
                              quoteattr("%s.Inputs[Multicast Input 1]" % ref)))
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "aggregate":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.Aggregate" description="Aggregate" name=%s version="3">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <properties>" % pad)
            out.append(self._prop(pad + "    ", "System.UInt32", "KeyScale", 0,
                                  "The approximate number of groups the component sizes its cache for."))
            out.append(self._prop(pad + "    ", "System.UInt32", "Keys", 0,
                                  "The exact number of groups the component sizes its cache for."))
            out.append(self._prop(pad + "    ", "System.UInt32", "CountDistinctScale", 0,
                                  "The approximate number of distinct values a count distinct is sized for."))
            out.append(self._prop(pad + "    ", "System.UInt32", "CountDistinctKeys", 0,
                                  "The exact number of distinct values a count distinct is sized for."))
            out.append(self._prop(pad + "    ", "System.Int32", "AutoExtendFactor", 25,
                                  "The share by which the cache may grow."))
            out.append("%s  </properties>" % pad)
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s name="Aggregate Input 1">' % (pad, quoteattr("%s.Inputs[Aggregate Input 1]" % ref)))
            out.append("%s      <inputColumns>" % pad)
            lineage = lineage_of("Aggregate Input 1")
            aggregate_columns = column_of("Aggregate Input 1")
            for gb in comp["group_by"]:
                if gb not in lineage:
                    continue
                out.append('%s        <inputColumn refId=%s %s lineageId=%s name=%s>'
                           % (pad, quoteattr("%s.Inputs[Aggregate Input 1].Columns[%s]" % (ref, gb)),
                              aggregate_columns[gb].cached_attrs(),
                              quoteattr(lineage[gb]), quoteattr(gb)))
                out.append("%s          <properties>" % pad)
                out.append(self._prop(pad + "            ", "System.Int32", "AggregationType", 0, "Group by."))
                out.append("%s          </properties>" % pad)
                out.append("%s        </inputColumn>" % pad)
            emitted = set(comp["group_by"])
            for src, dest, op in comp["aggregations"]:
                # An input column appears once in the collection even when it is
                # both grouped on and aggregated.
                if src not in lineage or src in emitted:
                    continue
                emitted.add(src)
                out.append('%s        <inputColumn refId=%s %s lineageId=%s name=%s>'
                           % (pad, quoteattr("%s.Inputs[Aggregate Input 1].Columns[%s]" % (ref, src)),
                              aggregate_columns[src].cached_attrs(),
                              quoteattr(lineage[src]), quoteattr(src)))
                out.append("%s          <properties>" % pad)
                out.append(self._prop(pad + "            ", "System.Int32", "AggregationType",
                                      aggregation_type(op), op))
                out.append(self._prop(pad + "            ", "System.String", "AggregationColumnName", dest, "Output column."))
                out.append("%s          </properties>" % pad)
                out.append("%s        </inputColumn>" % pad)
            out.append("%s      </inputColumns>" % pad)
            out.append("%s    </input>" % pad)
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s name="Aggregate Output 1">' % (pad, quoteattr("%s.Outputs[Aggregate Output 1]" % ref)))
            out.append("%s      <properties>" % pad)
            out.append(self._prop(pad + "        ", "System.UInt32", "KeyScale", 0,
                                  "The approximate number of groups this output is sized for."))
            out.append(self._prop(pad + "        ", "System.UInt32", "Keys", 0,
                                  "The exact number of groups this output is sized for."))
            out.append("%s      </properties>" % pad)
            out.append("%s      <outputColumns>" % pad)
            # Each output column names the input column it summarises and the
            # summary it is: the component reads both off the output column and
            # refuses one that does not carry them.
            aggregation_of = dict((gb, (gb, 0)) for gb in comp["group_by"])
            for src, dest, op in comp["aggregations"]:
                aggregation_of[dest] = (src, aggregation_type(op))
            for col_name, col_ref, col in outputs[(comp["name"], "Aggregate Output 1")]:
                src, summary = aggregation_of.get(col_name, (col_name, 0))
                if src not in lineage:
                    continue
                out.append('%s        <outputColumn refId=%s %s lineageId=%s name=%s>'
                           % (pad, quoteattr(col_ref), col.metadata_attrs(), quoteattr(col_ref), quoteattr(col_name)))
                out.append("%s          <properties>" % pad)
                out.append(
                    '%s            <property containsID="true" dataType="System.Int32" '
                    'description="The input column this column summarises." '
                    'name="AggregationColumnId">%s</property>'
                    % (pad, escape("#{%s}" % lineage[src])))
                out.append(self._prop(pad + "            ", "System.Int32", "AggregationType",
                                      summary, "The aggregation this column carries."))
                # A count or sum is big when it is carried in an eight byte
                # integer, and the aggregate rejects a column whose type and
                # this flag disagree.
                out.append(self._prop(pad + "            ", "System.Int32", "IsBig",
                                      1 if col.dtype in ("i8", "ui8") else 0,
                                      "Whether the column may hold a value beyond the range of a four byte integer."))
                out.append(self._prop(pad + "            ", "System.Int32", "AggregationComparisonFlags", 0,
                                      "The string comparison options used when grouping."))
                out.append("%s          </properties>" % pad)
                out.append("%s        </outputColumn>" % pad)
            out.append("%s      </outputColumns>" % pad)
            out.append("%s      <externalMetadataColumns />" % pad)
            out.append("%s    </output>" % pad)
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "sort":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.Sort" description="Sort" name=%s>'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <properties>" % pad)
            out.append(self._prop(pad + "    ", "System.Boolean", "EliminateDuplicates",
                                  "true" if comp["dedupe"] else "false", "Remove duplicate rows."))
            out.append(self._prop(pad + "    ", "System.Int32", "MaximumThreads", -1,
                                  "The number of threads the sort may use, or -1 for as many as it likes."))
            out.append("%s  </properties>" % pad)
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s name="Sort Input">' % (pad, quoteattr("%s.Inputs[Sort Input]" % ref)))
            out.append("%s      <inputColumns>" % pad)
            sort_positions = dict((sc, order) for order, sc in enumerate(comp["sort_columns"], start=1))
            for col_name, col_ref, col in upstream_cols("Sort Input"):
                out.append('%s        <inputColumn refId=%s %s lineageId=%s name=%s>'
                           % (pad, quoteattr("%s.Inputs[Sort Input].Columns[%s]" % (ref, col_name)),
                              col.cached_attrs(), quoteattr(col_ref), quoteattr(col_name)))
                out.append("%s          <properties>" % pad)
                out.append(self._prop(pad + "            ", "System.Int32", "NewSortKeyPosition",
                                      sort_positions.get(col_name, 0), "Sort key position."))
                out.append("%s          </properties>" % pad)
                out.append("%s        </inputColumn>" % pad)
            out.append("%s      </inputColumns>" % pad)
            out.append("%s    </input>" % pad)
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s name="Sort Output">' % (pad, quoteattr("%s.Outputs[Sort Output]" % ref)))
            out.append("%s      <outputColumns>" % pad)
            # The sort copies each input column through, and it finds the column
            # to copy by the SortColumnId the output column carries.
            sort_lineage = lineage_of("Sort Input")
            for col_name, col_ref, col in outputs[(comp["name"], "Sort Output")]:
                input_ref = "%s.Inputs[Sort Input].Columns[%s]" % (ref, col_name)
                out.append('%s        <outputColumn refId=%s %s lineageId=%s name=%s sourceColumn=%s>'
                           % (pad, quoteattr(col_ref), col.metadata_attrs(), quoteattr(col_ref), quoteattr(col_name),
                              quoteattr(input_ref)))
                out.append("%s          <properties>" % pad)
                out.append(
                    '%s            <property containsID="true" dataType="System.Int32" '
                    'description="The input column this column is copied from." '
                    'name="SortColumnId">%s</property>'
                    % (pad, escape("#{%s}" % sort_lineage[col_name])))
                out.append("%s          </properties>" % pad)
                out.append("%s        </outputColumn>" % pad)
            out.append("%s      </outputColumns>" % pad)
            out.append("%s      <externalMetadataColumns />" % pad)
            out.append("%s    </output>" % pad)
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "union":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.UnionAll" description="Union All" name=%s>'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s hasSideEffects="true" name="Union All Input 1">'
                       % (pad, quoteattr("%s.Inputs[Union All Input 1]" % ref)))
            out.append("%s      <inputColumns>" % pad)
            for col_name, col_ref, col in upstream_cols("Union All Input 1"):
                out.append('%s        <inputColumn refId=%s %s lineageId=%s name=%s>'
                           % (pad, quoteattr("%s.Inputs[Union All Input 1].Columns[%s]" % (ref, col_name)),
                              col.cached_attrs(), quoteattr(col_ref), quoteattr(col_name)))
                out.append("%s          <properties>" % pad)
                out.append(self._prop(pad + "            ", "System.String", "OutputColumnLineageID",
                                      "%s.Outputs[Union All Output 1].Columns[%s]" % (ref, col_name),
                                      "The output column this input column maps to."))
                out.append("%s          </properties>" % pad)
                out.append("%s        </inputColumn>" % pad)
            out.append("%s      </inputColumns>" % pad)
            out.append("%s    </input>" % pad)
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s name="Union All Output 1">' % (pad, quoteattr("%s.Outputs[Union All Output 1]" % ref)))
            out.append("%s      <outputColumns>" % pad)
            for col_name, col_ref, col in outputs[(comp["name"], "Union All Output 1")]:
                out.append('%s        <outputColumn refId=%s %s lineageId=%s name=%s />'
                           % (pad, quoteattr(col_ref), col.metadata_attrs(), quoteattr(col_ref), quoteattr(col_name)))
            out.append("%s      </outputColumns>" % pad)
            out.append("%s      <externalMetadataColumns />" % pad)
            out.append("%s    </output>" % pad)
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "convert":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.DataConvert" description="Data Conversion" name=%s usesDispositions="true">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s name="Data Conversion Input">' % (pad, quoteattr("%s.Inputs[Data Conversion Input]" % ref)))
            out.append("%s      <inputColumns>" % pad)
            lineage = lineage_of("Data Conversion Input")
            arriving = dict((col_name, col) for col_name, _rid, col
                            in upstream_cols("Data Conversion Input"))
            for src, dest, col in comp["conversions"]:
                if src not in lineage:
                    continue
                out.append('%s        <inputColumn refId=%s %s lineageId=%s name=%s />'
                           % (pad, quoteattr("%s.Inputs[Data Conversion Input].Columns[%s]" % (ref, src)),
                              arriving[src].cached_attrs(), quoteattr(lineage[src]), quoteattr(src)))
            out.append("%s      </inputColumns>" % pad)
            out.append("%s    </input>" % pad)
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s exclusionGroup="1" name="Data Conversion Output" synchronousInputId=%s>'
                       % (pad, quoteattr("%s.Outputs[Data Conversion Output]" % ref), quoteattr("%s.Inputs[Data Conversion Input]" % ref)))
            out.append("%s      <outputColumns>" % pad)
            for src, dest, col in comp["conversions"]:
                if dest in lineage:
                    continue
                col_ref = "%s.Outputs[Data Conversion Output].Columns[%s]" % (ref, dest)
                # The property holds a lineage ID, and an input column's lineage
                # is the upstream output column it reads - its own refId is a
                # different ID the component cannot resolve: 'Cannot find input
                # column with lineage ID ...'.
                src_ref = lineage[src]
                out.append('%s        <outputColumn refId=%s %s errorOrTruncationOperation="Conversion" errorRowDisposition="FailComponent" lineageId=%s name=%s truncationRowDisposition="FailComponent">'
                           % (pad, quoteattr(col_ref), col.metadata_attrs(), quoteattr(col_ref), quoteattr(dest)))
                out.append("%s          <properties>" % pad)
                # The component reads the column to convert from this property,
                # not from a sourceColumn attribute, and requires it by name.
                out.append('%s            <property containsID="true" dataType="System.Int32" description="The lineage ID of the input column." name="SourceInputColumnLineageID">%s</property>'
                           % (pad, escape("#{%s}" % src_ref)))
                out.append(self._prop(pad + "            ", "System.Boolean", "FastParse", "false",
                                      "Indicates whether the column uses the faster, locale-neutral parsing routines."))
                out.append("%s          </properties>" % pad)
                out.append("%s        </outputColumn>" % pad)
            out.append("%s      </outputColumns>" % pad)
            out.append("%s      <externalMetadataColumns />" % pad)
            out.append("%s    </output>" % pad)
            out.append(self._error_output_xml(pad, ref, "Data Conversion"))
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "oledb_dest":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.OLEDBDestination" contactInfo="OLE DB Destination" description="OLE DB Destination" name=%s usesDispositions="true" version="4">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <properties>" % pad)
            out.append(self._prop(pad + "    ", "System.Int32", "CommandTimeout", 0, "Command timeout in seconds."))
            out.append(self._prop(pad + "    ", "System.String", "OpenRowset", comp["table"],
                                  "Specifies the name of the database object used to open a rowset."))
            # Like the OLE DB source, the destination looks every custom
            # property up by name during validation regardless of access mode.
            out.append(self._prop(pad + "    ", "System.String", "OpenRowsetVariable", "",
                                  "Specifies the variable that contains the name of the database object used to open a rowset."))
            out.append(self._prop(pad + "    ", "System.String", "SqlCommand", "",
                                  "The SQL command to be executed."))
            out.append(self._prop(pad + "    ", "System.Boolean", "AlwaysUseDefaultCodePage", "false",
                                  "Forces the use of the DefaultCodePage property value when describing character data."))
            out.append(self._prop(pad + "    ", "System.Boolean", "FastLoadKeepNulls", "false",
                                  "Indicates whether the columns containing null will have null inserted in the destination."))
            out.append(self._prop(pad + "    ", "System.Int32", "AccessMode", 3 if comp["fast_load"] else 0,
                                  "Specifies the mode used to access the database."))
            out.append(self._prop(pad + "    ", "System.Boolean", "FastLoadKeepIdentity",
                                  "true" if comp["keep_identity"] else "false", "Keep identity values."))
            out.append(self._prop(pad + "    ", "System.String", "FastLoadOptions", "TABLOCK,CHECK_CONSTRAINTS",
                                  "Options used with fast load."))
            out.append(self._prop(pad + "    ", "System.Int32", "FastLoadMaxInsertCommitSize", comp["batch_size"],
                                  "Commit size during data insertion."))
            out.append(self._prop(pad + "    ", "System.Int32", "DefaultCodePage", 1252, "Default code page."))
            out.append("%s  </properties>" % pad)
            out.append("%s  <connections>" % pad)
            out.append(
                '%s    <connection refId=%s connectionManagerID=%s connectionManagerRefId=%s description="The OLE DB runtime connection used to access the database." name="OleDbConnection" />'
                % (pad, quoteattr(ref + ".Connections[OleDbConnection]"),
                   quoteattr(connection_manager_id(comp["connection"])),
                   quoteattr(connection_manager_ref_id(comp["connection"])))
            )
            out.append("%s  </connections>" % pad)
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s errorOrTruncationOperation="Insert" errorRowDisposition="%s" hasSideEffects="true" name="OLE DB Destination Input">'
                       % (pad, quoteattr("%s.Inputs[OLE DB Destination Input]" % ref), comp["error_disposition"]))
            # External metadata describes the *table*, which is what the
            # destination revalidates against when the package starts: metadata
            # copied from the buffer fails with VS_NEEDSNEWMETADATA before a row
            # moves. Buffer columns are mapped onto it by name, or by the pairs
            # the spec supplied where the names differ.
            lineage = lineage_of("OLE DB Destination Input")
            loaded = self._destination_contract(comp, lineage)
            out.append("%s      <inputColumns>" % pad)
            for target, col in loaded:
                if col is None:
                    continue
                out.append('%s        <inputColumn refId=%s %s externalMetadataColumnId=%s lineageId=%s name=%s />'
                           % (pad, quoteattr("%s.Inputs[OLE DB Destination Input].Columns[%s]" % (ref, col.name)),
                              col.cached_attrs(),
                              quoteattr("%s.Inputs[OLE DB Destination Input].ExternalColumns[%s]" % (ref, target.name)),
                              quoteattr(lineage[col.name]),
                              quoteattr(col.name)))
            out.append("%s      </inputColumns>" % pad)
            out.append("%s      <externalMetadataColumns isUsed=\"True\">" % pad)
            for target, _col in loaded:
                out.append('%s        <externalMetadataColumn refId=%s %s name=%s />'
                           % (pad, quoteattr("%s.Inputs[OLE DB Destination Input].ExternalColumns[%s]" % (ref, target.name)),
                              target.metadata_attrs(), quoteattr(target.name)))
            out.append("%s      </externalMetadataColumns>" % pad)
            out.append("%s    </input>" % pad)
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append(self._error_output_xml(pad, ref, "OLE DB Destination"))
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        else:  # pragma: no cover - guarded by the builder API
            raise ValueError("unknown component kind %r" % kind)
        return out


# ---------------------------------------------------------------------------
# Control flow tasks
# ---------------------------------------------------------------------------


class Task:
    creation_name = ""
    executable_type = ""
    description = ""

    # A task whose work is addressed through variables the control flow fills
    # in at run time cannot be validated at package start; see FileSystemTask.
    delay_validation = False

    def __init__(self, name):
        self.name = safe_name(name)

    def object_data(self, ref, indent):  # pragma: no cover - overridden
        return []

    def bind(self, package):
        """Resolve package-scoped references. Overridden where needed."""
        return self

    def to_xml(self, parent_ref, indent):
        ref = "%s\\%s" % (parent_ref, self.name)
        pad = " " * indent
        out = [
            "%s<DTS:Executable" % pad,
            "%s  %s" % (pad, attr("DTS:refId", ref)),
            '%s  DTS:CreationName="%s"' % (pad, self.creation_name),
            "%s  %s" % (pad, attr("DTS:Description", self.description)),
            '%s  DTS:DTSID="%s"' % (pad, guid(ref)),
        ]
        if self.delay_validation:
            out.append('%s  DTS:DelayValidation="True"' % pad)
        out.extend([
            '%s  DTS:ExecutableType="%s"' % (pad, self.executable_type),
            '%s  DTS:LocaleID="-1"' % pad,
            "%s  %s" % (pad, attr("DTS:ObjectName", self.name)),
            '%s  DTS:ThreadHint="0">' % pad,
            "%s  <DTS:Variables />" % pad,
        ])
        body = self.object_data(ref, indent + 2)
        if body:
            out.append("%s  <DTS:ObjectData>" % pad)
            out.extend(body)
            out.append("%s  </DTS:ObjectData>" % pad)
        out.append("%s</DTS:Executable>" % pad)
        return out


class ExecuteSql(Task):
    creation_name = "Microsoft.ExecuteSQLTask"
    executable_type = "Microsoft.ExecuteSQLTask"
    description = "Execute SQL Task"

    def __init__(self, name, connection, sql, result_type="ResultSetType_None", parameter_bindings=None,
                 result_bindings=None, is_stored_procedure=False, timeout=0):
        Task.__init__(self, name)
        self.connection = connection
        self.sql = sql
        self.result_type = result_type
        # (variable, index, dtype) or (variable, index, dtype, direction)
        self.parameter_bindings = parameter_bindings or []
        self.result_bindings = result_bindings or []  # (result_name, variable)
        self.is_stored_procedure = is_stored_procedure
        self.timeout = timeout

    def object_data(self, ref, indent):
        pad = " " * indent
        out = [
            "%s<SQLTask:SqlTaskData" % pad,
            '%s  SQLTask:Connection="%s"' % (pad, guid("cm:" + self.connection)),
            "%s  %s" % (pad, attr("SQLTask:SqlStatementSource", self.sql)),
            '%s  SQLTask:ResultType="%s"' % (pad, self.result_type),
            '%s  SQLTask:TimeOut="%d"' % (pad, self.timeout),
            '%s  SQLTask:SqlStmtSourceType="DirectInput"' % pad,
            '%s  SQLTask:IsStoredProc="%s"' % (pad, "True" if self.is_stored_procedure else "False"),
            '%s  xmlns:SQLTask="www.microsoft.com/sqlserver/dts/tasks/sqltask">' % pad,
        ]
        for binding in self.parameter_bindings:
            var, index, dtype = binding[0], binding[1], binding[2]
            direction = binding[3] if len(binding) > 3 else "Input"
            out.append(
                '%s  <SQLTask:ParameterBinding SQLTask:ParameterName="%s" SQLTask:DtsVariableName="%s" '
                'SQLTask:ParameterDirection="%s" SQLTask:DataType="%d" SQLTask:ParameterSize="%d" />'
                % (pad, index, var, direction, SQLTASK_TYPES[dtype], SQLTASK_SIZES[dtype])
            )
        for result_name, var in self.result_bindings:
            out.append(
                '%s  <SQLTask:ResultBinding SQLTask:ResultName="%s" SQLTask:DtsVariableName="%s" />'
                % (pad, result_name, var)
            )
        out.append("%s</SQLTask:SqlTaskData>" % pad)
        return out


class Expression(Task):
    creation_name = "Microsoft.ExpressionTask"
    executable_type = "Microsoft.ExpressionTask"
    description = "Expression Task"

    def __init__(self, name, expression):
        Task.__init__(self, name)
        # One task holds one <ExpressionTask Expression="..." />, so several
        # assignments strung together with semicolons reach the evaluator as a
        # single expression and fail task validation before the task runs.
        # Use expression_sequence() for more than one assignment.
        if expression_statement_count(expression) > 1:
            raise ContractError(
                "expression task %r carries %d statements; the Expression Task "
                "evaluates one expression and refuses the ';' token"
                % (name, expression_statement_count(expression)))
        self.expression = expression

    def object_data(self, ref, indent):
        pad = " " * indent
        return ["%s<ExpressionTask %s />" % (pad, attr("Expression", self.expression))]


def expression_sequence(name, assignments, description=None):
    """Sequence container running one Expression Task per assignment, in order.

    ``assignments`` are expressions evaluated left to right, so a later one may
    read a variable an earlier one wrote.
    """
    container = Container(name, kind="sequence",
                          description=description or "Sequence Container")
    steps = []
    for index, assignment in enumerate(assignments, start=1):
        steps.append(container.add(Expression("%s %d" % (name, index), assignment)))
    container.chain(*steps)
    return container


class ExecutePackage(Task):
    creation_name = "Microsoft.ExecutePackageTask"
    executable_type = "Microsoft.ExecutePackageTask"
    description = "Execute Package Task"

    def __init__(self, name, package_name, parameter_assignments=None,
                 parent_project=None, child_project=None):
        """Execute a child package held in the *same* project.

        A project reference resolves the child by name inside the executing
        project, so it cannot reach a package deployed in another .ispac.
        Callers pass both projects and a cross-project pair is refused here
        rather than emitted as a reference that fails at run time; those
        dependencies belong in the orchestration plan the external runner
        executes (tools/ssisgen/orchestration.py).
        """
        Task.__init__(self, name)
        if parent_project is not None and child_project is not None and parent_project != child_project:
            raise ValueError(
                "Execute Package Task %r would reference %r across projects (%s -> %s); "
                "a UseProjectReference edge must stay inside one project"
                % (name, package_name, parent_project, child_project))
        self.package_name = package_name
        self.parameter_assignments = parameter_assignments or []  # (child_param, parent_variable)

    def object_data(self, ref, indent):
        pad = " " * indent
        out = [
            "%s<ExecutePackageTask>" % pad,
            "%s  <UseProjectReference>True</UseProjectReference>" % pad,
            "%s  <PackageName>%s.dtsx</PackageName>" % (pad, escape(self.package_name)),
        ]
        for child_param, parent_var in self.parameter_assignments:
            out.append("%s  <ParameterAssignment>" % pad)
            out.append("%s    <ParameterName>%s</ParameterName>" % (pad, escape(child_param)))
            out.append("%s    <BindedVariableOrParameterName>%s</BindedVariableOrParameterName>"
                       % (pad, escape(parent_var)))
            out.append("%s  </ParameterAssignment>" % pad)
        out.append("%s</ExecutePackageTask>" % pad)
        return out


class FileSystemTask(Task):
    """A File System Task that moves or copies one variable-addressed path.

    The persisted attribute names are the ones the task host itself writes:
    ``TaskOperationType``, ``TaskSourcePath``, ``TaskIsSourceVariable``,
    ``TaskDestinationPath`` and ``TaskIsDestinationVariable``. Any other spelling
    is well-formed XML that the task host does not recognise, so it loads with
    its defaults - operation CopyFile over two empty literal paths - and fails
    validation with '"DestinationPath" is not valid on operation type
    "CopyFile"'. Nothing but loading the package through the runtime catches
    that, which is what validation/static/Test-PackageRuntimeContracts.ps1 does.
    """

    creation_name = "Microsoft.FileSystemTask"
    executable_type = "Microsoft.FileSystemTask"
    description = "File System Task"

    # DTSFileSystemOperation, as the task host parses it.
    OPERATIONS = ("CopyFile", "CopyDirectory", "MoveFile", "MoveDirectory",
                  "DeleteFile", "DeleteDirectory", "DeleteDirectoryContent",
                  "RenameFile", "SetAttributes", "CreateDirectory")

    # Both paths are variables the control flow fills in per iteration, so the
    # task has to be validated when it runs. Validated at package start the
    # variables still hold their design-time empty string and the task host
    # fails with 'Variable "X" is used as a source or destination and is
    # empty.', which is how every ING_FILE_* archive step failed.
    delay_validation = True

    def __init__(self, name, operation, source_variable, destination_variable):
        Task.__init__(self, name)
        if operation not in self.OPERATIONS:
            raise ValueError(
                "file system task %r asks for operation %r; the task host only "
                "parses %s" % (name, operation, ", ".join(self.OPERATIONS)))
        self.operation = operation
        self.source_variable = source_variable
        self.destination_variable = destination_variable

    def object_data(self, ref, indent):
        pad = " " * indent
        return [
            '%s<FileSystemData TaskOperationType="%s" TaskSourcePath="%s" '
            'TaskIsSourceVariable="True" TaskDestinationPath="%s" '
            'TaskIsDestinationVariable="True" />'
            % (pad, self.operation, self.source_variable, self.destination_variable)
        ]


class DataFlowTask(Task):
    creation_name = "Microsoft.Pipeline"
    executable_type = "Microsoft.Pipeline"
    description = "Data Flow Task"

    def __init__(self, data_flow, delay_validation=False):
        Task.__init__(self, data_flow.name)
        self.data_flow = data_flow
        self.delay_validation = delay_validation

    def bind(self, package):
        self.data_flow.bind(package)
        return self

    def object_data(self, ref, indent):
        return self.data_flow.to_xml(ref, indent)


class Container:
    """Sequence container or Foreach loop holding child executables."""

    def __init__(self, name, kind="sequence", enumerator=None, variable_mappings=None, description=None,
                 folder_expression=None, delay_validation=None):
        self.name = safe_name(name)
        self.kind = kind
        self.enumerator = enumerator or {}
        # The enumerator's Folder is a literal the runtime never evaluates, so a
        # loop that has to follow a configured root carries a Directory property
        # expression on the enumerator as well; the literal stays as the
        # design-time value.
        if folder_expression and kind != "foreach":
            raise ContractError(
                "%s: a Directory expression only has a home on a Foreach loop's enumerator" % self.name)
        self.folder_expression = folder_expression
        self.variable_mappings = variable_mappings or []
        self.description = description or ("Sequence Container" if kind == "sequence" else "Foreach Loop Container")
        # A Foreach loop hands its children a file path that only exists once
        # the loop is iterating, so its children have to be validated then and
        # not at package start, where the mapped variables are still empty.
        self.delay_validation = (kind == "foreach") if delay_validation is None else delay_validation
        self.tasks = []
        self.constraints = []

    def add(self, task):
        self.tasks.append(task)
        return task

    def bind(self, package):
        for task in self.tasks:
            task.bind(package)
        return self

    def link(self, from_task, to_task, value="Success", expression=None, logical_and=True):
        self.constraints.append((from_task, to_task, value, expression, logical_and))

    def chain(self, *tasks):
        for a, b in zip(tasks, tasks[1:]):
            self.link(a, b)

    def to_xml(self, parent_ref, indent):
        ref = "%s\\%s" % (parent_ref, self.name)
        pad = " " * indent
        creation = "STOCK:SEQUENCE" if self.kind == "sequence" else "STOCK:FOREACHLOOP"
        out = [
            "%s<DTS:Executable" % pad,
            "%s  %s" % (pad, attr("DTS:refId", ref)),
            '%s  DTS:CreationName="%s"' % (pad, creation),
            "%s  %s" % (pad, attr("DTS:Description", self.description)),
            '%s  DTS:DTSID="%s"' % (pad, guid(ref)),
        ]
        if self.delay_validation:
            out.append('%s  DTS:DelayValidation="True"' % pad)
        out.extend([
            '%s  DTS:ExecutableType="%s"' % (pad, creation),
            '%s  DTS:LocaleID="-1"' % pad,
            "%s  %s>" % (pad, attr("DTS:ObjectName", self.name)),
        ])
        out.append("%s  <DTS:Variables />" % pad)
        if self.kind == "foreach":
            enumerator_ref = "%s.ForEachEnumerator" % ref
            out.append("%s  <DTS:ForEachEnumerator" % pad)
            out.append('%s    DTS:DTSID="%s"' % (pad, guid(enumerator_ref)))
            out.append('%s    DTS:ObjectName="%s"' % (pad, guid(enumerator_ref)))
            out.append('%s    DTS:CreationName="Microsoft.ForEachFileEnumerator">' % pad)
            # Directory is a property of the enumerator, not of the loop that
            # holds it: the loop has no such property, so an expression written
            # on the loop is dropped by the loader without a word and the loop
            # enumerates the enumerator's own default, C:\ *.*.
            if self.folder_expression:
                out.append('%s    <DTS:PropertyExpression DTS:Name="Directory">%s</DTS:PropertyExpression>'
                           % (pad, escape(self.folder_expression)))
            out.append("%s    <DTS:ObjectData>" % pad)
            out.append("%s      <ForEachFileEnumeratorProperties>" % pad)
            out.append('%s        <FEFEProperty Folder="%s" />'
                       % (pad, escape(self.enumerator.get("folder", ""))))
            out.append('%s        <FEFEProperty FileSpec="%s" />'
                       % (pad, escape(self.enumerator.get("file_spec", "*.csv"))))
            # 0 is the fully qualified name: every consumer of the mapped
            # variable treats it as a path.
            out.append('%s        <FEFEProperty FileNameRetrievalType="0" />' % pad)
            out.append('%s        <FEFEProperty Recurse="0" />' % pad)
            out.append("%s      </ForEachFileEnumeratorProperties>" % pad)
            out.append("%s    </DTS:ObjectData>" % pad)
            out.append("%s  </DTS:ForEachEnumerator>" % pad)
            if self.variable_mappings:
                out.append("%s  <DTS:ForEachVariableMappings>" % pad)
                for index, var in enumerate(self.variable_mappings):
                    out.append(
                        '%s    <DTS:ForEachVariableMapping DTS:refId="%s.ForEachVariableMapping[%d]" '
                        'DTS:VariableName="%s" DTS:ValueIndex="%d" />' % (pad, ref, index, var, index)
                    )
                out.append("%s  </DTS:ForEachVariableMappings>" % pad)
        out.append("%s  <DTS:Executables>" % pad)
        for task in self.tasks:
            out.extend(task.to_xml(ref, indent + 4))
        out.append("%s  </DTS:Executables>" % pad)
        out.extend(_constraints_xml(self.constraints, ref, indent + 2))
        out.append("%s</DTS:Executable>" % pad)
        return out


def _constraints_xml(constraints, parent_ref, indent):
    if not constraints:
        return []
    pad = " " * indent
    out = ["%s<DTS:PrecedenceConstraints>" % pad]
    for index, (src, dst, value, expression, logical_and) in enumerate(constraints):
        name = "Constraint" if index == 0 else "Constraint %d" % index
        cref = "%s.PrecedenceConstraints[%s]" % (parent_ref, name)
        out.append("%s  <DTS:PrecedenceConstraint" % pad)
        out.append("%s    %s" % (pad, attr("DTS:refId", cref)))
        out.append('%s    DTS:CreationName=""' % pad)
        out.append('%s    DTS:DTSID="%s"' % (pad, guid(cref)))
        out.append("%s    %s" % (pad, attr("DTS:From", "%s\\%s" % (parent_ref, src.name))))
        out.append('%s    DTS:LogicalAnd="%s"' % (pad, "True" if logical_and else "False"))
        out.append("%s    %s" % (pad, attr("DTS:ObjectName", name)))
        if value != "Success":
            out.append('%s    DTS:Value="%s"' % (pad, {"Failure": "1", "Completion": "2"}[value]))
        if expression:
            out.append('%s    DTS:EvalOp="%s"' % (pad, "3" if value != "Success" else "2"))
            out.append("%s    %s" % (pad, attr("DTS:Expression", expression)))
        out.append("%s    %s />" % (pad, attr("DTS:To", "%s\\%s" % (parent_ref, dst.name))))
    out.append("%s</DTS:PrecedenceConstraints>" % pad)
    return out


# ---------------------------------------------------------------------------
# Package
# ---------------------------------------------------------------------------


class Package:
    """An SSIS package.

    ``creation_date`` is a fixed literal so regeneration is byte-stable; it is
    the date the estate expansion was authored, not a build timestamp.
    """

    CREATION_DATE = "6/14/2011 9:02:41 AM"
    CREATOR = "WWI\\etl_build"
    COMPUTER = "WWIBUILD01"

    def __init__(self, name, description="", package_type="5", protection_level="0"):
        self.name = name
        self.description = description
        self.package_type = package_type
        self.protection_level = protection_level
        self.parameters = []  # (name, value, dtype, required, sensitive, description)
        self.variables = []  # (name, value, dtype, namespace, expression)
        self.connection_managers = []  # project-level connection manager names
        self.flat_file_connections = []  # package-level FLATFILE connection managers
        self.tasks = []
        self.constraints = []
        self.event_handlers = []  # (event_name, [tasks], [constraints])
        self.log_providers = []

    # -- declaration --------------------------------------------------------

    def add_parameter(self, name, value, dtype="string", required=False, sensitive=False, description=""):
        self.parameters.append((name, value, dtype, required, sensitive, description))
        return self

    def add_variable(self, name, value, dtype="string", namespace="User", expression=None):
        self.variables.append((name, value, dtype, namespace, expression))
        return self

    def use_connection(self, *names):
        for name in names:
            if name not in self.connection_managers:
                self.connection_managers.append(name)
        return self

    def add(self, task):
        self.tasks.append(task)
        return task

    def link(self, from_task, to_task, value="Success", expression=None, logical_and=True):
        self.constraints.append((from_task, to_task, value, expression, logical_and))

    def chain(self, *tasks):
        for a, b in zip(tasks, tasks[1:]):
            self.link(a, b)

    def add_event_handler(self, event_name, tasks, constraints=None):
        self.event_handlers.append((event_name, tasks, constraints or []))
        return self

    def variable_dtsid(self, name):
        """DTSID of the variable or package parameter *name* addresses.

        Components that persist a variable reference by id - an OLE DB source
        parameter mapping, for one - need the id the package will emit, and a
        reference to something the package does not declare would load as a
        dangling GUID, so the lookup is strict.
        """
        if name.startswith("$Package::"):
            short = name[len("$Package::"):]
            if any(entry[0] == short for entry in self.parameters):
                return guid("Package.Variables[$Package::%s]" % short + self.name)
            raise ContractError("package %r has no parameter %r to bind" % (self.name, short))
        namespace, separator, short = name.partition("::")
        if not separator:
            namespace, short = "User", name
        if namespace == "System":
            raise ContractError(
                "package %r binds system variable %r; the runtime owns its id, so it "
                "cannot be referenced by DTSID" % (self.name, name))
        for vname, _value, _dtype, vnamespace, _expression in self.variables:
            if vname == short and vnamespace == namespace:
                return guid("Package.Variables[%s::%s]" % (namespace, short) + self.name)
        raise ContractError("package %r has no variable %r to bind" % (self.name, name))

    def bind(self):
        """Let every executable resolve its package-scoped references."""
        for task in self.tasks:
            task.bind(self)
        for _event_name, tasks, _constraints in self.event_handlers:
            for task in tasks:
                task.bind(self)
        return self

    def add_flat_file_connection(self, name, columns, delimiter=",", code_page=1252,
                                 header_in_first_row=True, design_time_path="",
                                 expression="@[User::CurrentFilePath]", description=""):
        """A package-scoped flat file connection manager for one feed.

        A project connection manager cannot see a package variable, so the file
        the Foreach loop is currently on can only be bound here. The delimiter,
        code page and header flag come from config/landing-zone.yaml, which is
        what the provider actually sends.
        """
        self.flat_file_connections.append(
            dict(name=safe_name(name), columns=list(columns), delimiter=delimiter,
                 code_page=int(code_page), header_in_first_row=bool(header_in_first_row),
                 design_time_path=design_time_path, expression=expression,
                 description=description or ("Flat file connection for %s." % name)))
        return self.flat_file_connections[-1]["name"]

    def _flat_file_connections_xml(self):
        if not self.flat_file_connections:
            return []
        out = ["  <DTS:ConnectionManagers>"]
        for spec in self.flat_file_connections:
            name = spec["name"]
            cref = "Package.ConnectionManagers[%s]" % name
            out.append("    <DTS:ConnectionManager")
            out.append("      %s" % attr("DTS:refId", cref))
            out.append('      DTS:CreationName="FLATFILE"')
            out.append('      DTS:DTSID="%s"' % guid("cm:" + name))
            out.append("      %s" % attr("DTS:Description", spec["description"]))
            out.append("      %s>" % attr("DTS:ObjectName", name))
            out.append('      <DTS:PropertyExpression DTS:Name="ConnectionString">%s</DTS:PropertyExpression>'
                       % escape(spec["expression"]))
            out.append("      <DTS:ObjectData>")
            out.append("        <DTS:ConnectionManager")
            out.append('          DTS:Format="Delimited"')
            out.append('          DTS:LocaleID="%d"' % LOCALE_ID)
            out.append('          DTS:HeaderRowDelimiter="_x000D__x000A_"')
            out.append('          DTS:ColumnNamesInFirstDataRow="%s"'
                       % ("True" if spec["header_in_first_row"] else "False"))
            out.append('          DTS:RowDelimiter=""')
            out.append('          DTS:TextQualifier="&quot;"')
            out.append('          DTS:CodePage="%d"' % spec["code_page"])
            out.append("          %s>" % attr("DTS:ConnectionString", spec["design_time_path"]))
            out.append("          <DTS:FlatFileColumns>")
            last = len(spec["columns"]) - 1
            for index, col in enumerate(spec["columns"]):
                out.append("            <DTS:FlatFileColumn")
                out.append('              DTS:ColumnType="Delimited"')
                out.append("              %s" % attr(
                    "DTS:ColumnDelimiter",
                    "_x000D__x000A_" if index == last else spec["delimiter"]))
                out.append('              DTS:DataType="%d"' % FLAT_FILE_TYPES.get(col.dtype, 130))
                out.append('              DTS:MaximumWidth="%d"' % (col.length or 255))
                out.append('              DTS:DTSID="%s"' % guid("ffcol:%s:%s" % (name, col.name)))
                out.append('              DTS:TextQualified="True"')
                out.append("              %s />" % attr("DTS:ObjectName", col.name))
            out.append("          </DTS:FlatFileColumns>")
            out.append("        </DTS:ConnectionManager>")
            out.append("      </DTS:ObjectData>")
            out.append("    </DTS:ConnectionManager>")
        out.append("  </DTS:ConnectionManagers>")
        return out

    def add_sql_log_provider(self, connection="WWI_Staging_DB"):
        self.log_providers.append(connection)
        return self

    # -- emission -----------------------------------------------------------

    def to_xml(self):
        self.bind()
        out = ['<?xml version="1.0"?>']
        out.append('<DTS:Executable xmlns:DTS="%s"' % DTS_NS)
        out.append('  DTS:refId="Package"')
        out.append('  DTS:CreationDate="%s"' % self.CREATION_DATE)
        out.append('  DTS:CreationName="Microsoft.Package"')
        out.append('  DTS:CreatorComputerName="%s"' % self.COMPUTER)
        out.append("  %s" % attr("DTS:CreatorName", self.CREATOR))
        out.append('  DTS:DTSID="%s"' % guid("pkg:" + self.name))
        out.append("  %s" % attr("DTS:Description", self.description))
        out.append('  DTS:ExecutableType="Microsoft.Package"')
        out.append('  DTS:LastModifiedProductVersion="13.0.4001.0"')
        out.append('  DTS:LocaleID="%d"' % LOCALE_ID)
        out.append("  %s" % attr("DTS:ObjectName", self.name))
        out.append('  DTS:PackageType="%s"' % self.package_type)
        out.append('  DTS:ProtectionLevel="%s"' % self.protection_level)
        out.append('  DTS:VersionBuild="1"')
        out.append('  DTS:VersionGUID="%s">' % guid("ver:" + self.name))
        out.append('  <DTS:Property DTS:Name="PackageFormatVersion">8</DTS:Property>')

        if self.log_providers:
            out.append("  <DTS:LogProviders>")
            for conn in self.log_providers:
                lref = "Package.LogProviders[SSIS log provider for SQL Server]"
                out.append("    <DTS:LogProvider")
                out.append("      %s" % attr("DTS:refId", lref))
                out.append('      DTS:ConfigString="%s"' % conn)
                out.append('      DTS:CreationName="DTS.LogProviderSQLServer"')
                out.append('      DTS:DTSID="%s"' % guid(lref + self.name))
                out.append('      DTS:ObjectName="SSIS log provider for SQL Server">')
                out.append("      <DTS:ObjectData>")
                out.append("        <InnerObject />")
                out.append("      </DTS:ObjectData>")
                out.append("    </DTS:LogProvider>")
            out.append("  </DTS:LogProviders>")

        if self.parameters:
            out.append("  <DTS:PackageParameters>")
            for name, value, dtype, required, sensitive, description in self.parameters:
                pref = "Package.Variables[$Package::%s]" % name
                out.append("    <DTS:PackageParameter")
                out.append("      %s" % attr("DTS:refId", pref))
                out.append('      DTS:CreationName=""')
                out.append('      DTS:DataType="%s"' % PARAM_TYPES[dtype])
                out.append("      %s" % attr("DTS:Description", description))
                out.append('      DTS:DTSID="%s"' % guid(pref + self.name))
                out.append('      DTS:Namespace="Package"')
                out.append("      %s" % attr("DTS:ObjectName", name))
                out.append('      DTS:Required="%s"' % ("True" if required else "False"))
                out.append('      DTS:Sensitive="%s">' % ("True" if sensitive else "False"))
                out.append('      <DTS:Property DTS:Name="ParameterValue" DTS:DataType="%s" xml:space="preserve">%s</DTS:Property>'
                           % (PARAM_TYPES[dtype], escape(param_value(value, dtype))))
                out.append("    </DTS:PackageParameter>")
            out.append("  </DTS:PackageParameters>")

        out.extend(self._flat_file_connections_xml())

        if self.variables:
            out.append("  <DTS:Variables>")
            for name, value, dtype, namespace, expression in self.variables:
                vref = "Package.Variables[%s::%s]" % (namespace, name)
                out.append("    <DTS:Variable")
                out.append("      %s" % attr("DTS:refId", vref))
                out.append('      DTS:CreationName=""')
                out.append('      DTS:DTSID="%s"' % guid(vref + self.name))
                out.append('      DTS:IncludeInDebugDump="2345"')
                out.append('      DTS:Namespace="%s"' % namespace)
                if expression:
                    out.append('      DTS:EvaluateAsExpression="True"')
                    out.append("      %s" % attr("DTS:Expression", expression))
                out.append("      %s>" % attr("DTS:ObjectName", name))
                out.append(
                    '      <DTS:VariableValue DTS:DataType="%s" xml:space="preserve">%s</DTS:VariableValue>'
                    % (VAR_TYPES[dtype], escape(str(value)))
                )
                out.append("    </DTS:Variable>")
            out.append("  </DTS:Variables>")

        out.append("  <DTS:Executables>")
        for task in self.tasks:
            out.extend(task.to_xml("Package", 4))
        out.append("  </DTS:Executables>")
        out.extend(_constraints_xml(self.constraints, "Package", 2))

        if self.event_handlers:
            out.append("  <DTS:EventHandlers>")
            for event_name, tasks, constraints in self.event_handlers:
                eref = "Package.EventHandlers[%s]" % event_name
                out.append("    <DTS:EventHandler")
                out.append("      %s" % attr("DTS:refId", eref))
                out.append('      DTS:CreationName="%s"' % event_name)
                out.append('      DTS:DTSID="%s"' % guid(eref + self.name))
                out.append('      DTS:EventName="%s"' % event_name)
                out.append('      DTS:LocaleID="-1"')
                out.append('      DTS:ObjectName="%s">' % event_name)
                out.append("      <DTS:Variables />")
                out.append("      <DTS:Executables>")
                for task in tasks:
                    out.extend(task.to_xml(eref, 8))
                out.append("      </DTS:Executables>")
                out.extend(_constraints_xml(constraints, eref, 6))
                out.append("    </DTS:EventHandler>")
            out.append("  </DTS:EventHandlers>")

        out.append("</DTS:Executable>")
        return "\n".join(out) + "\n"

    def write(self, path):
        directory = os.path.dirname(path)
        if directory and not os.path.isdir(directory):
            os.makedirs(directory)
        with open(path, "w") as handle:
            handle.write(self.to_xml())
        return path
