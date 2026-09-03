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
from xml.sax.saxutils import escape, quoteattr

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

# SSIS parameter data types (as persisted in Project.params / package params)
PARAM_TYPES = {"int": "9", "string": "18", "bool": "3", "datetime": "16", "decimal": "7"}

# Pipeline buffer column data types
DT = {
    "i4": ("i4", None, None, None),
    "i8": ("i8", None, None, None),
    "bool": ("bool", None, None, None),
    "date": ("date", None, None, None),
    "dbTimeStamp": ("dbTimeStamp", None, None, None),
    "guid": ("guid", None, None, None),
}


def guid(seed: str) -> str:
    """Deterministic {GUID} derived from a seed string."""
    h = hashlib.md5(seed.encode("utf-8")).hexdigest().upper()
    return "{%s-%s-%s-%s-%s}" % (h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])


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
        self.name = name
        self.description = description
        self._components = []  # list of dicts describing xml emission
        self._paths = []
        self._last_output = None
        self._columns = []

    # -- sources ------------------------------------------------------------

    def oledb_source(self, name, connection, sql, columns, timeout=0):
        self._columns = list(columns)
        self._components.append(
            dict(kind="oledb_source", name=name, connection=connection, sql=sql, columns=list(columns), timeout=timeout)
        )
        self._last_output = (name, "OLE DB Source Output")
        return self

    def flatfile_source(self, name, connection, columns):
        self._columns = list(columns)
        self._components.append(dict(kind="flatfile_source", name=name, connection=connection, columns=list(columns)))
        self._last_output = (name, "Flat File Source Output")
        return self

    # -- transforms ---------------------------------------------------------

    def derived_column(self, name, derivations):
        """derivations: list of (column_name, expression, Column)."""
        self._components.append(dict(kind="derived", name=name, derivations=list(derivations)))
        for _, _, col in derivations:
            self._columns.append(col)
        self._paths.append((self._last_output, (name, "Derived Column Input")))
        self._last_output = (name, "Derived Column Output")
        return self

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

    def data_conversion(self, name, conversions):
        """conversions: list of (source_column, output_column, Column)."""
        self._components.append(dict(kind="convert", name=name, conversions=list(conversions)))
        for _, _, col in conversions:
            self._columns.append(col)
        self._paths.append((self._last_output, (name, "Data Conversion Input")))
        self._last_output = (name, "Data Conversion Output")
        return self

    # -- destinations -------------------------------------------------------

    def oledb_destination(self, name, connection, table, fast_load=True, keep_identity=False,
                          batch_size=100000, error_disposition="FailComponent"):
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
            )
        )
        self._paths.append((self._last_output, (name, "OLE DB Destination Input")))
        return self

    def reject_destination(self, name, connection, table, from_component, from_output="OLE DB Source Error Output"):
        self._components.append(
            dict(kind="oledb_dest", name=name, connection=connection, table=table, fast_load=False,
                 keep_identity=False, batch_size=0, error_disposition="FailComponent", columns=list(self._columns))
        )
        self._paths.append(((from_component, from_output), (name, "OLE DB Destination Input")))
        return self

    def branch_destination(self, name, connection, table, from_component, from_output):
        """Attach an extra destination to a named upstream output (e.g. a split case)."""
        self._components.append(
            dict(kind="oledb_dest", name=name, connection=connection, table=table, fast_load=True,
                 keep_identity=False, batch_size=10000, error_disposition="FailComponent", columns=list(self._columns))
        )
        self._paths.append(((from_component, from_output), (name, "OLE DB Destination Input")))
        return self

    # -- emission -----------------------------------------------------------

    def _ref(self, base, component=None):
        return base if component is None else "%s\\%s" % (base, component)

    def to_xml(self, base_ref, indent):
        pad = " " * indent
        out = []
        out.append('%s<pipeline version="1">' % pad)
        out.append("%s  <components>" % pad)
        for comp in self._components:
            out.extend(self._component_xml(comp, base_ref, indent + 4))
        out.append("%s  </components>" % pad)
        out.append("%s  <paths>" % pad)
        for idx, (start, end) in enumerate(self._paths):
            start_ref = "%s\\%s.Outputs[%s]" % (base_ref, start[0], start[1])
            end_ref = "%s\\%s.Inputs[%s]" % (base_ref, end[0], end[1])
            out.append(
                '%s    <path refId="%s.Paths[%s]" endId="%s" name="%s" startId="%s" />'
                % (pad, base_ref, escape(start[1] if idx == 0 else "%s %d" % (start[1], idx)), escape(end_ref),
                   escape(start[1] if idx == 0 else "%s %d" % (start[1], idx)), escape(start_ref))
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

    def _component_xml(self, comp, base_ref, indent):
        pad = " " * indent
        ref = "%s\\%s" % (base_ref, comp["name"])
        kind = comp["kind"]
        out = []
        if kind in ("oledb_source", "flatfile_source"):
            is_flat = kind == "flatfile_source"
            class_id = "Microsoft.FlatFileSource" if is_flat else "Microsoft.OLEDBSource"
            out.append(
                '%s<component refId=%s componentClassID="%s" contactInfo="%s" description="%s" name=%s usesDispositions="true" version="%d">'
                % (pad, quoteattr(ref), class_id,
                   "Flat File Source" if is_flat else "OLE DB Source",
                   "Flat File Source" if is_flat else "OLE DB Source",
                   quoteattr(comp["name"]), 1 if is_flat else 7)
            )
            out.append("%s  <properties>" % pad)
            if not is_flat:
                out.append(self._prop(pad + "    ", "System.Int32", "CommandTimeout", comp["timeout"],
                                      "The number of seconds before a command times out."))
                out.append(self._prop(pad + "    ", "System.String", "OpenRowset", "",
                                      "Specifies the name of the database object used to open a rowset."))
                out.append(self._prop(pad + "    ", "System.String", "SqlCommand", comp["sql"],
                                      "The SQL command to be executed."))
                out.append(self._prop(pad + "    ", "System.Int32", "AccessMode", 2,
                                      "Specifies the mode used to access the database."))
                out.append(self._prop(pad + "    ", "System.Int32", "DefaultCodePage", 1252,
                                      "Specifies the column code page to use when code page information is unavailable."))
            else:
                out.append(self._prop(pad + "    ", "System.Boolean", "RetainNulls", "false",
                                      "Specifies whether zero-length strings are converted to nulls."))
                out.append(self._prop(pad + "    ", "System.String", "FileNameColumnName", "SourceFileName",
                                      "Specifies the name of the output column containing the file name."))
            out.append("%s  </properties>" % pad)
            out.append("%s  <connections>" % pad)
            out.append(
                '%s    <connection refId=%s connectionManagerID="%s" connectionManagerRefId=%s description="The connection used to access the source." name="%s" />'
                % (pad, quoteattr(ref + ".Connections[%s]" % ("FlatFileConnection" if is_flat else "OleDbConnection")),
                   guid("cm:" + comp["connection"]) + ":external",
                   quoteattr("Project.ConnectionManagers[%s]" % comp["connection"]),
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
                    '%s        <outputColumn refId=%s %s errorOrTruncationOperation="Conversion" errorRowDisposition="RedirectRow" externalMetadataColumnId=%s lineageId=%s name=%s truncationRowDisposition="RedirectRow" />'
                    % (pad, quoteattr("%s.Outputs[%s].Columns[%s]" % (ref, output_name, col.name)),
                       col.metadata_attrs(),
                       quoteattr("%s.Outputs[%s].ExternalColumns[%s]" % (ref, output_name, col.name)),
                       quoteattr("%s.Outputs[%s].Columns[%s]" % (ref, output_name, col.name)),
                       quoteattr(col.name))
                )
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
            for col in comp["columns"]:
                out.append(
                    '%s        <outputColumn refId=%s %s lineageId=%s name=%s />'
                    % (pad, quoteattr("%s.Outputs[%s].Columns[%s]" % (ref, error_name, col.name)),
                       col.metadata_attrs(),
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
                '%s<component refId=%s componentClassID="Microsoft.DerivedColumn" description="Derived Column" name=%s usesDispositions="true" version="1">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s name="Derived Column Input" />' % (pad, quoteattr("%s.Inputs[Derived Column Input]" % ref)))
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s exclusionGroup="1" name="Derived Column Output" synchronousInputId=%s>'
                       % (pad, quoteattr("%s.Outputs[Derived Column Output]" % ref), quoteattr("%s.Inputs[Derived Column Input]" % ref)))
            out.append("%s      <outputColumns>" % pad)
            for col_name, expr, col in comp["derivations"]:
                col_ref = "%s.Outputs[Derived Column Output].Columns[%s]" % (ref, col_name)
                out.append('%s        <outputColumn refId=%s %s lineageId=%s name=%s>'
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
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "lookup":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.Lookup" description="Lookup" name=%s usesDispositions="true" version="5">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <properties>" % pad)
            out.append(self._prop(pad + "    ", "System.String", "SqlCommand", comp["sql"],
                                  "The SQL command used to populate the lookup cache."))
            out.append(self._prop(pad + "    ", "System.Int32", "NoMatchBehavior",
                                  1 if comp["no_match"] == "IG" else 0,
                                  "Determines the behaviour when a lookup finds no match."))
            out.append(self._prop(pad + "    ", "System.Int32", "CacheType", 0, "Full cache."))
            out.append("%s  </properties>" % pad)
            out.append("%s  <connections>" % pad)
            out.append(
                '%s    <connection refId=%s connectionManagerID="%s" connectionManagerRefId=%s name="OleDbConnection" />'
                % (pad, quoteattr(ref + ".Connections[OleDbConnection]"),
                   guid("cm:" + comp["connection"]) + ":external",
                   quoteattr("Project.ConnectionManagers[%s]" % comp["connection"]))
            )
            out.append("%s  </connections>" % pad)
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s errorRowDisposition="%s" name="Lookup Input">'
                       % (pad, quoteattr("%s.Inputs[Lookup Input]" % ref),
                          "RedirectRow" if comp["no_match"] == "RD" else "FailComponent"))
            out.append("%s      <inputColumns>" % pad)
            for jc in comp["join_columns"]:
                out.append('%s        <inputColumn refId=%s cachedName=%s name=%s />'
                           % (pad, quoteattr("%s.Inputs[Lookup Input].Columns[%s]" % (ref, jc)), quoteattr(jc), quoteattr(jc)))
            out.append("%s      </inputColumns>" % pad)
            out.append("%s    </input>" % pad)
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s name="Lookup Match Output" synchronousInputId=%s>'
                       % (pad, quoteattr("%s.Outputs[Lookup Match Output]" % ref), quoteattr("%s.Inputs[Lookup Input]" % ref)))
            out.append("%s      <outputColumns>" % pad)
            for col in comp["output_columns"]:
                col_ref = "%s.Outputs[Lookup Match Output].Columns[%s]" % (ref, col.name)
                out.append('%s        <outputColumn refId=%s %s lineageId=%s name=%s />'
                           % (pad, quoteattr(col_ref), col.metadata_attrs(), quoteattr(col_ref), quoteattr(col.name)))
            out.append("%s      </outputColumns>" % pad)
            out.append("%s      <externalMetadataColumns />" % pad)
            out.append("%s    </output>" % pad)
            out.append('%s    <output refId=%s name="Lookup No Match Output" synchronousInputId=%s />'
                       % (pad, quoteattr("%s.Outputs[Lookup No Match Output]" % ref), quoteattr("%s.Inputs[Lookup Input]" % ref)))
            out.append('%s    <output refId=%s isErrorOut="true" name="Lookup Error Output" synchronousInputId=%s />'
                       % (pad, quoteattr("%s.Outputs[Lookup Error Output]" % ref), quoteattr("%s.Inputs[Lookup Input]" % ref)))
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "split":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.ConditionalSplit" description="Conditional Split" name=%s version="1">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s name="Conditional Split Input" />' % (pad, quoteattr("%s.Inputs[Conditional Split Input]" % ref)))
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            for order, (out_name, expr) in enumerate(comp["cases"]):
                out.append('%s    <output refId=%s exclusionGroup="1" name=%s synchronousInputId=%s>'
                           % (pad, quoteattr("%s.Outputs[%s]" % (ref, out_name)), quoteattr(out_name),
                              quoteattr("%s.Inputs[Conditional Split Input]" % ref)))
                out.append("%s      <properties>" % pad)
                out.append(self._prop(pad + "        ", "System.String", "Expression", expr, "The condition for this output."))
                out.append(self._prop(pad + "        ", "System.String", "FriendlyExpression", expr, "The condition as displayed."))
                out.append(self._prop(pad + "        ", "System.Int32", "EvaluationOrder", order, "Evaluation order."))
                out.append("%s      </properties>" % pad)
                out.append("%s    </output>" % pad)
            out.append('%s    <output refId=%s exclusionGroup="1" isDefaultOut="true" name=%s synchronousInputId=%s />'
                       % (pad, quoteattr("%s.Outputs[%s]" % (ref, comp["default"])), quoteattr(comp["default"]),
                          quoteattr("%s.Inputs[Conditional Split Input]" % ref)))
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "rowcount":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.RowCount" description="Row Count" name=%s version="1">'
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

        elif kind == "aggregate":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.Aggregate" description="Aggregate" name=%s version="1">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s name="Aggregate Input 1">' % (pad, quoteattr("%s.Inputs[Aggregate Input 1]" % ref)))
            out.append("%s      <inputColumns>" % pad)
            for gb in comp["group_by"]:
                out.append('%s        <inputColumn refId=%s cachedName=%s name=%s>'
                           % (pad, quoteattr("%s.Inputs[Aggregate Input 1].Columns[%s]" % (ref, gb)), quoteattr(gb), quoteattr(gb)))
                out.append("%s          <properties>" % pad)
                out.append(self._prop(pad + "            ", "System.Int32", "AggregationType", 0, "Group by."))
                out.append("%s          </properties>" % pad)
                out.append("%s        </inputColumn>" % pad)
            for src, dest, op in comp["aggregations"]:
                out.append('%s        <inputColumn refId=%s cachedName=%s name=%s>'
                           % (pad, quoteattr("%s.Inputs[Aggregate Input 1].Columns[%s]" % (ref, src)), quoteattr(src), quoteattr(src)))
                out.append("%s          <properties>" % pad)
                out.append(self._prop(pad + "            ", "System.Int32", "AggregationType",
                                      {"sum": 1, "avg": 2, "count": 3, "min": 4, "max": 5}.get(op, 1), op))
                out.append(self._prop(pad + "            ", "System.String", "AggregationColumnName", dest, "Output column."))
                out.append("%s          </properties>" % pad)
                out.append("%s        </inputColumn>" % pad)
            out.append("%s      </inputColumns>" % pad)
            out.append("%s    </input>" % pad)
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s name="Aggregate Output 1" />' % (pad, quoteattr("%s.Outputs[Aggregate Output 1]" % ref)))
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "sort":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.Sort" description="Sort" name=%s version="1">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <properties>" % pad)
            out.append(self._prop(pad + "    ", "System.Boolean", "EliminateDuplicates",
                                  "true" if comp["dedupe"] else "false", "Remove duplicate rows."))
            out.append("%s  </properties>" % pad)
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s name="Sort Input">' % (pad, quoteattr("%s.Inputs[Sort Input]" % ref)))
            out.append("%s      <inputColumns>" % pad)
            for order, sc in enumerate(comp["sort_columns"], start=1):
                out.append('%s        <inputColumn refId=%s cachedName=%s name=%s>'
                           % (pad, quoteattr("%s.Inputs[Sort Input].Columns[%s]" % (ref, sc)), quoteattr(sc), quoteattr(sc)))
                out.append("%s          <properties>" % pad)
                out.append(self._prop(pad + "            ", "System.Int32", "NewSortKeyPosition", order, "Sort key position."))
                out.append("%s          </properties>" % pad)
                out.append("%s        </inputColumn>" % pad)
            out.append("%s      </inputColumns>" % pad)
            out.append("%s    </input>" % pad)
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s name="Sort Output" />' % (pad, quoteattr("%s.Outputs[Sort Output]" % ref)))
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "union":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.UnionAll" description="Union All" name=%s version="1">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s hasSideEffects="true" name="Union All Input 1" />' % (pad, quoteattr("%s.Inputs[Union All Input 1]" % ref)))
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s name="Union All Output 1" />' % (pad, quoteattr("%s.Outputs[Union All Output 1]" % ref)))
            out.append("%s  </outputs>" % pad)
            out.append("%s</component>" % pad)

        elif kind == "convert":
            out.append(
                '%s<component refId=%s componentClassID="Microsoft.DataConvert" description="Data Conversion" name=%s version="1">'
                % (pad, quoteattr(ref), quoteattr(comp["name"]))
            )
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s name="Data Conversion Input">' % (pad, quoteattr("%s.Inputs[Data Conversion Input]" % ref)))
            out.append("%s      <inputColumns>" % pad)
            for src, dest, col in comp["conversions"]:
                out.append('%s        <inputColumn refId=%s cachedName=%s name=%s />'
                           % (pad, quoteattr("%s.Inputs[Data Conversion Input].Columns[%s]" % (ref, src)), quoteattr(src), quoteattr(src)))
            out.append("%s      </inputColumns>" % pad)
            out.append("%s    </input>" % pad)
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s name="Data Conversion Output" synchronousInputId=%s>'
                       % (pad, quoteattr("%s.Outputs[Data Conversion Output]" % ref), quoteattr("%s.Inputs[Data Conversion Input]" % ref)))
            out.append("%s      <outputColumns>" % pad)
            for src, dest, col in comp["conversions"]:
                col_ref = "%s.Outputs[Data Conversion Output].Columns[%s]" % (ref, dest)
                out.append('%s        <outputColumn refId=%s %s lineageId=%s name=%s sourceColumn=%s />'
                           % (pad, quoteattr(col_ref), col.metadata_attrs(), quoteattr(col_ref), quoteattr(dest),
                              quoteattr("%s.Inputs[Data Conversion Input].Columns[%s]" % (ref, src))))
            out.append("%s      </outputColumns>" % pad)
            out.append("%s      <externalMetadataColumns />" % pad)
            out.append("%s    </output>" % pad)
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
                '%s    <connection refId=%s connectionManagerID="%s" connectionManagerRefId=%s description="The OLE DB runtime connection used to access the database." name="OleDbConnection" />'
                % (pad, quoteattr(ref + ".Connections[OleDbConnection]"),
                   guid("cm:" + comp["connection"]) + ":external",
                   quoteattr("Project.ConnectionManagers[%s]" % comp["connection"]))
            )
            out.append("%s  </connections>" % pad)
            out.append("%s  <inputs>" % pad)
            out.append('%s    <input refId=%s errorOrTruncationOperation="Insert" errorRowDisposition="%s" hasSideEffects="true" name="OLE DB Destination Input">'
                       % (pad, quoteattr("%s.Inputs[OLE DB Destination Input]" % ref), comp["error_disposition"]))
            out.append("%s      <inputColumns>" % pad)
            for col in comp["columns"]:
                out.append('%s        <inputColumn refId=%s %s externalMetadataColumnId=%s name=%s />'
                           % (pad, quoteattr("%s.Inputs[OLE DB Destination Input].Columns[%s]" % (ref, col.name)),
                              col.cached_attrs(),
                              quoteattr("%s.Inputs[OLE DB Destination Input].ExternalColumns[%s]" % (ref, col.name)),
                              quoteattr(col.name)))
            out.append("%s      </inputColumns>" % pad)
            out.append("%s      <externalMetadataColumns isUsed=\"True\">" % pad)
            for col in comp["columns"]:
                out.append('%s        <externalMetadataColumn refId=%s %s name=%s />'
                           % (pad, quoteattr("%s.Inputs[OLE DB Destination Input].ExternalColumns[%s]" % (ref, col.name)),
                              col.metadata_attrs(), quoteattr(col.name)))
            out.append("%s      </externalMetadataColumns>" % pad)
            out.append("%s    </input>" % pad)
            out.append("%s  </inputs>" % pad)
            out.append("%s  <outputs>" % pad)
            out.append('%s    <output refId=%s isErrorOut="true" name="OLE DB Destination Error Output" />'
                       % (pad, quoteattr("%s.Outputs[OLE DB Destination Error Output]" % ref)))
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

    def __init__(self, name):
        self.name = name

    def object_data(self, ref, indent):  # pragma: no cover - overridden
        return []

    def to_xml(self, parent_ref, indent):
        ref = "%s\\%s" % (parent_ref, self.name)
        pad = " " * indent
        out = [
            "%s<DTS:Executable" % pad,
            "%s  %s" % (pad, attr("DTS:refId", ref)),
            '%s  DTS:CreationName="%s"' % (pad, self.creation_name),
            "%s  %s" % (pad, attr("DTS:Description", self.description)),
            '%s  DTS:DTSID="%s"' % (pad, guid(ref)),
            '%s  DTS:ExecutableType="%s"' % (pad, self.executable_type),
            '%s  DTS:LocaleID="-1"' % pad,
            "%s  %s" % (pad, attr("DTS:ObjectName", self.name)),
            '%s  DTS:ThreadHint="0">' % pad,
            "%s  <DTS:Variables />" % pad,
        ]
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
        self.parameter_bindings = parameter_bindings or []  # (variable, index, dtype)
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
        for var, index, dtype in self.parameter_bindings:
            out.append(
                '%s  <SQLTask:ParameterBinding SQLTask:ParameterName="%s" SQLTask:DtsVariableName="%s" '
                'SQLTask:ParameterDirection="Input" SQLTask:DataType="%s" SQLTask:ParameterSize="-1" />'
                % (pad, index, var, dtype)
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
        self.expression = expression

    def object_data(self, ref, indent):
        pad = " " * indent
        return ["%s<ExpressionTask %s />" % (pad, attr("Expression", self.expression))]


class ExecutePackage(Task):
    creation_name = "Microsoft.ExecutePackageTask"
    executable_type = "Microsoft.ExecutePackageTask"
    description = "Execute Package Task"

    def __init__(self, name, package_name, parameter_assignments=None):
        Task.__init__(self, name)
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
            out.append(
                '%s  <ParameterAssignment ParameterName="%s" BindedVariableOrParameterName="%s" />'
                % (pad, escape(child_param), escape(parent_var))
            )
        out.append("%s</ExecutePackageTask>" % pad)
        return out


class FileSystemTask(Task):
    creation_name = "Microsoft.FileSystemTask"
    executable_type = "Microsoft.FileSystemTask"
    description = "File System Task"

    def __init__(self, name, operation, source_variable, destination_variable):
        Task.__init__(self, name)
        self.operation = operation
        self.source_variable = source_variable
        self.destination_variable = destination_variable

    def object_data(self, ref, indent):
        pad = " " * indent
        return [
            '%s<FileSystemData taskOperationType="%s" taskSourceVariable="%s" taskDestinationVariable="%s" '
            'taskOperationIsSourceVariable="True" taskOperationIsDestinationVariable="True" '
            'xmlns="www.microsoft.com/sqlserver/dts/tasks/filesystemtask" />'
            % (pad, self.operation, self.source_variable, self.destination_variable)
        ]


class DataFlowTask(Task):
    creation_name = "Microsoft.Pipeline"
    executable_type = "Microsoft.Pipeline"
    description = "Data Flow Task"

    def __init__(self, data_flow):
        Task.__init__(self, data_flow.name)
        self.data_flow = data_flow

    def object_data(self, ref, indent):
        return self.data_flow.to_xml(ref, indent)


class Container:
    """Sequence container or Foreach loop holding child executables."""

    def __init__(self, name, kind="sequence", enumerator=None, variable_mappings=None, description=None):
        self.name = name
        self.kind = kind
        self.enumerator = enumerator or {}
        self.variable_mappings = variable_mappings or []
        self.description = description or ("Sequence Container" if kind == "sequence" else "Foreach Loop Container")
        self.tasks = []
        self.constraints = []

    def add(self, task):
        self.tasks.append(task)
        return task

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
            '%s  DTS:ExecutableType="%s"' % (pad, creation),
            '%s  DTS:LocaleID="-1"' % pad,
            "%s  %s>" % (pad, attr("DTS:ObjectName", self.name)),
            "%s  <DTS:Variables />" % pad,
        ]
        if self.kind == "foreach":
            out.append("%s  <DTS:ForEachEnumerator" % pad)
            out.append('%s    DTS:ObjectName="ForeachFileEnumerator"' % pad)
            out.append('%s    DTS:CreationName="Microsoft.ForEachFileEnumerator">' % pad)
            out.append("%s    <DTS:ObjectData>" % pad)
            out.append(
                '%s      <FEFE Folder="%s" FileSpec="%s" FileNameRetrieval="1" Recurse="0" />'
                % (pad, escape(self.enumerator.get("folder", "")), escape(self.enumerator.get("file_spec", "*.csv")))
            )
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

    def add_sql_log_provider(self, connection="WWI_Staging_DB"):
        self.log_providers.append(connection)
        return self

    # -- emission -----------------------------------------------------------

    def to_xml(self):
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
        out.append('  DTS:LocaleID="1033"')
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
                out.append('      DTS:CreationName="DTS.LogProviderSQLServer.3"')
                out.append('      DTS:DTSID="%s"' % guid(lref + self.name))
                out.append('      DTS:ObjectName="SSIS log provider for SQL Server" />')
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
                out.append('      <DTS:Property DTS:Name="ParameterValue" xml:space="preserve">%s</DTS:Property>' % escape(str(value)))
                out.append("    </DTS:PackageParameter>")
            out.append("  </DTS:PackageParameters>")

        if self.connection_managers:
            out.append("  <DTS:ConnectionManagers>")
            for name in self.connection_managers:
                cref = "Package.ConnectionManagers[%s]" % name
                out.append("    <DTS:ConnectionManager")
                out.append("      %s" % attr("DTS:refId", cref))
                out.append('      DTS:ConnectionString=""')
                out.append('      DTS:CreationName="OLEDB"')
                out.append('      DTS:DTSID="%s"' % guid("cm:" + name))
                out.append("      %s" % attr("DTS:ObjectName", name))
                out.append('      DTS:ProjectReference="True" />')
            out.append("  </DTS:ConnectionManagers>")

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
