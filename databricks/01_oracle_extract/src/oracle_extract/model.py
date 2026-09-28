"""Declarative description of one legacy EXT_ORA_* package.

Every field mirrors something that exists in the generated .dtsx: the OLE DB
source SQL and its '?' bindings, the derived-column expressions (translated to
Spark SQL), the conditional split, the lookup, the destination table and the
Execute SQL tasks that bracket the data flow.
"""
from dataclasses import dataclass, field
from typing import Optional, Tuple


WATERMARK_TIMESTAMP = "Timestamp"
WATERMARK_NUMERIC_KEY = "NumericKey"
WATERMARK_DATE_WINDOW = "DateWindow"

# How the package prepares the bronze table before the append.
LOAD_APPEND = "append"                    # incremental: append rows for this window
LOAD_TRUNCATE = "truncate"                # TRUNCATE TABLE raw.X then load (reference full loads)
LOAD_DELETE_SCOPE = "delete_scope"        # DELETE ... WHERE <scopePredicate> then load (RecordKind slices)
LOAD_DELETE_WINDOW = "delete_window"      # DELETE ... WHERE <col> in [from, to) then load (date windows)
LOAD_DELETE_SNAPSHOT = "delete_snapshot"  # DELETE today's snapshot rows then load (AP aging)


@dataclass(frozen=True)
class SourceColumn:
    name: str
    sparkType: str


@dataclass(frozen=True)
class DerivedColumn:
    name: str
    sparkExpr: str
    sparkType: Optional[str] = None
    legacyExpr: str = ""


@dataclass(frozen=True)
class ConditionalSplit:
    """SSIS conditional split. Rows matching ``matchExpr`` take the named case
    output; everything else (including NULL evaluations, as in SSIS) goes to the
    default output. ``defaultIsReject`` says whether the default output feeds an
    err.* destination (reject) or the same raw table (kept, just routed)."""
    name: str
    caseName: str
    matchExpr: str
    defaultName: str
    defaultIsReject: bool
    rejectReasonCode: Optional[str] = None
    legacyErrTable: Optional[str] = None


@dataclass(frozen=True)
class Lookup:
    """SSIS lookup against a raw.* table already loaded by a sibling package.
    ``joinColumns`` map extract column -> lookup column; unmatched rows are
    redirected to the reject output (no_match='RD')."""
    name: str
    legacyTable: str
    joinColumns: Tuple[Tuple[str, str], ...]
    outputColumns: Tuple[Tuple[str, str, str], ...]   # (lookup column, output name, spark type)
    filterExpr: Optional[str] = None
    rejectReasonCode: str = "LKP_NOMATCH"
    rejectReason: str = ""
    legacyErrTable: Optional[str] = None
    rejectObjectName: Optional[str] = None


@dataclass(frozen=True)
class SourceQuery:
    """One OLE DB source (Oracle) inside a data flow."""
    name: str
    oracleSql: str
    bindOrder: Tuple[str, ...]              # 'from' / 'to' for each '?' in oracleSql
    columns: Tuple[SourceColumn, ...]
    fileName: str                            # relative path (no extension) under the extract Volume
    windowColumn: Optional[str] = None       # column the watermark predicate applies to
    windowLowerInclusive: bool = True        # >= (timestamp/date) vs > (numeric key)
    windowUpperOpen: bool = True             # <  (timestamp/date) vs <= (numeric key)
    upperUnbounded: bool = False             # legacy query binds only the lower bound
    timeoutSeconds: int = 3600
    partitionColumn: Optional[str] = None    # JDBC partitionColumn for big tables
    derived: Tuple[DerivedColumn, ...] = ()
    split: Optional[ConditionalSplit] = None
    lookup: Optional[Lookup] = None
    rowCountVariable: str = "RowsRead"       # legacy User:: variable the row count fed
    legacyErrTable: Optional[str] = None     # destination error output target (RedirectRow)


@dataclass(frozen=True)
class RowOwnership:
    """Identifies the rows a package owns inside a bronze table shared by several packages
    (legacy: raw.OracleCustomerMaster also carries CODEXREF rows, raw.OracleProductMaster the
    HIERARCHY nodes, raw.OracleApInvoiceHdr the AGING snapshot, raw.OracleApPayment the applications).

    ``defaultOwner`` packages own the undiscriminated rows: when ``columns`` are missing from the
    table only their rows exist, so a full-history reload may overwrite the whole table."""
    predicate: str
    columns: Tuple[str, ...]
    defaultOwner: bool = False


@dataclass(frozen=True)
class ExtractSpec:
    packageName: str
    description: str
    sourceSystemCode: str
    legacyTargetTable: str                   # raw.OracleX
    loadMode: str
    sources: Tuple[SourceQuery, ...]
    watermarkObject: Optional[str] = None    # etl.Watermark ObjectName, None => full load
    watermarkType: Optional[str] = None
    numericUpperBoundSql: Optional[str] = None   # 'Read Source Max Key' Execute SQL task
    scopePredicate: Optional[str] = None     # Spark SQL predicate for delete_scope / delete_snapshot
    windowDeleteColumn: Optional[str] = None  # bronze column for delete_window
    constantColumns: Tuple[Tuple[str, str], ...] = ()   # (name, string literal) e.g. RecordKind
    ownership: Optional[RowOwnership] = None  # None => sole writer of legacyTargetTable
    postSteps: Tuple[str, ...] = ()
    dependsOn: Tuple[str, ...] = ()
    legacyTasks: Tuple[str, ...] = field(default_factory=tuple)

    @property
    def isIncremental(self) -> bool:
        return self.watermarkObject is not None
