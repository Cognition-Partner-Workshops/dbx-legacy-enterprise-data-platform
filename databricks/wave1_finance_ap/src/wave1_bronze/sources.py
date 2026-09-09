"""Swappable source read path for the wave-1 bronze load.

Bronze only ever calls ``SourceReader.read(table) -> SourceBatch``. Which
system sits behind that call is chosen by the ``source_kind`` bundle variable,
so replacing the synthetic volume drop with the real Oracle ERP is a config
change plus one reader class, not a rewrite of the load.

Contracts
---------
Every table has a column contract at ``<landing>/oracle/_contracts/<SCHEMA>.<TABLE>.json``
mirroring the extract's column order and physical types (Oracle NUMBER(p,s) ->
decimal(p,s), NUMBER(n) -> bigint, DATE -> date, VARCHAR2 -> string, Y/N flags ->
string). The delimited reader needs it because the .dat files carry no header;
the JDBC/federation readers would use it to assert the live schema matches.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

CONTRACT_DIR = "oracle/_contracts"
DATA_DIR = "oracle"

_TYPE_MAP = {
    "integer": lambda c: T.LongType(),
    "decimal": lambda c: T.DecimalType(int(c["precision"] or 19), int(c["scale"] or 2)),
    "date": lambda c: T.DateType(),
    "timestamp": lambda c: T.TimestampType(),
    "string": lambda c: T.StringType(),
    "flag": lambda c: T.StringType(),
}


@dataclass(frozen=True)
class TableRef:
    schema: str
    name: str

    @classmethod
    def parse(cls, qualified: str) -> "TableRef":
        schema, _, name = qualified.strip().upper().partition(".")
        if not schema or not name:
            raise ValueError("expected SCHEMA.TABLE, got %r" % qualified)
        return cls(schema, name)

    @property
    def qualified(self) -> str:
        return "%s.%s" % (self.schema, self.name)

    def bronze_table(self, catalog: str, bronze_prefix: str) -> str:
        return "%s.%s_%s.%s" % (catalog, bronze_prefix, self.schema.lower(), self.name.lower())


@dataclass
class Contract:
    table: TableRef
    columns: list
    delimiter: str = "|"
    header: bool = False
    encoding: str = "utf-8"
    generator_scale: str = ""
    expected_rows: int = 0

    @classmethod
    def load(cls, spark: SparkSession, landing_path: str, table: TableRef) -> "Contract":
        path = os.path.join(landing_path, CONTRACT_DIR, table.qualified + ".json")
        raw = json.loads("\n".join(r.value for r in spark.read.text(path).collect()))
        return cls(
            table=table,
            columns=raw["columns"],
            delimiter=raw.get("delimiter", "|"),
            header=bool(raw.get("header", False)),
            encoding=raw.get("encoding", "utf-8"),
            generator_scale=raw.get("generator_scale", ""),
            expected_rows=int(raw.get("rows", 0)),
        )

    def struct(self) -> T.StructType:
        return T.StructType(
            [T.StructField(c["name"], _TYPE_MAP[c["type"]](c), bool(c.get("nullable", True)))
             for c in self.columns]
        )


@dataclass
class SourceBatch:
    """One read of one table: the typed frame plus where it came from."""

    df: DataFrame
    source_kind: str
    source_ref: str            # file glob, JDBC query, or federated table
    contract: Contract
    files: list = field(default_factory=list)


def _strip_scheme(col):
    return F.regexp_replace(col, "^dbfs:", "")


class SourceReader:
    kind = "abstract"

    def __init__(self, spark: SparkSession, landing_path: str):
        self.spark = spark
        self.landing_path = landing_path

    def read(self, table: TableRef) -> SourceBatch:
        raise NotImplementedError


class VolumeDelimitedReader(SourceReader):
    """Pipe-delimited, header-less Oracle-shaped extracts in a UC volume.

    This is also the shape a real ``sqlplus``/Data Pump-to-CSV drop takes, so a
    live source can land files here and bronze does not change.
    """

    kind = "volume_delimited"

    def read(self, table: TableRef) -> SourceBatch:
        contract = Contract.load(self.spark, self.landing_path, table)
        folder = os.path.join(self.landing_path, DATA_DIR, table.schema, table.name)
        df = (
            self.spark.read.format("csv")
            .schema(contract.struct())
            .option("sep", contract.delimiter)
            .option("header", str(contract.header).lower())
            .option("encoding", contract.encoding)
            .option("nullValue", "")
            .option("emptyValue", "")
            .option("dateFormat", "yyyy-MM-dd")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .option("mode", "PERMISSIVE")
            .load(folder + "/*.dat")
            .withColumn("_source_file", _strip_scheme(F.col("_metadata.file_path")))
        )
        files = [
            r.path
            for r in self.spark.read.format("binaryFile").load(folder + "/*.dat")
            .select(_strip_scheme(F.col("path")).alias("path")).collect()
        ]
        return SourceBatch(df=df, source_kind=self.kind, source_ref=folder, contract=contract,
                           files=files)


class OracleJdbcReader(SourceReader):
    """Drop-in real source: read the extract view straight from the ERP over JDBC.

    Connection details come from a UC connection / secret scope named in
    ``ORACLE_JDBC_CONNECTION``; nothing credential-shaped is passed as a job
    parameter. Not wired in this demo - there is no Oracle behind it.
    """

    kind = "oracle_jdbc"

    def read(self, table: TableRef) -> SourceBatch:
        raise NotImplementedError(
            "oracle_jdbc reader is the real-source seam; configure a UC connection "
            "and implement read() with spark.read.format('jdbc') against %s" % table.qualified
        )


class LakehouseFederationReader(SourceReader):
    """Drop-in real source: Lakehouse Federation foreign catalog over the ERP.

    ``read`` becomes ``spark.table(f"{foreign_catalog}.{schema}.{table}")`` once the
    foreign catalog exists; the contract is then used to assert schema parity.
    """

    kind = "lakehouse_federation"

    def read(self, table: TableRef) -> SourceBatch:
        raise NotImplementedError(
            "lakehouse_federation reader needs a foreign catalog over the Oracle ERP; "
            "none exists in the demo workspace"
        )


READERS = {r.kind: r for r in (VolumeDelimitedReader, OracleJdbcReader, LakehouseFederationReader)}


def make_reader(kind: str, spark: SparkSession, landing_path: str) -> SourceReader:
    try:
        return READERS[kind](spark, landing_path)
    except KeyError:
        raise SystemExit("unknown source_kind %r; expected one of %s" % (kind, sorted(READERS)))
