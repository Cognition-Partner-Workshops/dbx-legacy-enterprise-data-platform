"""Source readers behind the single ``source_mode`` parameter.

``jdbc``  - Spark JDBC against Oracle (oracle.jdbc.OracleDriver), the legacy
            source query verbatim with the watermark window bound into the WHERE
            clause so the predicate is evaluated by Oracle; big tables are read
            with ``partitionColumn``/``numPartitions`` bounded by a MIN/MAX probe.
``files`` - generated extract files (``generators/`` output landed in a Unity
            Catalog Volume): ``<volume>/<SCHEMA>/<OBJECT>.dat``, pipe-delimited,
            no header, one column per source-query output column in order,
            dates formatted YYYY-MM-DD[ HH24:MI:SS] as in the SQL*Loader control
            files. The watermark window is applied as a Spark filter on the
            window column, reproducing the legacy predicate.
"""
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from oracle_extract.model import SourceQuery
from oracle_extract.watermark import WatermarkWindow, bindOracleSql, windowPredicate

SOURCE_MODE_JDBC = "jdbc"
SOURCE_MODE_FILES = "files"
ORACLE_DRIVER = "oracle.jdbc.OracleDriver"


@dataclass(frozen=True)
class OracleConnection:
    """Mirror of Project.params OracleHost/OraclePort/OracleService/OracleUser;
    the password comes from the secret scope, never from a parameter."""
    host: str
    port: int
    service: str
    user: str
    password: str

    @property
    def url(self) -> str:
        return f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service}"


def sourceSchema(source: SourceQuery) -> T.StructType:
    return T.StructType([T.StructField(c.name, _parseType(c.sparkType), True) for c in source.columns])


def _parseType(sparkType: str) -> T.DataType:
    return T._parse_datatype_string(sparkType)


def castToSourceSchema(df: DataFrame, source: SourceQuery) -> DataFrame:
    """Project exactly the legacy output columns with the legacy (DT_*) types."""
    return df.select(*[F.col(c.name).cast(c.sparkType).alias(c.name) for c in source.columns])


class OracleJdbcReader:
    def __init__(self, connection: OracleConnection, fetchSize: int = 10000, numPartitions: int = 8,
                 sessionInitStatement: Optional[str] = None):
        self.connection = connection
        self.fetchSize = fetchSize
        self.numPartitions = numPartitions
        self.sessionInitStatement = sessionInitStatement

    def _reader(self, spark: SparkSession, timeoutSeconds: int):
        reader = (
            spark.read.format("jdbc")
            .option("url", self.connection.url)
            .option("driver", ORACLE_DRIVER)
            .option("user", self.connection.user)
            .option("password", self.connection.password)
            .option("fetchsize", str(self.fetchSize))
            .option("queryTimeout", str(timeoutSeconds))
            .option("oracle.jdbc.mapDateToTimestamp", "true")
        )
        if self.sessionInitStatement:
            reader = reader.option("sessionInitStatement", self.sessionInitStatement)
        return reader

    def read(self, spark: SparkSession, source: SourceQuery, window: Optional[WatermarkWindow]) -> DataFrame:
        sql = boundSourceSql(source, window)
        if source.partitionColumn and self.numPartitions > 1:
            bounds = self.scalarRow(
                spark,
                f"SELECT MIN({source.partitionColumn}) AS LO, MAX({source.partitionColumn}) AS HI FROM ({stripOrderBy(sql)}) q",
                source.timeoutSeconds,
            )
            lo, hi = bounds[0], bounds[1]
            if lo is not None and hi is not None and int(hi) > int(lo):
                df = (
                    self._reader(spark, source.timeoutSeconds)
                    .option("dbtable", f"({stripOrderBy(sql)}) q")
                    .option("partitionColumn", source.partitionColumn)
                    .option("lowerBound", str(int(lo)))
                    .option("upperBound", str(int(hi)))
                    .option("numPartitions", str(self.numPartitions))
                    .load()
                )
                return castToSourceSchema(df, source)
        df = self._reader(spark, source.timeoutSeconds).option("query", sql).load()
        return castToSourceSchema(df, source)

    def scalarRow(self, spark: SparkSession, sql: str, timeoutSeconds: int = 600):
        return self._reader(spark, timeoutSeconds).option("query", sql).load().first()

    def scalar(self, spark: SparkSession, sql: str, timeoutSeconds: int = 600):
        row = self.scalarRow(spark, sql, timeoutSeconds)
        return None if row is None else row[0]


class GeneratedFileReader:
    def __init__(self, volumePath: str, delimiter: str = "|", header: bool = False, extension: str = ".dat"):
        self.volumePath = volumePath.rstrip("/")
        self.delimiter = delimiter
        self.header = header
        self.extension = extension

    def path(self, source: SourceQuery) -> str:
        return f"{self.volumePath}/{source.fileName}{self.extension}"

    def read(self, spark: SparkSession, source: SourceQuery, window: Optional[WatermarkWindow]) -> DataFrame:
        stringSchema = T.StructType([T.StructField(c.name, T.StringType(), True) for c in source.columns])
        df = (
            spark.read.format("csv")
            .option("sep", self.delimiter)
            .option("header", str(self.header).lower())
            .option("nullValue", "")
            .option("emptyValue", "")
            .option("timestampFormat", "yyyy-MM-dd[ HH:mm:ss]")
            .option("dateFormat", "yyyy-MM-dd")
            .schema(stringSchema)
            .load(self.path(source))
        )
        df = castToSourceSchema(df, source)
        if window is not None and source.windowColumn:
            df = df.where(
                windowPredicate(window, source.windowColumn, source.windowLowerInclusive,
                                source.windowUpperOpen, source.upperUnbounded, oracle=False)
            )
        return df

    def scalar(self, spark: SparkSession, sql: str, timeoutSeconds: int = 600):
        raise NotImplementedError("scalar probes are only available in jdbc mode")

    def maxColumn(self, spark: SparkSession, source: SourceQuery, column: str):
        return self.read(spark, source, None).agg(F.max(F.col(column))).first()[0]


def boundSourceSql(source: SourceQuery, window: Optional[WatermarkWindow]) -> str:
    if not source.bindOrder:
        return source.oracleSql
    if window is None:
        raise ValueError(f"{source.name}: watermark window required to bind {len(source.bindOrder)} markers")
    return bindOracleSql(source.oracleSql, source.bindOrder, window)


def stripOrderBy(sql: str) -> str:
    """Drop a trailing top-level ORDER BY (meaningless under a partitioned read)."""
    upper = sql.upper()
    idx = upper.rfind("ORDER BY")
    if idx == -1:
        return sql
    tail = sql[idx:]
    if ")" in tail:
        return sql
    return sql[:idx].rstrip()


def buildReader(sourceMode: str, oracleConnection: Optional[OracleConnection], extractVolumePath: Optional[str],
                fetchSize: int, numPartitions: int):
    if sourceMode == SOURCE_MODE_JDBC:
        if oracleConnection is None:
            raise ValueError("source_mode=jdbc requires the Oracle connection parameters")
        return OracleJdbcReader(oracleConnection, fetchSize=fetchSize, numPartitions=numPartitions)
    if sourceMode == SOURCE_MODE_FILES:
        if not extractVolumePath:
            raise ValueError("source_mode=files requires extract_volume_path")
        return GeneratedFileReader(extractVolumePath)
    raise ValueError(f"unknown source_mode {sourceMode!r} (expected 'jdbc' or 'files')")
