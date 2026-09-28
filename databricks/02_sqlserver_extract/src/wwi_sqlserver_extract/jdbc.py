"""Spark JDBC access to the WideWorldImporters OLTP database (WWI_Source_DB.conmgr).

Connection settings come from job parameters that mirror Project.params
(SqlServerHost / SqlServerPort / SqlServerOltpDb / SqlServerTrustServerCertificate /
SourceQueryTimeoutSeconds / DefaultBatchSize); credentials come from a Databricks
secret scope and are never written to configuration or logs.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession

SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

PARAMETER_DEFAULTS: Dict[str, str] = {
    "SqlServerHost": "sqlserver.internal.example",
    "SqlServerPort": "1433",
    "SqlServerOltpDb": "WideWorldImporters",
    "SqlServerTrustServerCertificate": "false",
    "SourceQueryTimeoutSeconds": "3600",
    "DefaultBatchSize": "100000",
    "SqlServerSecretScope": "wwi",
    "SqlServerUserSecretKey": "sqlserver-oltp-user",
    "SqlServerPasswordSecretKey": "sqlserver-oltp-password",
}


def getWidget(dbutils: Any, name: str, default: str) -> str:
    """Read a job parameter / widget, falling back to the Project.params default."""
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return default
    return value if value is not None and value != "" else default


@dataclass(frozen=True)
class SqlServerConnection:
    host: str
    port: int
    database: str
    user: str
    password: str
    trustServerCertificate: bool = False
    queryTimeoutSeconds: int = 3600
    fetchSize: int = 100000

    @classmethod
    def fromWidgets(cls, dbutils: Any) -> "SqlServerConnection":
        get = lambda name: getWidget(dbutils, name, PARAMETER_DEFAULTS[name])  # noqa: E731
        scope = get("SqlServerSecretScope")
        credentials = {
            name: dbutils.secrets.get(scope=scope, key=get(widget))
            for name, widget in (("user", "SqlServerUserSecretKey"), ("password", "SqlServerPasswordSecretKey"))
        }
        return cls(
            host=get("SqlServerHost"),
            port=int(get("SqlServerPort")),
            database=get("SqlServerOltpDb"),
            **credentials,
            trustServerCertificate=get("SqlServerTrustServerCertificate").strip().lower() == "true",
            queryTimeoutSeconds=int(get("SourceQueryTimeoutSeconds")),
            fetchSize=int(get("DefaultBatchSize")),
        )

    @property
    def url(self) -> str:
        return (
            "jdbc:sqlserver://%s:%d;databaseName=%s;encrypt=true;trustServerCertificate=%s;applicationName=%s"
            % (
                self.host,
                self.port,
                self.database,
                "true" if self.trustServerCertificate else "false",
                "wwi_02_sqlserver_extract",
            )
        )

    def readerOptions(self, sql: str, queryTimeoutSeconds: Optional[int] = None) -> Dict[str, str]:
        timeout = queryTimeoutSeconds if queryTimeoutSeconds else self.queryTimeoutSeconds
        return {
            "url": self.url,
            "driver": SQLSERVER_DRIVER,
            "user": self.user,
            "password": self.password,
            "query": sql,
            "fetchsize": str(self.fetchSize),
            "queryTimeout": str(timeout),
        }

    def readQuery(self, spark: SparkSession, sql: str, queryTimeoutSeconds: Optional[int] = None) -> DataFrame:
        return spark.read.format("jdbc").options(**self.readerOptions(sql, queryTimeoutSeconds)).load()


def sqlLiteral(value: Any) -> str:
    """Render a Python value as a T-SQL literal for the legacy `?` placeholders."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    text = str(value)
    return "N'" + text.replace("'", "''") + "'"


def renderSql(sql: str, paramNames, bindings: Dict[str, Any]) -> str:
    """Replace the positional `?` placeholders of a legacy OLE DB command in order."""
    parts = sql.split("?")
    if len(parts) - 1 != len(paramNames):
        raise ValueError("SQL has %d placeholders but %d parameter names" % (len(parts) - 1, len(paramNames)))
    rendered = parts[0]
    for name, tail in zip(paramNames, parts[1:]):
        if name not in bindings:
            raise KeyError("No binding supplied for SQL parameter %s" % name)
        rendered += sqlLiteral(bindings[name]) + tail
    return rendered
