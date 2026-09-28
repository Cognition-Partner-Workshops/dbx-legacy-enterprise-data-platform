"""Notebook entry point shared by the 22 package notebooks (widgets -> settings -> runner)."""
import json
from dataclasses import asdict
from typing import Optional

from pyspark.sql import SparkSession

from dbx_etl_common import params

from oracle_extract.model import ExtractSpec
from oracle_extract.runner import PackageRunner, RunSettings, RunSummary, ensureBatch
from oracle_extract.source_reader import SOURCE_MODE_FILES, SOURCE_MODE_JDBC, OracleConnection, buildReader

# Notebook-level widgets (task base_parameters, all string). Job-level parameters
# BatchId / BusinessDate / ReloadFullHistory / EnvironmentCode / RestartFromStep /
# catalog are read through dbx_etl_common.params.getJobParams.
WIDGETS = {
    "source_mode": SOURCE_MODE_JDBC,          # 'jdbc' | 'files'
    "oracle_host": "",                        # Project.params OracleHost
    "oracle_port": "1521",                    # Project.params OraclePort
    "oracle_service": "WWIGERP",              # Project.params OracleService
    "oracle_user": "WWI_EXTRACT",             # Project.params OracleUser
    "oracle_secret_scope": "wwi",             # secret scope holding the Oracle password
    "oracle_password_key": "oracle-erp-password",
    "extract_volume_path": "",                # /Volumes/<catalog>/bronze/oracle_extracts (files mode)
    "jdbc_fetch_size": "10000",               # Project.params OracleFetchArraySize equivalent
    "jdbc_num_partitions": "8",
}


def defineWidgets(dbutils) -> None:
    for name, default in WIDGETS.items():
        dbutils.widgets.text(name, default, name)


def widget(dbutils, name: str) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return WIDGETS[name]


def readSettings(dbutils) -> RunSettings:
    p = params.getJobParams(dbutils)
    return RunSettings(
        catalog=p["catalog"],
        batchId=int(p["batchId"]),
        reloadFullHistory=bool(p["reloadFullHistory"]),
        environmentCode=p["environmentCode"],
        businessDate=p.get("businessDate"),
        restartFromStep=p.get("restartFromStep") or "",
    )


def readOracleConnection(dbutils) -> Optional[OracleConnection]:
    host = widget(dbutils, "oracle_host")
    if not host:
        return None
    scope = widget(dbutils, "oracle_secret_scope")
    key = widget(dbutils, "oracle_password_key")
    secretValue = dbutils.secrets.get(scope=scope, key=key)
    return OracleConnection(host, int(widget(dbutils, "oracle_port")), widget(dbutils, "oracle_service"),
                            widget(dbutils, "oracle_user"), secretValue)


def buildRunner(spark: SparkSession, dbutils) -> PackageRunner:
    settings = readSettings(dbutils)
    sourceMode = widget(dbutils, "source_mode").strip().lower() or SOURCE_MODE_JDBC
    connection = readOracleConnection(dbutils) if sourceMode == SOURCE_MODE_JDBC else None
    reader = buildReader(
        sourceMode, connection, widget(dbutils, "extract_volume_path"),
        fetchSize=int(widget(dbutils, "jdbc_fetch_size")), numPartitions=int(widget(dbutils, "jdbc_num_partitions")),
    )
    settings.batchId = ensureBatch(spark, settings)
    return PackageRunner(spark, reader, settings)


def runPackage(spark: SparkSession, dbutils, spec: ExtractSpec) -> RunSummary:
    defineWidgets(dbutils)
    runner = buildRunner(spark, dbutils)
    return runner.run(spec)


def summaryJson(summary: RunSummary) -> str:
    return json.dumps(asdict(summary), default=str)
