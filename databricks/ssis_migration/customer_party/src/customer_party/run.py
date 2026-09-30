"""Task dispatcher: maps every SSIS package name to its Databricks-native implementation.

Notebooks are thin wrappers that call `runPackages([...])`; the same entry point
works from a Python wheel task (`python -m customer_party.run PACKAGE ...`).
"""
from __future__ import annotations

import argparse
import os
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from customer_party import dim_customer, dim_supporting, extract, staging
from customer_party.config import PipelineConfig
from customer_party.dedup import runCustomerDedup
from customer_party.quality import runCustomerQualityScreen
from customer_party.recon import writeEvidence
from customer_party.rekey import runLateArrivingRekey
from customer_party.spark_session import getSpark

PackageRunner = Callable[[SparkSession, PipelineConfig], DataFrame]


def _stagingEmployee(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    employees, _ = staging.loadStagedEmployee(spark, cfg)
    return employees


def _stagingPromotionAndTerritory(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    promotions, _ = staging.loadStagedPromotionAndTerritory(spark, cfg)
    return promotions


PACKAGES: dict[str, PackageRunner] = {
    "EXT_ORA_CustomerMaster": extract.extractOracleCustomerMaster,
    "EXT_ORA_CustomerAddress": extract.extractOracleCustomerAddress,
    "EXT_SQL_CustomerSegments": extract.extractSqlCustomerSegments,
    "EXT_SQL_People": extract.extractSqlPeople,
    "EXT_SQL_SalesTerritories": extract.extractSqlSalesTerritories,
    "EXT_SQL_Promotions": extract.extractSqlPromotions,
    "STG_Load_Customer": staging.loadStagedCustomer,
    "STG_Load_CustomerAddress": staging.loadStagedCustomerAddress,
    "STG_Load_Employee": _stagingEmployee,
    "STG_Load_PromotionAndTerritory": _stagingPromotionAndTerritory,
    "STG_Work_CustomerDedup": runCustomerDedup,
    "DQ_Customer_Screen": runCustomerQualityScreen,
    "DIM_NA_Load_Customer": lambda spark, cfg: dim_customer.loadRegionalCustomerDimension(spark, cfg, "NA"),
    "DIM_EU_Load_Customer": lambda spark, cfg: dim_customer.loadRegionalCustomerDimension(spark, cfg, "EU"),
    "DIM_APAC_Load_Customer": lambda spark, cfg: dim_customer.loadRegionalCustomerDimension(spark, cfg, "APAC"),
    "DIM_Load_CustomerCategory": dim_supporting.loadCustomerCategoryDimension,
    "DIM_Load_CustomerSegment": dim_supporting.loadCustomerSegmentDimension,
    "DIM_Load_Employee": dim_supporting.loadEmployeeDimension,
    "DIM_Load_Salesperson": dim_supporting.loadSalespersonDimension,
    "DIM_Load_SalesTerritory": dim_supporting.loadSalesTerritoryDimension,
    "DIM_Load_Promotion": dim_supporting.loadPromotionDimension,
    "DIM_Rekey_LateArriving": runLateArrivingRekey,
    "RECON": writeEvidence,
}


def configFromEnv(overrides: dict[str, str] | None = None) -> PipelineConfig:
    values = {**os.environ, **(overrides or {})}
    return PipelineConfig(
        catalog=values.get("CP_CATALOG", "otterorders_migration"),
        schema=values.get("CP_SCHEMA", "ssis_customer_party"),
        evidenceSchema=values.get("CP_EVIDENCE_SCHEMA", "evidence"),
        batchId=int(values.get("CP_BATCH_ID", "1")),
        reloadFullHistory=values.get("CP_RELOAD_FULL_HISTORY", "false").lower() == "true",
        lookbackMinutes=int(values.get("CP_LOOKBACK_MINUTES", "120")),
        gitSha=values.get("CP_GIT_SHA", "unknown"),
    )


def runPackages(packages: list[str], cfg: PipelineConfig, spark: SparkSession | None = None) -> dict[str, int]:
    spark = spark or getSpark()
    counts: dict[str, int] = {}
    for name in packages:
        if name not in PACKAGES:
            raise KeyError(f"{name} is not a customer_party package")
        result = PACKAGES[name](spark, cfg)
        counts[name] = result.count()
        print(f"[customer_party] {name}: {counts[name]} rows")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Run customer_party packages")
    parser.add_argument("packages", nargs="+", help="package names as in docs/inventories/ssis-packages.csv, or RECON")
    parser.add_argument("--catalog")
    parser.add_argument("--schema")
    parser.add_argument("--batch-id")
    parser.add_argument("--git-sha")
    parser.add_argument("--reload-full-history")
    args = parser.parse_args()
    overrides = {
        k: v
        for k, v in {
            "CP_CATALOG": args.catalog,
            "CP_SCHEMA": args.schema,
            "CP_BATCH_ID": args.batch_id,
            "CP_GIT_SHA": args.git_sha,
            "CP_RELOAD_FULL_HISTORY": args.reload_full_history,
        }.items()
        if v is not None
    }
    runPackages(args.packages, configFromEnv(overrides))


if __name__ == "__main__":
    main()
