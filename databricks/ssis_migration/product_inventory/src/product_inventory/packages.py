"""Package registry and stage runner used by the thin notebooks."""
from __future__ import annotations

from typing import Callable, Dict, List

from pyspark.sql import SparkSession

from product_inventory import bronze, gold_aggregates, gold_dimensions, gold_facts, gold_inventory, silver
from product_inventory.config import PipelineConfig

PackageRunner = Callable[[SparkSession, PipelineConfig], Dict[str, int]]

STAGES: Dict[str, List[str]] = {
    "bronze": ["EXT_ORA_ProductMaster", "EXT_ORA_ProductHierarchy", "EXT_SQL_StockItems", "EXT_SQL_StockMovements", "EXT_SQL_StockTransfers"],
    "silver": ["STG_Load_Product", "STG_Load_StockItem", "STG_Load_StockMovement", "STG_Work_ProductCrosswalk", "STG_Work_InventoryPosition"],
    "dimensions": ["DIM_Load_ProductCategory", "DIM_Load_StockItem"],
    "facts": ["FACT_Load_Movement", "FACT_Load_StockHolding", "FACT_Load_DailyInventorySnapshot"],
    "inventory": ["INV_Load_CycleCountVariance", "INV_Load_DailySnapshot", "INV_Load_Replenishment", "INV_Load_StockTransfer", "INV_Reconcile_OnHand"],
    "aggregates": ["AGG_Refresh_DailyInventoryHealth"],
}

PACKAGES: Dict[str, PackageRunner] = {
    "EXT_ORA_ProductMaster": bronze.runExtOraProductMaster,
    "EXT_ORA_ProductHierarchy": bronze.runExtOraProductHierarchy,
    "EXT_SQL_StockItems": bronze.runExtSqlStockItems,
    "EXT_SQL_StockMovements": bronze.runExtSqlStockMovements,
    "EXT_SQL_StockTransfers": bronze.runExtSqlStockTransfers,
    "STG_Load_Product": silver.runStgLoadProduct,
    "STG_Load_StockItem": silver.runStgLoadStockItem,
    "STG_Load_StockMovement": silver.runStgLoadStockMovement,
    "STG_Work_ProductCrosswalk": silver.runStgWorkProductCrosswalk,
    "STG_Work_InventoryPosition": silver.runStgWorkInventoryPosition,
    "DIM_Load_ProductCategory": gold_dimensions.runDimLoadProductCategory,
    "DIM_Load_StockItem": gold_dimensions.runDimLoadStockItem,
    "FACT_Load_Movement": gold_facts.runFactLoadMovement,
    "FACT_Load_StockHolding": gold_facts.runFactLoadStockHolding,
    "FACT_Load_DailyInventorySnapshot": gold_facts.runFactLoadDailyInventorySnapshot,
    "INV_Load_CycleCountVariance": gold_inventory.runInvLoadCycleCountVariance,
    "INV_Load_DailySnapshot": gold_inventory.runInvLoadDailySnapshot,
    "INV_Load_Replenishment": gold_inventory.runInvLoadReplenishment,
    "INV_Load_StockTransfer": gold_inventory.runInvLoadStockTransfer,
    "INV_Reconcile_OnHand": gold_inventory.runInvReconcileOnHand,
    "AGG_Refresh_DailyInventoryHealth": gold_aggregates.runAggRefreshDailyInventoryHealth,
}


def runStage(spark: SparkSession, cfg: PipelineConfig, stage: str) -> Dict[str, Dict[str, int]]:
    if stage not in STAGES:
        raise ValueError(f"unknown stage '{stage}'; expected one of {sorted(STAGES)}")
    results: Dict[str, Dict[str, int]] = {}
    for package in STAGES[stage]:
        print(f"[{stage}] running {package}")
        results[package] = PACKAGES[package](spark, cfg)
        print(f"[{stage}] {package}: {results[package]}")
    return results
