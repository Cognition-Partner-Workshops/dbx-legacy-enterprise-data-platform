"""Post-load maintenance for Dimension.Employee / Dimension.Salesperson (DIM_Load_Employee / DIM_Load_Salesperson).

* manager surrogate-key repair on the current rows (Integration.usp_RepairEmployeeManagerKeys)
* organisation level / leaf-node derivation by walking the manager chain (usp_RefreshEmployeeHierarchy)
* Salesperson -> Sales Territory bridge maintenance (Integration.usp_MaintainSalespersonTerritoryBridge)
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from wwi_dimensions import specs, tables

MAX_HIERARCHY_DEPTH = 12


def _sqlTs(ts: datetime) -> str:
    return f"TIMESTAMP '{ts.strftime('%Y-%m-%d %H:%M:%S')}'"


def repairManagerKeys(spark: SparkSession, catalog: str, batchId: int, packageExecutionId: int, lineageKey: int) -> int:
    """Point ManagerEmployeeKey at the manager's current surrogate key (or -1 when the manager is unknown)."""
    table = specs.EMPLOYEE.fullTableName(catalog)
    fixes = spark.sql(
        f"""
        SELECT e.EmployeeKey,
               CASE WHEN e.ManagerEmployeeNumber IS NULL THEN {specs.NOT_APPLICABLE_KEY}
                    ELSE COALESCE(m.EmployeeKey, {specs.UNKNOWN_KEY}) END AS NewManagerKey
          FROM {table} e
          LEFT JOIN {table} m
            ON m.EmployeeBusinessKey = e.ManagerEmployeeNumber AND m.IsCurrentRow = true AND m.EmployeeKey > 0
         WHERE e.IsCurrentRow = true AND e.EmployeeKey > 0
        """
    )
    fixes = fixes.join(spark.table(table).select("EmployeeKey", F.col("ManagerEmployeeKey").alias("_old")), "EmployeeKey").where(
        F.coalesce(F.col("_old"), F.lit(-999)) != F.col("NewManagerKey")).select("EmployeeKey", "NewManagerKey")
    n = fixes.count()
    if n:
        fixes.createOrReplaceTempView("_employee_manager_fix")
        spark.sql(
            f"""
            MERGE INTO {table} AS t USING _employee_manager_fix AS s ON t.EmployeeKey = s.EmployeeKey
            WHEN MATCHED THEN UPDATE SET t.ManagerEmployeeKey = s.NewManagerKey, t.RekeyedByLineageKey = {int(lineageKey)},
                 t.LastLoadBatchId = {int(batchId)}, t.LastLoadPackageExecutionId = {int(packageExecutionId)}
            """
        )
    return int(n)


def refreshHierarchy(spark: SparkSession, catalog: str) -> int:
    """OrganisationLevel = 1 for roots, manager level + 1 otherwise (capped); IsLeafNode = nobody reports to me."""
    table = specs.EMPLOYEE.fullTableName(catalog)
    cur = spark.table(table).where("IsCurrentRow = true AND EmployeeKey > 0").select("EmployeeKey", "ManagerEmployeeKey")
    levels = cur.where(F.coalesce(F.col("ManagerEmployeeKey"), F.lit(0)) <= 0).select("EmployeeKey", F.lit(1).alias("Lvl"))
    frontier = levels
    for depth in range(2, MAX_HIERARCHY_DEPTH + 1):
        nxt = cur.join(frontier.select(F.col("EmployeeKey").alias("_mgr")), cur["ManagerEmployeeKey"] == F.col("_mgr")).select("EmployeeKey", F.lit(depth).alias("Lvl"))
        nxt = nxt.join(levels.select(F.col("EmployeeKey").alias("_seen")), F.col("EmployeeKey") == F.col("_seen"), "left_anti")
        if nxt.limit(1).count() == 0:
            break
        levels = levels.unionByName(nxt)
        frontier = nxt
    managers = cur.select(F.col("ManagerEmployeeKey").alias("_m")).where("_m > 0").distinct()
    result = cur.join(levels, "EmployeeKey", "left").join(managers, cur["EmployeeKey"] == F.col("_m"), "left").select(
        "EmployeeKey",
        F.coalesce(F.col("Lvl"), F.lit(MAX_HIERARCHY_DEPTH)).cast("smallint").alias("OrganisationLevel"),
        F.col("_m").isNull().alias("IsLeafNode"),
    )
    result.createOrReplaceTempView("_employee_hierarchy")
    spark.sql(
        f"""
        MERGE INTO {table} AS t USING _employee_hierarchy AS s ON t.EmployeeKey = s.EmployeeKey
        WHEN MATCHED AND (t.OrganisationLevel IS DISTINCT FROM s.OrganisationLevel OR t.IsLeafNode IS DISTINCT FROM s.IsLeafNode)
             THEN UPDATE SET t.OrganisationLevel = s.OrganisationLevel, t.IsLeafNode = s.IsLeafNode
        """
    )
    return int(result.count())


def maintainTerritoryBridge(spark: SparkSession, catalog: str, now: datetime, lineageKey: int) -> dict:
    """Close bridge rows whose salesperson now points elsewhere and open rows for the current assignments."""
    bridge = tables.ensureSalespersonTerritoryBridge(spark, catalog)
    dim = specs.SALESPERSON.fullTableName(catalog)
    current = spark.sql(
        f"""SELECT SalespersonKey, SalesTerritoryKey FROM {dim}
             WHERE IsCurrentRow = true AND SalespersonKey > 0 AND SalesTerritoryKey IS NOT NULL"""
    )
    current.createOrReplaceTempView("_bridge_current")
    closed = spark.sql(
        f"""SELECT COUNT(*) AS c FROM {bridge} b WHERE b.IsCurrentAssignment = true
              AND NOT EXISTS (SELECT 1 FROM _bridge_current c WHERE c.SalespersonKey = b.SalespersonKey AND c.SalesTerritoryKey = b.SalesTerritoryKey)"""
    ).collect()[0]["c"]
    spark.sql(
        f"""
        MERGE INTO {bridge} AS b
        USING (SELECT b2.SalespersonKey, b2.SalesTerritoryKey FROM {bridge} b2
                WHERE b2.IsCurrentAssignment = true
                  AND NOT EXISTS (SELECT 1 FROM _bridge_current c WHERE c.SalespersonKey = b2.SalespersonKey AND c.SalesTerritoryKey = b2.SalesTerritoryKey)) AS s
           ON b.SalespersonKey = s.SalespersonKey AND b.SalesTerritoryKey = s.SalesTerritoryKey AND b.IsCurrentAssignment = true
        WHEN MATCHED THEN UPDATE SET b.IsCurrentAssignment = false, b.AssignmentEnded = {_sqlTs(now)}
        """
    )
    opened = spark.sql(
        f"""
        SELECT c.SalespersonKey, c.SalesTerritoryKey FROM _bridge_current c
         WHERE NOT EXISTS (SELECT 1 FROM {bridge} b WHERE b.SalespersonKey = c.SalespersonKey
                             AND b.SalesTerritoryKey = c.SalesTerritoryKey AND b.IsCurrentAssignment = true)
        """
    )
    openedCount = opened.count()
    if openedCount:
        opened.select(
            "SalespersonKey", "SalesTerritoryKey",
            F.lit(100).cast("decimal(9,4)").alias("AllocationPercent"),
            F.lit(True).alias("IsCurrentAssignment"),
            F.lit(now.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp").alias("AssignmentStarted"),
            F.lit(None).cast("timestamp").alias("AssignmentEnded"),
            F.lit(int(lineageKey)).cast("bigint").alias("LineageKey"),
        ).write.format("delta").mode("append").saveAsTable(bridge)
    return {"closed": int(closed), "opened": int(openedCount)}
