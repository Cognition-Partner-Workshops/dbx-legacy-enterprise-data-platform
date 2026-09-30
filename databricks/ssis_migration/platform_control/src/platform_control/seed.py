"""Seed the Delta control tables from the legacy wwi_legacy_staging.etl.* / ref.* rule tables.

The legacy tables are the read-only baseline: rows are copied (snake_cased) into our schema so the
DQ engine, tolerances and configuration checks run against governed Delta copies that we own.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from platform_control.control import ControlFramework
from platform_control.rules import translateRuleExpression

# target table -> (legacy schema, legacy table, {target column: legacy column expression})
SEED_MAP: dict[str, tuple[str, str, dict[str, str]]] = {
    "etl_data_quality_rule": (
        "etl",
        "DataQualityRule",
        {
            "data_quality_rule_id": "DataQualityRuleId",
            "rule_code": "RuleCode",
            "rule_group_code": "RuleGroupCode",
            "object_name": "ObjectName",
            "rule_name": "RuleName",
            "rule_expression": "RuleExpression",
            "dimension_code": "DimensionCode",
            "severity_code": "SeverityCode",
            "threshold_value": "CAST(ThresholdValue AS DECIMAL(18,4))",
            "region_code": "RegionCode",
            "source_system_code": "SourceSystemCode",
            "is_active": "CAST(IsActive AS BOOLEAN)",
            "owner_name": "OwnerName",
            "notes": "Notes",
            "created_at_utc": "CreatedAtUtc",
            "updated_at_utc": "UpdatedAtUtc",
        },
    ),
    "etl_data_quality_rule_exception": (
        "etl",
        "DataQualityRuleException",
        {
            "rule_exception_id": "RuleExceptionId",
            "rule_code": "RuleCode",
            "object_name": "ObjectName",
            "region_code": "RegionCode",
            "effective_from": "EffectiveFrom",
            "effective_to": "EffectiveTo",
            "reason": "Reason",
            "approved_by": "ApprovedBy",
            "created_at_utc": "CreatedAtUtc",
        },
    ),
    "etl_row_count_tolerance": (
        "etl",
        "RowCountTolerance",
        {
            "row_count_tolerance_id": "RowCountToleranceId",
            "object_name": "ObjectName",
            "tolerance_percent": "CAST(TolerancePercent AS DECIMAL(9,4))",
            "absolute_tolerance": "CAST(AbsoluteTolerance AS BIGINT)",
            "explanation_code": "ExplanationCode",
            "approved_by": "ApprovedBy",
        },
    ),
    "etl_reconciliation_exemption": (
        "etl",
        "ReconciliationExemption",
        {"exemption_id": "ExemptionId", "object_name": "ObjectName", "reason": "Reason"},
    ),
    "etl_configuration": (
        "etl",
        "Configuration",
        {
            "configuration_id": "ConfigurationId",
            "configuration_key": "ConfigurationKey",
            "environment_code": "EnvironmentCode",
            "configuration_value": "CASE WHEN CAST(IsSensitive AS BOOLEAN) THEN '***' ELSE ConfigurationValue END",
            "value_data_type": "ValueDataType",
            "description": "Description",
            "is_sensitive": "CAST(IsSensitive AS BOOLEAN)",
            "modified_at_utc": "ModifiedAtUtc",
        },
    ),
    "etl_required_configuration_key": (
        "etl",
        "RequiredConfigurationKey",
        {
            "required_configuration_key_id": "RequiredConfigurationKeyId",
            "configuration_key": "ConfigurationKey",
            "environment_code": "EnvironmentCode",
            "is_mandatory": "CAST(IsMandatory AS BOOLEAN)",
            "description": "Description",
        },
    ),
    "etl_staging_table_register": (
        "etl",
        "StagingTableRegister",
        {
            "staging_table_id": "StagingTableId",
            "schema_name": "SchemaName",
            "table_name": "TableName",
            "load_date_column": "LoadDateColumn",
            "retention_days": "CAST(RetentionDays AS INT)",
            "is_purge_eligible": "CAST(IsPurgeEligible AS BOOLEAN)",
            "approximate_row_count": "CAST(ApproximateRowCount AS BIGINT)",
        },
    ),
}


def legacyColumns(cf: ControlFramework, schemaName: str, tableName: str) -> set[str]:
    try:
        return set(cf.spark.table(cf.cfg.legacyStaging(schemaName, tableName)).columns)
    except Exception:  # noqa: BLE001 - federation / table absent
        return set()


def readLegacy(cf: ControlFramework, targetTable: str) -> DataFrame | None:
    schemaName, tableName, mapping = SEED_MAP[targetTable]
    available = legacyColumns(cf, schemaName, tableName)
    if not available:
        return None
    selects = []
    for target, expression in mapping.items():
        referenced = [c for c in available if c in expression]
        if referenced or expression.startswith("'"):
            selects.append(f"{expression} AS {target}")
        else:
            selects.append(f"CAST(NULL AS STRING) AS {target}")
    return cf.spark.table(cf.cfg.legacyStaging(schemaName, tableName)).selectExpr(*selects)


def seedTable(cf: ControlFramework, targetTable: str, sourceDf: DataFrame | None = None) -> int:
    df = sourceDf if sourceDf is not None else readLegacy(cf, targetTable)
    if df is None:
        return 0
    if targetTable == "etl_data_quality_rule":
        df = df.withColumn("spark_expression", F.lit(None).cast("string"))
    target = cf.spark.table(cf.t(targetTable))
    aligned = df.select(*[F.col(c).cast(target.schema[c].dataType) if c in df.columns else F.lit(None).cast(target.schema[c].dataType).alias(c) for c in target.columns])
    aligned.write.format("delta").mode("overwrite").saveAsTable(cf.t(targetTable))
    if targetTable == "etl_data_quality_rule":
        precompileRuleExpressions(cf)
    return aligned.count()


def precompileRuleExpressions(cf: ControlFramework) -> None:
    """Store the reviewed Spark translation next to the T-SQL predicate so the engine never
    builds SQL from an untranslated expression at run time."""
    from platform_control.quality import ObjectResolver

    resolver = ObjectResolver(cf)
    rules = cf.spark.table(cf.t("etl_data_quality_rule")).select("rule_code", "object_name", "rule_expression").collect()
    for r in rules:
        try:
            sparkExpr = translateRuleExpression(r["rule_expression"], r["object_name"], resolver.resolve)
        except Exception as exc:  # noqa: BLE001
            sparkExpr = f"/* untranslatable: {exc} */ 1 = 0"
        cf.update("etl_data_quality_rule", {"spark_expression": sparkExpr}, f"rule_code = '{r['rule_code']}'")


def seedAll(cf: ControlFramework) -> dict[str, int]:
    return {name: seedTable(cf, name) for name in SEED_MAP}
