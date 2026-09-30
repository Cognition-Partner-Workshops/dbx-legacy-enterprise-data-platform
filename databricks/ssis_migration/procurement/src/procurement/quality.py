"""DQ_Supplier_Screen: quality gate over silver_supplier. Passing suppliers are recorded in
silver_dq_supplier_result (etl.DataQualityResult) and flagged dq_status_code on silver_supplier;
rejected suppliers go to err_rejected_row with reject_target err.RejectedSupplier."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from procurement import io
from procurement.config import qualified
from procurement.staging import SILVER_SUPPLIER

DQ_RESULT_TABLE = "silver_dq_supplier_result"
EU_COUNTRIES = ["AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV", "LT",
                "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"]


def screenSuppliers(silverSupplier: DataFrame, batchId):
    """Returns (passed, rejected). Rules (in SSIS order):
    1. tax identifier normalised (missing -> NONE);
    2. missing payment terms -> flag (warning, not rejection);
    3. EU suppliers must carry a VAT-shaped tax id (2 letters + 8..12 alphanumerics) -> flag;
    4. duplicate normalised non-NONE tax ids across suppliers -> reject DUPLICATE_TAX_ID (keep earliest);
    5. NONE tax id on a non-pending supplier -> reject MISSING_TAX_ID."""
    s = silverSupplier.where("is_survivor_row")
    dupWindow = Window.partitionBy("tax_identifier").orderBy(F.col("source_modified_date").asc_nulls_last(), F.col("source_supplier_id").asc())
    screened = (
        s.withColumn("dq_missing_payment_terms", F.col("payment_terms_code").isNull())
        .withColumn(
            "dq_invalid_eu_tax_shape",
            F.col("country_code").isin(EU_COUNTRIES) & ~F.coalesce(F.col("tax_identifier"), F.lit("")).rlike("^[A-Z]{2}[A-Z0-9]{8,12}$"),
        )
        .withColumn("_dup_rank", F.when(F.col("tax_identifier") != "NONE", F.row_number().over(dupWindow)).otherwise(F.lit(1)))
        .withColumn(
            "dq_reject_reason_code",
            F.when(F.col("_dup_rank") > 1, "DUPLICATE_TAX_ID")
            .when((F.col("tax_identifier") == "NONE") & (F.col("supplier_status_code") != "PEND"), "MISSING_TAX_ID")
            .otherwise(F.lit(None).cast("string")),
        )
        .drop("_dup_rank")
    )
    passed = screened.where(F.col("dq_reject_reason_code").isNull()).withColumn(
        "dq_status_code", F.when(F.col("dq_missing_payment_terms") | F.col("dq_invalid_eu_tax_shape"), "WARN").otherwise("PASS")
    )
    rejected = screened.where(F.col("dq_reject_reason_code").isNotNull())
    result = passed.select(
        F.lit(int(batchId)).cast("long").alias("batch_id"),
        F.lit("DQ_Supplier_Screen").alias("package_name"),
        F.col("supplier_business_key"),
        F.col("dq_status_code"),
        F.col("dq_missing_payment_terms"),
        F.col("dq_invalid_eu_tax_shape"),
        F.current_timestamp().alias("screened_at"),
    )
    return passed, rejected, result


def runSupplierScreen(spark, batchId):
    packageName = "DQ_Supplier_Screen"
    table = qualified(SILVER_SUPPLIER)
    silver = spark.table(table)
    passed, rejected, result = screenSuppliers(silver, batchId)
    io.writeDelta(result, qualified(DQ_RESULT_TABLE))
    rejectedCount = io.appendRejects(spark, rejected, batchId, packageName, "err.RejectedSupplier", "dq_reject_reason_code", "supplier_business_key")
    # stamp the gate outcome back on silver_supplier (rejected suppliers are kept but blocked)
    result.createOrReplaceTempView("_dq_pass")
    rejected.select("supplier_business_key", "dq_reject_reason_code").createOrReplaceTempView("_dq_fail")
    spark.sql(f"""
        MERGE INTO {table} AS t USING _dq_pass AS s ON t.supplier_business_key = s.supplier_business_key AND t.is_survivor_row
        WHEN MATCHED THEN UPDATE SET t.dq_status_code = s.dq_status_code
    """)
    spark.sql(f"""
        MERGE INTO {table} AS t USING _dq_fail AS s ON t.supplier_business_key = s.supplier_business_key AND t.is_survivor_row
        WHEN MATCHED THEN UPDATE SET t.dq_status_code = 'FAIL'
    """)
    passedCount = passed.count()
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=passedCount + rejectedCount, rowsInserted=passedCount, rowsRejected=rejectedCount)
    return passedCount
