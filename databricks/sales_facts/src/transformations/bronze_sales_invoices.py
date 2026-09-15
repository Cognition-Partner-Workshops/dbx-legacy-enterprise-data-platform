"""Bronze: raw invoice extracts landed from the SQL Server OLTP.

Replaces EXT_SQL_Invoices / EXT_SQL_InvoiceLines and the raw.SqlInvoice /
raw.SqlInvoiceLine landing tables. Every extract batch is appended and kept
(all columns as text, as the legacy raw tables did) so the fact can rebuild its
audit trail from history.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_ROOT = spark.conf.get("sales_facts.landing_root")  # noqa: F821 - `spark` is injected by the pipeline runtime


def _landed_csv(subdir: str):
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .load(f"{LANDING_ROOT}/{subdir}/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(comment="raw.SqlInvoice equivalent: invoice headers as extracted, one row per header per extract batch.")
@dp.expect_all_or_fail(
    {"batch_id_present": "BatchId IS NOT NULL", "source_system_present": "SourceSystemCode IS NOT NULL"}
)
def bronze_sql_invoice():
    return _landed_csv("sqlserver/Sales.Invoices")


@dp.table(comment="raw.SqlInvoiceLine equivalent: invoice lines as extracted, one row per line per extract batch.")
@dp.expect_all_or_fail(
    {"batch_id_present": "BatchId IS NOT NULL", "source_system_present": "SourceSystemCode IS NOT NULL"}
)
def bronze_sql_invoice_line():
    return _landed_csv("sqlserver/Sales.InvoiceLines")


@dp.table(comment="stg.ExchangeRateDaily equivalent: GROUP and APAC_TREASURY daily rates to the reporting currency.")
@dp.expect_all_or_drop(
    {
        "rate_date_present": "RateDate IS NOT NULL",
        "currency_present": "CurrencyCode IS NOT NULL",
        "rate_source_present": "RateSourceCode IN ('GROUP', 'APAC_TREASURY')",
    }
)
def bronze_exchange_rate_daily():
    return _landed_csv("reference/ExchangeRateDaily")
