"""Silver: the steward reject queue (err.RejectedInvoiceLine +
etl.usp_LogRejectedRecord). Structural rejects from Integration.usp_LoadFactSale
and the regional tax-variance rejects from stg.usp_AppendIncremental_SaleLine."""

from pyspark import pipelines as dp

from sales_facts import transforms as t


@dp.materialized_view(
    comment="Invoice lines rejected before the fact, with the legacy reject reason text.",
    cluster_by=["region_code", "reject_reason"],
)
def silver_rejected_invoice_line():
    return t.rejected_invoice_lines(spark.read.table("sale_work"))  # noqa: F821 - `spark` is injected by the pipeline runtime
