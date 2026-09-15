"""Silver: Fact.[Fact Load Hold] equivalent. Lines whose stock item cannot be
keyed are parked here instead of being loaded against the unknown member.
They release automatically on the next refresh once dim_stock_item has the key
(the legacy Integration.usp_RekeyLateArrivingDimensions sweep)."""

from pyspark import pipelines as dp

from sales_facts import transforms as t


@dp.materialized_view(
    comment="Sale lines held because their stock item is not yet in dim_stock_item.",
    cluster_by=["region_code", "business_date"],
)
def silver_fact_load_hold():
    return t.fact_load_hold(spark.read.table("sale_work_keyed"))  # noqa: F821 - `spark` is injected by the pipeline runtime
