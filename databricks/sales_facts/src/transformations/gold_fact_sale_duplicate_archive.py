"""Gold: Fact.[Sale Duplicate Archive] equivalent. Replays of an already
loaded line (same natural key, unchanged net amount, later batch) never reach
fact_sale; they are kept here so a bad dedup can always be traced back."""

from pyspark import pipelines as dp

from sales_facts import transforms as t


@dp.materialized_view(comment="Sale-line versions dropped as duplicates of an already loaded version.")
def fact_sale_duplicate_archive():
    accepted = t.accepted_sale_work(spark.read.table("sale_work"))  # noqa: F821 - `spark` is injected by the pipeline runtime
    return t.duplicate_archive(accepted, spark.read.table("silver_sale_line"))  # noqa: F821
