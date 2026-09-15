"""Silver: early-arriving customers. The legacy proc inserted inferred members
straight into Dimension.Customer; the fact pipeline must not write to another
slice's table, so it publishes the keys and the dimension pipeline creates the
inferred members ('*** INFERRED <key>')."""

from pyspark import pipelines as dp

from sales_facts import transforms as t


@dp.materialized_view(comment="Customer business keys seen on sales before the MDM extract delivered them.")
def silver_inferred_customer():
    return t.inferred_customers(spark.read.table("sale_work_keyed"))  # noqa: F821 - `spark` is injected by the pipeline runtime
