"""Silver: conformed, keyed, priced invoice lines (the #SaleWork of
Integration.usp_LoadFactSale plus the stg.SaleLine conformance rules).

Temporary views hold the intermediate steps so the reject, hold, inferred-
customer and duplicate outputs read the same data as the fact.
"""

from pyspark import pipelines as dp

from sales_facts import transforms as t

DIM_SCHEMA = spark.conf.get("sales_facts.dimension_schema")  # noqa: F821 - `spark` is injected by the pipeline runtime


def _dim(name: str):
    return spark.read.table(f"{DIM_SCHEMA}.{name}")  # noqa: F821


@dp.temporary_view(comment="Typed invoice headers, DRAFT excluded, legacy defaults applied.")
def sale_header_conformed():
    return t.conform_invoice_header(spark.read.table("bronze_sql_invoice"))  # noqa: F821


@dp.temporary_view(comment="Typed invoice lines; malformed numerics become NULL for the reject rules.")
def sale_line_conformed():
    return t.conform_invoice_line(spark.read.table("bronze_sql_invoice_line"))  # noqa: F821


@dp.temporary_view(comment="Header + line per extract batch with the legacy natural key hash.")
def sale_work():
    return t.build_sale_work(spark.read.table("sale_header_conformed"), spark.read.table("sale_line_conformed"))  # noqa: F821


@dp.temporary_view(comment="Accepted lines, one per natural key per batch (highest source row version wins).")
def sale_work_deduplicated():
    survivors, _dropped = t.dedup_within_batch(t.accepted_sale_work(spark.read.table("sale_work")))  # noqa: F821
    return survivors


@dp.temporary_view(comment="Accepted lines with surrogate keys resolved (unknown member 0, n/a -1).")
def sale_work_keyed():
    return t.resolve_dimension_keys(
        spark.read.table("sale_work_deduplicated"),  # noqa: F821
        dim_customer=_dim("dim_customer"),
        dim_stock_item=_dim("dim_stock_item"),
        dim_salesperson=_dim("dim_salesperson"),
        dim_city=_dim("dim_city"),
        dim_sales_channel=_dim("dim_sales_channel"),
        dim_promotion=_dim("dim_promotion"),
    )


@dp.materialized_view(
    comment=(
        "Fact-ready invoice lines: keyed, FX-converted and taxed by region. One row per "
        "natural key per extract batch; rows held for a missing stock item are excluded."
    ),
    cluster_by=["region_code", "invoice_date"],
)
@dp.expect_all(
    {
        "region_mapped": "region_code IN ('NA', 'EU', 'APAC')",
        "fx_rate_positive": "fx_rate > 0",
        "invoice_total_is_net_plus_tax": "total_including_tax = net_amount + tax_amount",
    }
)
def silver_sale_line():
    released = t.release_held(spark.read.table("sale_work_keyed"))  # noqa: F821
    fx = t.conform_fx_rates(spark.read.table("bronze_exchange_rate_daily"))  # noqa: F821
    return t.compute_measures(t.apply_fx(released, fx))
