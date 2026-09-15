"""Gold: Fact.Sale.

Replaces FACT_NA_Load_Sale, FACT_EU_Load_Sale, FACT_APAC_Load_Sale,
FACT_Dedup_Sale and FACT_Apply_Corrections. Correction rows follow the legacy
REVERSAL pattern (ORIG row untouched, negated REV row, restated RES row) so the
monthly revenue pack reconciles for any as-at date.
"""

from pyspark import pipelines as dp

from sales_facts import transforms as t


@dp.materialized_view(
    comment=(
        "Sales fact with audit-preserving corrections. correction_type_code is ORIG, REV or RES; "
        "summing measures over all rows gives the current position, filtering by batch_id gives any as-at position."
    ),
    cluster_by=["invoice_date", "region_code"],
)
@dp.expect_all_or_fail(
    {
        "sale_key_present": "sale_key IS NOT NULL",
        "correction_type_valid": "correction_type_code IN ('ORIG', 'REV', 'RES')",
        "corrections_reference_a_row": "correction_type_code = 'ORIG' OR corrected_sale_key IS NOT NULL",
    }
)
@dp.expect_all(
    {
        "stock_item_keyed": "stock_item_key <> 0",
        "customer_keyed_or_inferred": "customer_key <> 0 OR inferred_member_flag = 1",
    }
)
def fact_sale():
    return t.build_fact_sale(spark.read.table("silver_sale_line"))  # noqa: F821 - `spark` is injected by the pipeline runtime
