"""Legacy object -> Delta table mapping for the Customer 360 domain.

Naming contract: ``Dimension.X`` -> ``gold.dim_x``, ``Fact.X`` -> ``gold.fact_x``,
``Aggregate.X`` -> ``gold.agg_x``, ``work.X`` -> ``silver.work_x``, ``stg.X`` -> ``silver.stg_x``.
The legacy ``Customer360.*`` mart schema has no entry in the shared contract; it is mapped to
``gold.c360_*`` (a domain mart inside gold) and its published view to ``gold.rpt_*``.
"""
from dbx_etl_common import naming

LEGACY_TO_DELTA = {
    # upstream warehouse inputs
    "Dimension.Customer": ("gold", "dim_customer"),
    "Fact.Sale": ("gold", "fact_sale"),
    "Fact.Payment": ("gold", "fact_payment"),
    "Fact.Return": ("gold", "fact_return"),
    "Aggregate.Customer 360": ("gold", "agg_customer_360"),
    "Aggregate.Customer Rolling 12 Month": ("gold", "agg_customer_rolling_12_month"),
    "stg.FiscalCalendar445Period": ("silver", "stg_fiscal_calendar_445_period"),
    # C360_Build_CustomerProfile
    "work.CustomerSalesSummary": ("silver", "work_customer_sales_summary"),
    "work.CustomerPaymentSummary": ("silver", "work_customer_payment_summary"),
    "work.CustomerAddressStandardised": ("silver", "work_customer_address_standardised"),
    "work.CustomerIdentityGraph": ("silver", "work_customer_identity_graph"),
    "work.CustomerProfile": ("silver", "work_customer_profile"),
    "Customer360.CustomerProfile": ("gold", "c360_customer_profile"),
    # C360_Build_RollingMetrics
    "work.CustomerRollingMetric": ("silver", "work_customer_rolling_metric"),
    "Customer360.CustomerRollingMetric": ("gold", "c360_customer_rolling_metric"),
    # C360_Build_LoyaltyOverlay
    "work.LoyaltyQualifyingSale": ("silver", "work_loyalty_qualifying_sale"),
    "work.LoyaltyPointLedger": ("silver", "work_loyalty_point_ledger"),
    "work.LoyaltyOverlayCurrent": ("silver", "work_loyalty_overlay_current"),
    "work.LoyaltyOverlay": ("silver", "work_loyalty_overlay"),
    "Customer360.LoyaltyOverlay": ("gold", "c360_loyalty_overlay"),
    # C360_Build_ChurnFlags
    "work.ChurnFeatureSet": ("silver", "work_churn_feature_set"),
    "work.CustomerChurnHighRisk": ("silver", "work_customer_churn_high_risk"),
    "work.CustomerOutreachQueue": ("silver", "work_customer_outreach_queue"),
    "Customer360.CustomerChurnFlag": ("gold", "c360_customer_churn_flag"),
    # C360_Publish_Segments
    "work.CustomerSegmentPrevious": ("silver", "work_customer_segment_previous"),
    "work.CustomerSegment": ("silver", "work_customer_segment"),
    "Customer360.CustomerSegment": ("gold", "c360_customer_segment"),
    "Report.vw_CustomerSegment": ("gold", "rpt_customer_segment"),
}


def table(catalog, legacyName):
    """Fully qualified Delta name for a legacy object, e.g. ``table(c, "Fact.Sale")``."""
    schema, name = LEGACY_TO_DELTA[legacyName]
    return naming.table(catalog, schema, name)


def tableExists(spark, fullName):
    return spark.catalog.tableExists(fullName)
