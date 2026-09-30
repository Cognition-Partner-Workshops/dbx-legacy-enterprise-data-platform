"""Databricks-native reimplementation of the WWI SSIS ``customer_engagement`` package group."""

PACKAGES = [
    "EXT_SQL_LoyaltyLedger",
    "EXT_SQL_WebSessions",
    "STG_Load_LoyaltyLedger",
    "STG_Load_WebSession",
    "FACT_Load_LoyaltyPoints",
    "FACT_Load_WebSession",
    "C360_Build_CustomerProfile",
    "C360_Build_LoyaltyOverlay",
    "C360_Build_RollingMetrics",
    "C360_Build_ChurnFlags",
    "C360_Publish_Segments",
    "AGG_Refresh_Customer360",
    "AGG_Refresh_CustomerRolling12Month",
]
