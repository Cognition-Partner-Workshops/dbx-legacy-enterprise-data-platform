# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_WebSession
# MAGIC Port of `ssis/08_facts/FACT_Load_WebSession.dtsx` (`build_fact_packages.py::build_fact_load_web_session`).
# MAGIC
# MAGIC * Source `silver.stg_web_session` (`LoadedAtUtc` watermark); natural key `WebSessionBusinessKey`.
# MAGIC * Bot sessions (user agent keywords or > 500 page views) and consent-suppressed sessions are rejected (`WEB_SESSION_FILTERED`), never loaded.
# MAGIC * Lookups: customer (-2 for anonymous sessions, otherwise SCD2 miss -> -1), promotion via `CampaignCode` (-2 when absent).
# MAGIC * `Attribute Sessions` step is a Derived Column: traffic source from campaign / referrer, bounce flag, pages per minute.
# MAGIC * Target `gold.fact_web_session` (liquid-clustered `session_start_date_key, region_code`).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import Window
from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import fact_common as fc
import fact_load
import fact_rules as rules

# COMMAND ----------

PACKAGE_NAME = "FACT_Load_WebSession"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Web Session"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_web_session")))

# COMMAND ----------

SESSION_DATE_COL = "SessionStartDate"


def sessionSource(df):
    start = F.col("SessionStartedAt") if "SessionStartedAt" in df.columns else F.col("LoadedAtUtc")
    ua = F.col("UserAgentString") if "UserAgentString" in df.columns else F.lit(None).cast("string")
    return df.withColumn(SESSION_DATE_COL, start.cast("date")).withColumn("SessionStartedAtTs", start).withColumn("UserAgent", ua)


SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_web_session",
    sourceTable="stg_web_session", sourceDateCol=SESSION_DATE_COL, sourceTimestampCol="LoadedAtUtc",
    businessKeyCol="WebSessionBusinessKey", naturalKeyCols=["WebSessionBusinessKey"],
    surrogateKeyCol="web_session_key", dateKeyCol="session_start_date_key",
    sourceFilter=sessionSource,
    validation=lambda df: rules.isBotSession(F.col("UserAgent"), F.col("PageViewCount")) | (F.col("SuppressedForConsentFlag") == True) | F.col("VisitorKeyHashed").isNull(),
    rejectReasonCode="WEB_SESSION_FILTERED",
    lookups=[
        fact_load.LookupSpec("Customer", "CustomerBusinessKey", "customer_key", SESSION_DATE_COL, notApplicableWhenNull=True),
        fact_load.LookupSpec("Promotion", "CampaignCode", "promotion_key", notApplicableWhenNull=True),
    ],
    extraClusterCols=["region_code"],
)


def trafficSource(campaignCode, referrerDomain):
    return (
        F.when(campaignCode.isNotNull(), F.lit("CAMPAIGN"))
        .when(referrerDomain.isNull() | (F.trim(referrerDomain) == ""), F.lit("DIRECT"))
        .when(F.lower(referrerDomain).rlike("google|bing|yahoo|duckduckgo"), F.lit("SEARCH"))
        .when(F.lower(referrerDomain).rlike("facebook|instagram|twitter|linkedin|tiktok"), F.lit("SOCIAL"))
        .otherwise(F.lit("REFERRAL"))
    )


def transform(spark, catalog, df):
    return df.select(
        F.col(SESSION_DATE_COL).alias("session_start_date_key"),
        F.col("SessionStartedAtTs").alias("session_started_at"),
        "customer_key", "promotion_key",
        F.col("RegionCode").alias("region_code"), F.col("CountryCode").alias("country_code"),
        F.col("WebSessionBusinessKey").alias("session_id"), F.col("VisitorKeyHashed").alias("visitor_id"),
        F.col("SessionDurationSeconds").cast("int").alias("session_duration_seconds"),
        F.col("PageViewCount").cast("int").alias("page_view_count"),
        F.coalesce(F.col("CartCreatedFlag"), F.lit(False)).alias("cart_created_flag"),
        (F.coalesce(F.col("CartCreatedFlag"), F.lit(False)) & ~F.coalesce(F.col("OrderPlacedFlag"), F.lit(False))).alias("cart_abandoned_flag"),
        F.coalesce(F.col("OrderPlacedFlag"), F.lit(False)).alias("order_placed_flag"),
        F.col("OrderBusinessKey").alias("order_number"),
        F.col("DeviceCategoryCode").alias("device_type_code"), F.col("BrowserFamily").alias("browser_family"),
        F.col("UserAgent").alias("user_agent_string"),
        trafficSource(F.col("CampaignCode"), F.col("ReferrerDomain")).alias("traffic_source_code"),
        F.col("CampaignCode").alias("campaign_code"), F.col("ReferrerDomain").alias("referrer_domain"),
        F.col("LandingPagePath").alias("landing_page_path"),
        rules.isBounce(F.col("PageViewCount"), F.col("SessionDurationSeconds")).alias("bounce_flag"),
        rules.pagesPerMinute(F.col("PageViewCount"), F.col("SessionDurationSeconds")).alias("pages_per_minute"),
        F.coalesce(F.col("AnalyticsConsentFlag"), F.lit(False)).alias("cookie_consent_flag"),
        F.col("CustomerBusinessKey").isNull().alias("anonymous_session_flag"),
        F.date_add(F.col(SESSION_DATE_COL), 730).alias("anonymise_after_date"),
        "natural_key_hash",
    )

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform)
    print(result)
