"""Regional customer dimension load shared by DIM_NA/EU/APAC_Load_Customer.

Each SSIS package read stg.Customer for its RegionCode, applied the regional derived-column / conditional-split
rules (regional.py), looked up Customer Category, rejected rows to err.CustomerReject and called
Integration.usp_MigrateStagedCustomerData (hybrid SCD). The three notebooks differ only by RegionCode.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wwi_dimensions import regional, runtime, scd, specs
from wwi_dimensions.loaders import CREDIT_LIMIT_BANDS
from wwi_dimensions.runtime import band

REJECT_OBJECT_NAME = "err.CustomerReject"


@dataclass
class CustomerLoadResult:
    regionCode: str
    scdResult: Optional[scd.ScdResult] = None
    rowsRead: int = 0
    rowsRejected: int = 0
    rowsQueuedLateArriving: int = 0
    rejected: Optional[DataFrame] = field(default=None, repr=False)


def _withPrimaryAddress(customers: DataFrame, addresses: Optional[DataFrame]) -> DataFrame:
    """Attach the primary (or first) staged address so postal / jurisdiction rules can run."""
    if addresses is None:
        return customers
    a = addresses
    if "IsPrimaryAddress" in a.columns:
        a = a.withColumn("_pri", F.when(F.col("IsPrimaryAddress") == True, 0).otherwise(1))  # noqa: E712
    else:
        a = a.withColumn("_pri", F.lit(0))
    from pyspark.sql.window import Window
    a = a.withColumn("_rn", F.row_number().over(Window.partitionBy("CustomerBusinessKey").orderBy("_pri", *[c for c in ("AddressTypeCode", "PostalCodeRaw") if c in a.columns]))).where("_rn = 1")
    keep = [c for c in ("CityName", "StateProvinceCode", "CountyName", "PostalCodeRaw", "CountryCode") if c in a.columns and c not in customers.columns]
    a = a.select(F.col("CustomerBusinessKey").alias("_abk"), *keep)
    return customers.join(a, customers["CustomerBusinessKey"] == a["_abk"], "left").drop("_abk")


def prepareCustomerSource(customers: DataFrame, regionCode: str, addresses: Optional[DataFrame] = None) -> DataFrame:
    """Region filter + survivor filter + address join + column padding, before the regional conditioning."""
    df = customers.where(F.upper(F.col("RegionCode")) == regionCode.upper())
    if "IsSurvivorRow" in df.columns:
        df = df.where(F.coalesce(F.col("IsSurvivorRow"), F.lit(True)) == True)  # noqa: E712
    df = _withPrimaryAddress(df, addresses)
    if "PrimaryCountryCode" not in df.columns and "CountryCode" in df.columns:
        df = df.withColumnRenamed("CountryCode", "PrimaryCountryCode")
    return regional.withMissingSourceColumns(df)


def conformCustomer(conditioned: DataFrame) -> DataFrame:
    """Regional output -> Dimension.Customer attribute names (Category / keys are filled by the notebook lookups)."""
    df = conditioned
    df = df.withColumn("Category", F.col("CustomerCategoryCode")) if "Category" not in df.columns else df
    df = df.withColumn("CreditLimitBand", F.when(F.col("CreditLimitAmountUsd").isNull(), F.lit("UNRATED")).otherwise(band("CreditLimitAmountUsd", CREDIT_LIMIT_BANDS, "PLATINUM"))) if "CreditLimitBand" not in df.columns else df
    df = df.withColumn("AccountStatusCode", F.coalesce(F.col("CustomerStatusCode"), F.lit("ACTIVE"))) if "AccountStatusCode" not in df.columns else df
    df = df.withColumn("BuyingGroup", F.col("BuyingGroupName")) if "BuyingGroup" not in df.columns else df
    df = df.withColumn("BillToCustomer", F.coalesce(F.col("BillToCustomerName"), F.col("Customer"))) if "BillToCustomer" not in df.columns else df
    df = df.withColumn("PrimaryContact", F.col("PrimaryContactName")) if "PrimaryContact" not in df.columns else df
    df = df.withColumn("WebsiteURL", F.col("WebsiteUrl")) if "WebsiteURL" not in df.columns else df
    df = df.withColumn("CountryCode", F.upper(F.col("PrimaryCountryCode"))) if "CountryCode" not in df.columns else df
    df = df.withColumn("PostalCode", F.col("PostalCodeRaw")) if "PostalCode" not in df.columns else df
    retentionExpiry = F.add_months(F.coalesce(F.col("LastActivityDate"), F.col("AccountOpenedDate")).cast("date"), F.col("RetentionYears").cast("int") * 12)
    df = df.withColumn("RetentionExpiryDate", F.coalesce(F.col("RetentionExpiryDate").cast("date"), retentionExpiry) if "RetentionExpiryDate" in df.columns else retentionExpiry)
    df = df.withColumn("CustomerSegmentKey", F.lit(specs.NOT_APPLICABLE_KEY))
    return df


def loadRegionalCustomers(
    spark: SparkSession,
    catalog: str,
    regionCode: str,
    batchId: int,
    packageExecutionId: int,
    businessDate,
    loadTimestamp: datetime,
    reloadFullHistory: bool = False,
) -> CustomerLoadResult:
    result = CustomerLoadResult(regionCode=regionCode)
    spec = specs.CUSTOMER
    runtime.prepareDimension(spark, catalog, spec)

    staged = runtime.readStaging(spark, catalog, "stg_customer", batchId, reloadFullHistory)
    addresses = runtime.readStagingIfExists(spark, catalog, "stg_customer_address", batchId, reloadFullHistory)
    source = prepareCustomerSource(staged, regionCode, addresses)
    valid, dqRejects = runtime.splitRejects(source)
    conditioned, ruleRejects = regional.conditionCustomers(valid, regionCode)

    rejectCols = ["CustomerBusinessKey", "SourceSystemCode", "RejectReasonCode"]
    rejected = dqRejects.select(*rejectCols).unionByName(ruleRejects.select(*rejectCols))
    conformed = conformCustomer(conditioned)
    conformed = runtime.lookupSurrogateKey(spark, catalog, conformed, specs.CUSTOMER_CATEGORY, "CustomerCategoryCode", "CustomerCategoryKey")

    result.rowsRead = source.count()
    result.rowsRejected = rejected.count()
    result.rejected = rejected
    result.rowsQueuedLateArriving = runtime.queueLateArrivingMembers(
        spark, catalog, conformed, specs.CUSTOMER_CATEGORY.name, "CustomerCategoryCode", "CustomerCategoryKey",
        batchId, packageExecutionId, f"Dimension.Customer[{regionCode}]", loadTimestamp,
    )
    result.scdResult = scd.applyScd(spark, catalog, spec, conformed, batchId, packageExecutionId, loadTimestamp=loadTimestamp)
    return result
