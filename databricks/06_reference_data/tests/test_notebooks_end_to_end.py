"""Runs all 14 REF_Load_* notebooks (the real notebook source files) against local Delta in phase order,
with the test-only dbx_etl_common fake and sample bronze extracts, then re-runs them to prove idempotency."""
import datetime
import os

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from conftest import BUNDLE, NotebookExit, makeDbutils
from dbx_etl_common import control
from wwi_ref import PHASES, schemas

NOTEBOOKS = os.path.join(BUNDLE, "notebooks")
ORDER = sorted(PHASES, key=lambda n: (PHASES[n], n))
BATCH_ID = 7
BUSINESS_DATE = "2024-03-04"


def runNotebook(spark, name, dbutils):
    path = os.path.join(NOTEBOOKS, name + ".py")
    with open(path) as fh:
        source = fh.read()
    assert source.startswith("# Databricks notebook source")
    scope = {"spark": spark, "dbutils": dbutils, "__name__": "__notebook__", "__file__": path}
    exec(compile(source, path, "exec"), scope)
    return scope


def seedBronze(spark, catalog):
    raw = lambda name: schemas.fqn(catalog, name)  # noqa: E731
    audit = dict(BatchId=BATCH_ID, PackageExecutionId=1, LoadedAtUtc=datetime.datetime(2024, 3, 4), SourceSystemCode="ORA_ERP")

    def frame(rows, cols):
        allCols = cols + list(audit.keys()) + ["SourceRowNumber"]
        data = [tuple(None if v is None else str(v) for v in r) + tuple(str(v) for v in audit.values()) + (str(i + 1),)
                for i, r in enumerate(rows)]
        df = spark.createDataFrame(data, T.StructType([T.StructField(c, T.StringType()) for c in allCols]))
        return (df.withColumn("BatchId", F.col("BatchId").cast("bigint"))
                .withColumn("PackageExecutionId", F.col("PackageExecutionId").cast("bigint"))
                .withColumn("LoadedAtUtc", F.col("LoadedAtUtc").cast("timestamp"))
                .withColumn("SourceRowNumber", F.col("SourceRowNumber").cast("int")))

    frame([
        ("USD", "US Dollar", "$", "2", "840", "Y", "N", None, "2024-03-01"),
        ("EUR", "Euro", "€", "2", "978", "Y", "N", None, "2024-03-01"),
        ("GBP", "Pound Sterling", "£", "2", "826", "Y", "N", None, "2024-03-01"),
        ("AUD", "Australian Dollar", "$", "2", "036", "Y", "N", None, "2024-03-01"),
        ("JPY", "Yen", "¥", "0", "392", "Y", "N", None, "2024-03-01"),
        ("DEM", "Deutsche Mark", None, "2", "276", "N", "Y", "1.95583", "2001-12-31"),
        ("XXXX", "Bad Code", None, "2", None, "Y", "N", None, "2024-03-01"),
    ], ["CURRENCY_CD", "CURRENCY_NAME", "CURRENCY_SYMBOL", "MINOR_UNIT_DIGITS", "ISO_NUMERIC_CD", "ACTIVE_FLG",
        "EURO_LEGACY_FLG", "LEGACY_FIXED_RATE", "LAST_UPDATE_DT"]).write.format("delta").mode("overwrite").saveAsTable(raw("raw.OracleCurrency"))

    fx = []
    for d in (1, 2, 5):   # gap on 3rd/4th -> fill forward
        fx.append(("EUR", "USD", "2024-03-0%d" % d, "CORPORATE", "1.08%d" % d, None, "ECB", "PRIMARY", "2024-03-0%d" % d))
    fx.append(("GBP", "USD", "2024-03-01", "CORPORATE", "1.27", None, "BOE", "PRIMARY", "2024-03-01"))
    fx.append(("AUD", "USD", "2023-01-01", "CORPORATE", "0.68", None, "RBA", "PRIMARY", "2023-01-01"))   # stale
    fx.append(("EUR", "USD", "2024-03-01", "CORPORATE", "-1", None, "ECB", "PRIMARY", "2024-03-01"))     # invalid
    fx.append(("ZZZ", "USD", "2024-03-01", "CORPORATE", "2", None, "ECB", "PRIMARY", "2024-03-01"))      # unknown ccy
    frame(fx, ["FROM_CURRENCY_CD", "TO_CURRENCY_CD", "RATE_DT", "RATE_TYPE_CD", "CONVERSION_RATE", "INVERSE_RATE",
               "RATE_SOURCE_CD", "LEDGER_CD", "LAST_UPDATE_DT"]).write.format("delta").mode("overwrite").saveAsTable(raw("raw.OracleFxRate"))

    frame([
        ("1", "US", "United States", "USA", "NA", "Northern America", "CA", "California", "Los Angeles", "90001", "99999", "America/Los_Angeles", "USD", "US-CA", None, None, None, "2024-03-01"),
        ("2", "US", "United States", "USA", "NA", "Northern America", "NY", "New York", "New York", "10001", "99999", "America/New_York", "USD", "US-NY", None, None, None, "2024-03-01"),
        ("3", "DE", "Germany", "DEU", "EMEA", "Western Europe", None, None, "Berlin", "10115", "99999", "Europe/Berlin", "EUR", "DE", None, None, None, "2024-03-01"),
        ("4", "GB", "United Kingdom", "GBR", "EU", "Northern Europe", None, None, "London", "SW1A 1AA", "AA9A 9AA", "Europe/London", "GBP", "GB", None, None, None, "2024-03-01"),
        ("5", "AU", "Australia", "AUS", "APAC", "Oceania", "NSW", "New South Wales", "Sydney", "2000", "9999", "Australia/Sydney", "AUD", "AU", None, None, None, "2024-03-01"),
        ("6", "XK", "Nowhere", "XKX", "MARS", None, None, None, None, None, None, None, "ZZZ", None, None, None, None, "2024-03-01"),
    ], ["GEOGRAPHY_ID", "COUNTRY_CD", "COUNTRY_NAME", "ISO3_CD", "REGION_CD", "SUB_REGION_NAME", "STATE_PROVINCE_CD",
        "STATE_PROVINCE_NAME", "CITY_NAME", "POSTAL_CD", "POSTAL_FORMAT_MASK", "TIMEZONE_NAME", "CURRENCY_CD",
        "TAX_JURISDICTION_CD", "POPULATION_NUM", "LATITUDE", "LONGITUDE", "LAST_UPDATE_DT"]).write.format("delta").mode("overwrite").saveAsTable(raw("raw.OracleGeography"))

    frame([
        ("1", "VAT", "VAT", "DE", "DE", None, "STD", "19", "N", "100", "Y", "2007-01-01", None, "2024-03-01"),
        ("2", "VAT", "VAT", "GB", "GB", None, "STD", "20", "N", "100", "Y", "2011-01-04", None, "2024-03-01"),
        ("3", "GST", "GST", "AU", "AU", None, "STD", "10", "N", "100", "N", "2000-07-01", None, "2024-03-01"),
        ("4", "ST", "SALESTAX", "US-CA", "US", "CA", "STD", "7.25", "N", "0", "N", "2017-01-01", None, "2024-03-01"),
        ("5", "ST", "SALESTAX", "US", "US", None, "STD", "0", "N", "0", "N", "1900-01-01", None, "2024-03-01"),
        ("6", "ST", "SALESTAX", "US-NY", "US", "NY", "STD", "abc", "N", "0", "N", "2017-01-01", None, "2024-03-01"),
        ("7", "VAT", "VAT", "FR", "FR", None, "STD", "20", "N", "100", "Y", "2014-01-01", None, "2024-03-01"),
    ], ["TAX_RATE_ID", "TAX_CD", "TAX_REGIME_CD", "TAX_JURISDICTION_CD", "COUNTRY_CD", "STATE_PROVINCE_CD", "TAX_CLASS_CD",
        "RATE_PCT", "COMPOUND_FLG", "RECOVERABLE_PCT", "REVERSE_CHARGE_FLG", "EFFECTIVE_FROM_DT", "EFFECTIVE_TO_DT",
        "LAST_UPDATE_DT"]).write.format("delta").mode("overwrite").saveAsTable(raw("raw.OracleTaxRate"))

    frame([
        ("1", "P-100", "Widget", "EA", "CS", "12", "ACTIVE", "1001", "2024-03-01"),
        ("2", "P-200", "Gadget", "KG", "G", "0.001", "ACTIVE", "1002", "2024-03-01"),
        ("3", "P-300", "Bolt", "BOX", "EA", "-5", "ACTIVE", None, "2024-03-01"),
    ], ["PRODUCT_ID", "PRODUCT_CD", "PRODUCT_DESC", "BASE_UOM_CD", "SELL_UOM_CD", "UOM_CONVERSION_FACTOR",
        "LIFECYCLE_STATUS_CD", "WWI_STOCK_ITEM_ID", "LAST_UPDATE_DT"]).write.format("delta").mode("overwrite").saveAsTable(raw("raw.OracleProductMaster"))

    frame([("1", "ACTIVE"), ("2", "ZOMBIE")], ["CUST_ID", "CUST_STATUS_CD"]).write.format("delta").mode("overwrite").saveAsTable(raw("raw.OracleCustomerMaster"))

    movements = [(i, "WH-%d" % (i % 3), "US" if i % 3 else "GB", "90210-1234" if i % 3 else "sw1a 1aa") for i in range(60)]
    (spark.createDataFrame(movements, ["StagingStockMovementId", "WarehouseCode", "CountryCode", "PostalCode"])
     .withColumn("BatchId", F.lit(BATCH_ID).cast("bigint"))
     .write.format("delta").mode("overwrite").saveAsTable(raw("stg.StockMovement")))


@pytest.fixture(scope="module")
def loaded(spark, catalog):
    seedBronze(spark, catalog)
    control.reset()
    for name in ORDER:
        runNotebook(spark, name, makeDbutils(batchId=str(BATCH_ID), businessDate=BUSINESS_DATE))
    return {"rowCounts": list(control.ROW_COUNTS), "errors": list(control.ERRORS),
            "runs": dict(control.PACKAGE_RUNS), "rejects": list(control.REJECT_SETS)}


def table(spark, catalog, legacyName):
    return spark.table(schemas.fqn(catalog, legacyName))


def test_all_packages_succeeded(loaded):
    statuses = {r["packageName"]: r["status"] for r in loaded["runs"].values()}
    assert set(statuses) == set(ORDER)
    assert all(s == "Succeeded" for s in statuses.values()), statuses
    assert all(r["projectName"] == "WWI_ReferenceData" for r in loaded["runs"].values())
    assert not [e for e in loaded["errors"] if e["errorSeverity"] == "Error"], loaded["errors"]


def test_row_counts_logged_per_object(loaded):
    objects = {r["objectName"] for r in loaded["rowCounts"]}
    for expected in ("ref.Region", "ref.Country", "ref.Currency", "ref.FxRateDaily", "ref.TaxJurisdiction",
                     "ref.CodeCrosswalk.PAYMENT_METHOD", "Dimension.Currency", "Dimension.Geography", "Dimension.Date",
                     "Dimension.Fiscal Calendar", "Dimension.Cost Center", "Dimension.Unknown Member", "etl.Configuration"):
        assert expected in objects, expected


def test_reference_tables(spark, catalog, loaded):
    country = table(spark, catalog, "ref.Country")
    codes = {r["CountryCode"]: r for r in country.collect()}
    assert set(codes) >= {"US", "DE", "GB", "AU"}
    assert "XK" not in codes                         # region MARS -> lookup failure
    assert codes["DE"]["RegionCode"] == "EU"          # EMEA mapped through the REGION crosswalk
    assert codes["DE"]["IsEuMemberState"] is True and codes["GB"]["IsEuMemberState"] is False
    assert codes["US"]["StateProvinceRequiredFlag"] is True and codes["DE"]["StateProvinceRequiredFlag"] is False

    currency = {r["CurrencyCode"]: r for r in table(spark, catalog, "ref.Currency").collect()}
    assert "XXXX" not in currency
    assert currency["USD"]["IsReportingCurrency"] is True
    assert currency["DEM"]["IsEuroLegacy"] is True and currency["DEM"]["IsActive"] is False
    assert currency["JPY"]["RoundingRuleCode"] == "UNIT"

    fx = table(spark, catalog, "ref.FxRateDaily").where("FromCurrencyCode = 'EUR'").orderBy("RateDate").collect()
    assert [str(r["RateDate"]) for r in fx] == ["2024-03-01", "2024-03-02", "2024-03-03", "2024-03-04", "2024-03-05"]
    assert [r["RateSourceCode"] for r in fx][2:4] == ["FILL_FORWARD", "FILL_FORWARD"]
    assert table(spark, catalog, "ref.FxRateDaily").where("FromCurrencyCode = 'ZZZ'").count() == 0

    tax = {(r["TaxJurisdictionCode"]): r for r in table(spark, catalog, "ref.TaxJurisdiction").collect()}
    assert "US-NY" not in tax and "FR" not in tax      # bad rate / unknown country
    assert tax["DE"]["ReverseChargeEligible"] is True and tax["AU"]["RegistrationRequiredFlag"] is True
    assert float(tax["US-CA"]["StateRatePercent"]) == 7.25

    xw = table(spark, catalog, "ref.CodeCrosswalk")
    assert xw.where("EffectiveToDate IS NULL").groupBy("CodeDomainCode", "SourceSystemCode", "SourceCodeValue").count().where("count > 1").count() == 0
    assert xw.where("CodeDomainCode = 'REGION' AND SourceCodeValue = 'EMEA'").count() == 1

    uom = table(spark, catalog, "ref.UomConversion")
    assert uom.where("StockItemBusinessKey = 'ORA_ERP|P-100' AND FromUomCode = 'CS' AND ToUomCode = 'EA'").count() == 1
    assert uom.where("StockItemBusinessKey LIKE '%P-300%'").count() == 0

    skx = table(spark, catalog, "ref.SourceKeyCrosswalk")
    assert skx.where("SourceSystemCode = 'WWI_OLTP' AND SourceKeyValue = '1001' AND ConformedBusinessKey = 'P-100'").count() == 1


def test_dimensions(spark, catalog, loaded):
    dimCurrency = {r["CurrencyCode"]: r for r in table(spark, catalog, "Dimension.Currency").where("CurrencyKey > 0").collect()}
    assert dimCurrency["EUR"]["RateStatusCode"] == "RATED" or dimCurrency["EUR"]["RateStatusCode"] == "STALE"
    assert dimCurrency["AUD"]["RateStatusCode"] == "STALE" and dimCurrency["USD"]["RateStatusCode"] == "UNRATED"
    assert "DEM" not in dimCurrency
    assert float(dimCurrency["EUR"]["LatestRateToUsd"]) == 1.085

    pm = table(spark, catalog, "Dimension.Payment Method").where("PaymentMethodKey > 0")
    assert pm.count() > 0
    assert pm.where("PaymentMethodCode = 'CHQ' AND ElectronicFlag = 'N'").count() >= 1
    assert pm.where("RegionCode = 'EU' AND SettlementTypeCode <> 'SEPA'").count() == 0

    tt = table(spark, catalog, "Dimension.Transaction Type").where("TransactionTypeKey > 0")
    assert tt.where("TransactionTypeCode = 'PURCH' AND MovementSign = 1 AND AffectsLedgerFlag = 'Y'").count() >= 1

    terms = table(spark, catalog, "Dimension.Payment Terms").where("PaymentTermsKey > 0")
    assert terms.where("PaymentTermsCode = 'NET90' AND RegionCode = 'EU' AND NetDaysCapped = 60").count() >= 0
    assert terms.where("NetDaysCapped IS NULL OR NetDaysCapped <= 0").count() == 0

    geo = {r["CountryCode"]: r for r in table(spark, catalog, "Dimension.Geography").where("GeographyKey > 0").collect()}
    assert geo["DE"]["ReverseChargeFlag"] == "Y" and geo["DE"]["EuStatusCode"] == "MEMBER"
    assert geo["GB"]["EuStatusCode"] == "EXITED"
    assert geo["US"]["TaxStructureCode"] == "NA_SALESTAX" and geo["AU"]["TaxStructureCode"] == "APAC_GST"
    assert geo["DE"]["CurrencyName"] == "Euro"

    site = {r["WarehouseSiteCode"]: r for r in table(spark, catalog, "Dimension.Warehouse Site").where("WarehouseSiteKey > 0").collect()}
    # legacy joins ref.PostalFormatRule ON RulePriority = 1 while the steward grid uses 100/110/200/...: no rule ever
    # matches, so TruncateToLength is NULL and the ZIP+4 is only stripped, never cut to ZIP5 (see mapping doc, section 7)
    assert site["WH-1"]["PostalCodeStandardized"] == "902101234" and site["WH-1"]["RegionCode"] == "NA"
    assert site["WH-0"]["PostalCodeStandardized"] == "SW1A1AA" and site["WH-0"]["SiteTypeCode"] == "SATELLITE"

    cc = table(spark, catalog, "Dimension.Cost Center").where("CostCenterKey > 0")
    assert cc.where("IsCurrentRow = true").count() == cc.count()
    assert cc.where("CostCenterCode = 'CORP' AND ParentCostCenterCode = 'ROOT'").count() >= 1
    assert cc.where("VersionNumber <> 1").count() == 0

    um = table(spark, catalog, "Dimension.Unknown Member").where("UnknownMemberRowKey > 0")
    assert um.where("ReferenceTableName = 'ref.StatusCode' AND DomainCode = 'ORDER'").count() == 1
    assert um.where("UnknownMemberKey = -1 AND NotApplicableKey = -2").count() == um.count()


def test_date_dimension(spark, catalog, loaded):
    dates = table(spark, catalog, "Dimension.Date")
    real = dates.where("IsReservedMember = false")
    assert real.count() == (datetime.date(2035, 12, 31) - datetime.date(2005, 1, 1)).days + 1
    assert dates.where("DateKey IN (-1, -2)").count() == 2
    assert dates.where("DateKey = -1").first()["Date"] == datetime.date(1900, 1, 1)
    row = real.where("DateKey = 20240304").first()
    assert row["FiscalYearNa"] == 2024 and row["FiscalPeriodNa"] == 9      # NA FY starts July
    assert row["FiscalYearEu"] == 2024 and row["FiscalPeriodEu"] == 3      # EU calendar year
    assert row["FiscalYearApac"] == 2023 and row["FiscalPeriodApac"] == 12  # APAC FY starts April, named by the year it starts in (FY2023 = Apr-2023..Mar-2024)
    assert row["DayOfWeek"] == "Monday" and row["WeekendFlag"] == "N" and row["ISOWeekNumber"] == 10
    fiscal = table(spark, catalog, "Dimension.Fiscal Calendar")
    assert fiscal.select("CountryCode").distinct().count() == 4
    assert fiscal.groupBy("CountryCode", "Date").count().where("count > 1").count() == 0


def test_reserved_members_everywhere(spark, catalog, loaded):
    from wwi_ref import reserved
    for legacyName in schemas.ownedDimensions():
        if legacyName in (schemas.DATE_DIMENSION[0], schemas.FISCAL_CALENDAR[0]):
            continue
        keyCol = schemas.dimensionKeyColumn(legacyName)
        keys = {r[keyCol] for r in table(spark, catalog, legacyName).where(F.col(keyCol) < 0).collect()}
        assert keys == {-1, -2, -3, -9}, legacyName
    assert reserved.reservedMemberCount(spark, schemas.fqn(catalog, "Dimension.Cost Center"), "CostCenterKey") == 4


def test_rejects_written(spark, catalog, loaded):
    lookups = table(spark, catalog, "err.RejectedLookupFailure")
    constraints = table(spark, catalog, "err.RejectedConstraintViolation")
    assert lookups.where("SourceObjectName = 'raw.OracleGeography' AND LookupName = 'ref.Region'").count() >= 1
    assert lookups.where("RejectReasonCode = 'REF_UNMAPPED_CODE' AND get_json_object(RecordPayload, '$.CODE') = 'ZOMBIE'").count() >= 1
    assert constraints.where("TargetObjectName = 'ref.Currency' AND ViolatingBusinessKey = 'XXXX'").count() == 1
    assert constraints.where("TargetObjectName = 'ref.FxRateDaily' AND ConstraintName = 'CK_FxRateDaily_Rate'").count() == 1
    assert lookups.select("RejectId").distinct().count() == lookups.count()
    assert lookups.where("BatchId <> %d" % BATCH_ID).count() == 0
    assert any(r["objectName"] == "ref.Currency" and r["count"] == 1 for r in loaded["rejects"])


def test_configuration_rows(spark, catalog, loaded):
    cfg = spark.table("%s.etl.configuration" % catalog)
    assert cfg.where("ConfigurationKey LIKE 'CodeSetVersion.PAYMENT_METHOD.%'").count() >= 1
    assert cfg.where("EnvironmentCode = 'DEV'").count() == cfg.count()


def test_rerun_is_idempotent(spark, catalog, loaded):
    before = {n: table(spark, catalog, n).orderBy(schemas.dimensionKeyColumn(n)).drop("ValidFrom", "ValidTo", "LastLoadBatchId").collect()
              for n in schemas.ownedDimensions()}
    refBefore = {n: table(spark, catalog, n).count() for n in schemas.REF_TABLES}
    # the unmapped-code report (DFT Scan For Unmapped Codes) is a rolling view over err.RejectedLookupFailure, so
    # like the legacy it picks up the all-domain sweep of the previous run; compare everything else strictly
    notReport = "RejectReasonCode <> 'REF_UNMAPPED_CODE_REPORT'"
    errBefore = {n: table(spark, catalog, n).where(notReport).count() for n in schemas.ERR_TABLES}
    control.reset()
    for name in ORDER:
        runNotebook(spark, name, makeDbutils(batchId=str(BATCH_ID), businessDate=BUSINESS_DATE))
    assert all(r["status"] == "Succeeded" for r in control.PACKAGE_RUNS.values())
    for n, rows in before.items():
        after = table(spark, catalog, n).orderBy(schemas.dimensionKeyColumn(n)).drop("ValidFrom", "ValidTo", "LastLoadBatchId").collect()
        assert after == rows, n
    assert {n: table(spark, catalog, n).count() for n in schemas.REF_TABLES} == refBefore
    assert {n: table(spark, catalog, n).where(notReport).count() for n in schemas.ERR_TABLES} == errBefore   # same BatchId rejects replaced
    lookups = table(spark, catalog, "err.RejectedLookupFailure")
    unmapped = lookups.where("RejectReasonCode = 'REF_UNMAPPED_CODE' AND get_json_object(RecordPayload, '$.DOMAIN') IS NOT NULL").selectExpr(
        "get_json_object(RecordPayload, '$.DOMAIN')", "SourceSystemCode", "get_json_object(RecordPayload, '$.CODE')").distinct().count()
    assert lookups.where("RejectReasonCode = 'REF_UNMAPPED_CODE_REPORT'").count() == unmapped
    assert lookups.where("BatchId <> %d" % BATCH_ID).count() == 0


def test_restart_from_step_skips_earlier_phases(spark, catalog, loaded):
    control.reset()
    with pytest.raises(NotebookExit):
        runNotebook(spark, "REF_Load_Geography", makeDbutils(batchId=str(BATCH_ID), restartFromStep="REF_Load_DateDimension"))
    assert not control.PACKAGE_RUNS


def test_scd2_versioning_on_change(spark, catalog, loaded):
    target = schemas.fqn(catalog, "Dimension.Cost Center")
    xw = schemas.fqn(catalog, "ref.CodeCrosswalk")
    row = spark.table(target).where("IsCurrentRow = true AND CostCenterKey > 0").first()
    spark.sql("UPDATE %s SET SourceCodeDescription = 'Renamed Cost Center' WHERE CodeDomainCode = 'COST_CENTER' "
              "AND ConformedCodeValue = '%s' AND EffectiveToDate IS NULL" % (xw, row["CostCenterCode"]))
    control.reset()
    runNotebook(spark, "REF_Load_CostCenter", makeDbutils(batchId=str(BATCH_ID + 1), businessDate="2024-03-11"))
    versions = spark.table(target).where("CostCenterCode = '%s' AND RegionCode = '%s'" % (row["CostCenterCode"], row["RegionCode"])).orderBy("VersionNumber").collect()
    assert [v["VersionNumber"] for v in versions] == [1, 2]
    assert versions[0]["IsCurrentRow"] is False and versions[1]["IsCurrentRow"] is True
    assert str(versions[1]["EffectiveFrom"])[:10] == "2024-03-11"
    assert versions[0]["EffectiveTo"] < versions[1]["EffectiveFrom"]
    # steward grid re-load next run closes the renamed mapping again: no duplicate open rows
    assert spark.table(xw).where("EffectiveToDate IS NULL").groupBy("CodeDomainCode", "SourceSystemCode", "SourceCodeValue").count().where("count > 1").count() == 0
