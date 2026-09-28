"""Delta table definitions owned by the WWI_ReferenceData bundle.

Legacy -> Delta naming follows the estate contract: ``ref.X`` -> ``silver.ref_x``, ``err.X`` ->
``silver.err_x``, ``Dimension.X`` -> ``gold.dim_x`` (snake_case, spaces -> ``_``). Column names keep
the legacy PascalCase so the validation/runtime baseline queries port with minimal edits. Type mapping:
NVARCHAR/NCHAR -> STRING, BIT -> BOOLEAN, TINYINT/SMALLINT/INT -> INT, DATETIME2 -> TIMESTAMP.

Tables under ``etl`` (etl.configuration, etl.package_execution, ...) belong to session 00 /
dbx_etl_common and are only referenced here, never created.
"""
import re

from dbx_etl_common import naming

HIGH_DATE = "9999-12-31 23:59:59"
SENTINEL_UNKNOWN_DATE = "1900-01-01"
SENTINEL_NOT_APPLICABLE_DATE = "1900-01-02"
SENTINEL_INVALID_DATE = "1900-01-03"


def deltaName(legacyName):
    """Map a legacy two-part object name to (schema, table) in the medallion layout.

    >>> deltaName("ref.CodeCrosswalk")
    ('silver', 'ref_code_crosswalk')
    >>> deltaName("Dimension.Payment Method")
    ('gold', 'dim_payment_method')
    """
    legacySchema, legacyTable = legacyName.replace("[", "").replace("]", "").split(".", 1)
    schemaMap = {
        "raw": ("bronze", "raw"), "stg": ("silver", "stg"), "work": ("silver", "work"),
        "err": ("silver", "err"), "ref": ("silver", "ref"), "Integration": ("silver", "int"),
        "Dimension": ("gold", "dim"), "Fact": ("gold", "fact"), "Aggregate": ("gold", "agg"),
        "Report": ("gold", "rpt"), "etl": ("etl", None),
    }
    schema, prefix = schemaMap[legacySchema]
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", legacyTable.replace(" ", "_"))
    snake = re.sub(r"_+", "_", snake).lower()
    return schema, snake if prefix is None else "%s_%s" % (prefix, snake)


def fqn(catalog, legacyName):
    schema, table = deltaName(legacyName)
    return naming.table(catalog, schema, table)


# --------------------------------------------------------------------------- silver.ref_*
REF_TABLES = {
    "ref.Region": """
        RegionCode STRING NOT NULL, RegionName STRING NOT NULL, TaxRegimeCode STRING NOT NULL,
        DefaultCurrencyCode STRING NOT NULL, FiscalCalendarCode STRING NOT NULL, FiscalYearStartMonth INT NOT NULL,
        AddressRuleSetCode STRING NOT NULL, WeightUomCode STRING NOT NULL, ConsentModelCode STRING NOT NULL,
        DefaultRetentionMonths INT NOT NULL, DateFormatHint STRING NOT NULL, DecimalSeparator STRING NOT NULL,
        IsActive BOOLEAN NOT NULL""",
    "ref.Country": """
        CountryCode STRING NOT NULL, CountryCodeIso3 STRING, CountryName STRING NOT NULL, RegionCode STRING NOT NULL,
        SubRegionName STRING, LocalCurrencyCode STRING, PostalFormatMask STRING, PostalCodeRequiredFlag BOOLEAN NOT NULL,
        StateProvinceRequiredFlag BOOLEAN NOT NULL, AddressLineOrderCode STRING, VatRegistrationMask STRING,
        IsEuMemberState BOOLEAN NOT NULL, EuAccessionDate DATE, EuExitDate DATE, IsActive BOOLEAN NOT NULL""",
    "ref.Currency": """
        CurrencyCode STRING NOT NULL, CurrencyName STRING NOT NULL, CurrencySymbol STRING, MinorUnitDigits INT NOT NULL,
        RoundingRuleCode STRING NOT NULL, IsReportingCurrency BOOLEAN NOT NULL, IsEuroLegacy BOOLEAN NOT NULL,
        EuroFixedRate DECIMAL(19,8), RetiredDate DATE, IsActive BOOLEAN NOT NULL""",
    "ref.FxRateDaily": """
        FromCurrencyCode STRING NOT NULL, ToCurrencyCode STRING NOT NULL, RateDate DATE NOT NULL,
        RateTypeCode STRING NOT NULL, ConversionRate DECIMAL(19,8) NOT NULL, RateSourceCode STRING,
        IsTreasuryOverride BOOLEAN NOT NULL, EffectiveFromUtc TIMESTAMP, EffectiveToUtc TIMESTAMP, LoadedFromBatchId BIGINT""",
    "ref.UnitOfMeasure": """
        UomCode STRING NOT NULL, UomName STRING NOT NULL, UomClassCode STRING NOT NULL, BaseUomCode STRING NOT NULL,
        IsBaseUom BOOLEAN NOT NULL, DecimalPrecision INT NOT NULL, IsActive BOOLEAN NOT NULL""",
    "ref.UomConversion": """
        FromUomCode STRING NOT NULL, ToUomCode STRING NOT NULL, StockItemBusinessKey STRING NOT NULL,
        ConversionFactor DECIMAL(18,8) NOT NULL, IsItemSpecific BOOLEAN NOT NULL, EffectiveFromDate DATE,
        MaintainedByName STRING, MaintenanceNote STRING""",
    "ref.StatusCode": """
        StatusDomainCode STRING NOT NULL, ConformedStatusCode STRING NOT NULL, ConformedStatusName STRING NOT NULL,
        StatusGroupCode STRING, SortOrder INT, IsTerminalStatus BOOLEAN NOT NULL, IsActive BOOLEAN NOT NULL""",
    "ref.ReasonCode": """
        ReasonDomainCode STRING NOT NULL, ConformedReasonCode STRING NOT NULL, ConformedReasonName STRING NOT NULL,
        ReasonGroupCode STRING, IsCustomerFault BOOLEAN, IsSupplierFault BOOLEAN, RequiresApproval BOOLEAN NOT NULL,
        IsActive BOOLEAN NOT NULL""",
    "ref.TaxJurisdiction": """
        TaxJurisdictionCode STRING NOT NULL, TaxJurisdictionName STRING NOT NULL, TaxRegimeCode STRING NOT NULL,
        CountryCode STRING NOT NULL, StateProvinceCode STRING, CountyOrDistrictName STRING, CityName STRING,
        PostalCodeLow STRING, PostalCodeHigh STRING, CombinedRatePercent DECIMAL(9,4), StateRatePercent DECIMAL(9,4),
        CountyRatePercent DECIMAL(9,4), CityRatePercent DECIMAL(9,4), SpecialDistrictRatePercent DECIMAL(9,4),
        ReverseChargeEligible BOOLEAN NOT NULL, RegistrationRequiredFlag BOOLEAN NOT NULL,
        EffectiveFromDate DATE, EffectiveToDate DATE""",
    "ref.CodeCrosswalk": """
        CrosswalkId BIGINT NOT NULL, CodeDomainCode STRING NOT NULL, SourceSystemCode STRING NOT NULL,
        SourceCodeValue STRING NOT NULL, SourceCodeDescription STRING, ConformedCodeValue STRING NOT NULL,
        RegionCode STRING, IsDefaultForConformed BOOLEAN NOT NULL, EffectiveFromDate DATE NOT NULL, EffectiveToDate DATE,
        MaintainedByName STRING, MaintenanceNote STRING""",
    "ref.SourceKeyCrosswalk": """
        CrosswalkId BIGINT NOT NULL, EntityName STRING NOT NULL, SourceSystemCode STRING NOT NULL,
        SourceKeyValue STRING NOT NULL, ConformedBusinessKey STRING NOT NULL, MatchMethodCode STRING NOT NULL,
        SupersededByBusinessKey STRING, IsActive BOOLEAN NOT NULL, CreatedAtUtc TIMESTAMP NOT NULL, MaintainedByName STRING""",
    "ref.PostalFormatRule": """
        RuleId INT NOT NULL, CountryCode STRING NOT NULL, RuleSetCode STRING NOT NULL, RulePriority INT NOT NULL,
        StripCharacters STRING, UpperCaseFlag BOOLEAN NOT NULL, FormatMask STRING, MinimumLength INT, MaximumLength INT,
        TruncateToLength INT, InsertSeparatorAt INT, SeparatorCharacter STRING, RuleNote STRING""",
}

# --------------------------------------------------------------------------- silver.err_*
ERR_TABLES = {
    "err.RejectedLookupFailure": """
        RejectId BIGINT NOT NULL, BatchId BIGINT NOT NULL, PackageExecutionId BIGINT, SourceObjectName STRING NOT NULL,
        SourceBusinessKey STRING, LookupName STRING NOT NULL, LookupColumnName STRING, LookupValue STRING,
        SourceSystemCode STRING, RejectReasonCode STRING NOT NULL, RejectReason STRING, RejectStage STRING NOT NULL,
        RoutedToUnknownMember BOOLEAN NOT NULL, QueuedForLateArrival BOOLEAN NOT NULL, OccurrenceCount INT NOT NULL,
        RecordPayload STRING, ReprocessStatusCode STRING NOT NULL, RejectedAtUtc TIMESTAMP NOT NULL""",
    "err.RejectedConstraintViolation": """
        RejectId BIGINT NOT NULL, BatchId BIGINT NOT NULL, PackageExecutionId BIGINT, TargetObjectName STRING NOT NULL,
        ConstraintName STRING, ConstraintTypeCode STRING, ViolatingBusinessKey STRING, ViolatingColumnName STRING, ViolatingValue STRING, SqlErrorNumber INT,
        SqlErrorMessage STRING, RejectReasonCode STRING NOT NULL, RejectReason STRING, RejectStage STRING NOT NULL,
        RecordPayload STRING, ReprocessStatusCode STRING NOT NULL, RejectedAtUtc TIMESTAMP NOT NULL""",
}

# --------------------------------------------------------------------------- gold.dim_*
# Every SCD1 dimension carries the same audit tail; the surrogate key is assigned by delta_io
# (max + row_number) so that the reserved members (-1/-2/-3/-9) and existing keys stay stable.
SCD1_AUDIT = """
        ChangeHash STRING, LineageKey STRING NOT NULL, LastLoadBatchId BIGINT,
        ValidFrom TIMESTAMP NOT NULL, ValidTo TIMESTAMP NOT NULL"""

DIMENSIONS = {
    "Dimension.Currency": ("CurrencyKey", ["CurrencyCode"], """
        CurrencyCode STRING NOT NULL, CurrencyName STRING NOT NULL, CurrencySymbol STRING, MinorUnitDigits INT,
        RoundingRuleCode STRING, IsReportingCurrency BOOLEAN, IsEuroLegacy BOOLEAN, RetiredDate DATE,
        LatestRateToUsd DECIMAL(19,8), LatestRateDate DATE, CurrencyStatusCode STRING, RateStalenessDays INT,
        RateStatusCode STRING"""),
    "Dimension.Payment Method": ("PaymentMethodKey", ["PaymentMethodCode", "RegionCode"], """
        PaymentMethodCode STRING NOT NULL, PaymentMethodName STRING NOT NULL, RegionCode STRING NOT NULL,
        SettlementTypeCode STRING, ElectronicFlag STRING, EffectiveFromDate DATE, SourceCodeCount BIGINT"""),
    "Dimension.Transaction Type": ("TransactionTypeKey", ["TransactionTypeCode", "RegionCode"], """
        TransactionTypeCode STRING NOT NULL, TransactionTypeName STRING NOT NULL, RegionCode STRING NOT NULL,
        MovementDirectionCode STRING, MovementSign INT, AffectsLedgerFlag STRING, ReversalAllowedFlag STRING,
        EffectiveFromDate DATE, SourceCodeCount BIGINT"""),
    "Dimension.Unit Of Measure": ("UnitOfMeasureKey", ["UomCode"], """
        UomCode STRING NOT NULL, UomName STRING NOT NULL, UomClassCode STRING, BaseUomCode STRING,
        DecimalPrecision INT, ConversionFactor DECIMAL(18,8), BaseFactorText STRING"""),
    "Dimension.Payment Terms": ("PaymentTermsKey", ["PaymentTermsCode", "RegionCode"], """
        PaymentTermsCode STRING NOT NULL, PaymentTermsName STRING NOT NULL, RegionCode STRING NOT NULL,
        NetDays INT, DiscountDays INT, DiscountPercent DECIMAL(9,4), NetDaysCapped INT, EarlySettlementFlag STRING,
        EffectiveFromDate DATE, SourceCodeCount BIGINT"""),
    "Dimension.Return Reason": ("ReturnReasonKey", ["ReturnReasonCode"], """
        ReturnReasonCode STRING NOT NULL, ReturnReasonName STRING NOT NULL, ReasonGroupCode STRING,
        ReasonCategoryCode STRING, IsCustomerFault BOOLEAN, IsSupplierFault BOOLEAN, RequiresApproval BOOLEAN,
        SupplierRecoverableFlag STRING, RestockingFeePercentEu DECIMAL(9,4), RestockingFeePercentNa DECIMAL(9,4),
        RestockingFeePercentApac DECIMAL(9,4), SourceCodeCount BIGINT"""),
    "Dimension.Loyalty Tier": ("LoyaltyTierKey", ["LoyaltyTierCode", "RegionCode"], """
        LoyaltyTierCode STRING NOT NULL, TierName STRING NOT NULL, RegionCode STRING NOT NULL, TierRank INT,
        DiscountPercent DECIMAL(9,4), PointsMultiplier DECIMAL(18,2), EffectiveFromDate DATE, SourceCodeCount BIGINT"""),
    "Dimension.Sales Channel": ("SalesChannelKey", ["ChannelCode", "RegionCode"], """
        ChannelCode STRING NOT NULL, ChannelName STRING NOT NULL, RegionCode STRING NOT NULL, ChannelGroupCode STRING,
        DigitalFlag STRING, EffectiveFromDate DATE, SourceCodeCount BIGINT"""),
    "Dimension.Carrier": ("CarrierKey", ["CarrierCode", "RegionCode"], """
        CarrierCode STRING NOT NULL, CarrierName STRING NOT NULL, RegionCode STRING NOT NULL, CrossBorderFlag STRING,
        OnTimeTargetDays DECIMAL(9,2), OwnFleetFlag STRING, EffectiveFromDate DATE, SourceCodeCount BIGINT"""),
    "Dimension.Warehouse Site": ("WarehouseSiteKey", ["WarehouseSiteId"], """
        WarehouseSiteId INT NOT NULL, WarehouseSiteCode STRING NOT NULL, WarehouseSiteName STRING, CountryCode STRING,
        RegionCode STRING, RegionName STRING, AddressRuleSetCode STRING, WeightUomCode STRING, PostalCode STRING,
        PostalCodeStandardized STRING, SiteTypeCode STRING, MovementCount BIGINT"""),
    "Dimension.Geography": ("GeographyKey", ["GeographyCode"], """
        GeographyCode STRING NOT NULL, GeographyName STRING NOT NULL, CountryCode STRING NOT NULL, CountryCodeIso3 STRING,
        CountryName STRING, RegionCode STRING NOT NULL, SubRegionName STRING, LocalCurrencyCode STRING, CurrencyName STRING,
        MinorUnitDigits INT, TaxRegimeCode STRING, TaxStructureCode STRING, TaxJurisdictionCode STRING,
        CombinedRatePercent DECIMAL(9,4), ReverseChargeFlag STRING, IsEuMemberState BOOLEAN, EuExitDate DATE,
        EuStatusCode STRING, PostalFormatMask STRING"""),
    "Dimension.Unknown Member": ("UnknownMemberRowKey", ["ReferenceTableName", "DomainCode"], """
        ReferenceTableName STRING NOT NULL, DomainCode STRING NOT NULL, UnknownCodeValue STRING, UnknownDescription STRING,
        UnknownMemberKey INT, NotApplicableKey INT, IsUnknownMemberFlag STRING"""),
}

SCD2_DIMENSIONS = {
    "Dimension.Cost Center": ("CostCenterKey", ["CostCenterCode", "RegionCode"], """
        CostCenterCode STRING NOT NULL, CostCenterName STRING NOT NULL, RegionCode STRING NOT NULL,
        ParentCostCenterCode STRING, CompanyCode STRING, FunctionCode STRING, SuspenseFlag STRING,
        EffectiveFromDate DATE, SourceCodeCount BIGINT,
        EffectiveFrom TIMESTAMP NOT NULL, EffectiveTo TIMESTAMP NOT NULL, IsCurrentRow BOOLEAN NOT NULL,
        VersionNumber INT NOT NULL, RowHashType2 STRING, LineageKey STRING NOT NULL, LastLoadBatchId BIGINT,
        ValidFrom TIMESTAMP NOT NULL, ValidTo TIMESTAMP NOT NULL"""),
}

DATE_DIMENSION = ("Dimension.Date", "DateKey", """
        DateKey INT NOT NULL, Date DATE NOT NULL, DayNumber INT NOT NULL, Day STRING NOT NULL, DayOfWeek STRING NOT NULL,
        DayOfWeekNumber INT NOT NULL, Month STRING NOT NULL, ShortMonth STRING NOT NULL, CalendarMonthNumber INT NOT NULL,
        CalendarMonthLabel STRING NOT NULL, CalendarQuarterNumber INT NOT NULL, CalendarYear INT NOT NULL,
        CalendarYearLabel STRING NOT NULL, FiscalMonthNumber INT NOT NULL, FiscalMonthLabel STRING NOT NULL,
        FiscalYear INT NOT NULL, FiscalYearLabel STRING NOT NULL, ISOWeekNumber INT NOT NULL, ISOYear INT NOT NULL,
        DayOfYear INT NOT NULL, WeekendFlag STRING NOT NULL, WorkingDayFlag STRING NOT NULL,
        FiscalYearNa INT, FiscalPeriodNa INT, FiscalQuarterNa INT, FiscalWeekNa INT,
        FiscalYearEu INT, FiscalPeriodEu INT, FiscalQuarterEu INT, FiscalWeekEu INT,
        FiscalYearApac INT, FiscalPeriodApac INT, FiscalQuarterApac INT,
        FiscalYearApacAu INT, FiscalPeriodApacAu INT, FiscalQuarterApacAu INT,
        IsReservedMember BOOLEAN NOT NULL, LineageKey STRING NOT NULL, LastLoadBatchId BIGINT""")

FISCAL_CALENDAR = ("Dimension.Fiscal Calendar", "FiscalCalendarKey", """
        FiscalCalendarKey BIGINT NOT NULL, CountryCode STRING NOT NULL, Date DATE NOT NULL, DateKey INT NOT NULL,
        RegionCode STRING, FiscalCalendarCode STRING, FiscalYear INT, FiscalYearLabel STRING, FiscalQuarter INT,
        FiscalPeriod INT, FiscalPeriodLabel STRING, FiscalWeek INT, PeriodStartDate DATE, PeriodEndDate DATE,
        IsPeriodEnd BOOLEAN, IsYearEnd BOOLEAN, IsPublicHoliday BOOLEAN, HolidayName STRING, HolidayScopeCode STRING,
        HolidaySubdivisionCode STRING, IsWorkingDay BOOLEAN, IsWarehouseOperatingDay BOOLEAN, IsCarrierCollectionDay BOOLEAN,
        WorkingDayOfMonth INT, WorkingDayOfYear INT, WorkingDaysRemainingMonth INT, NextWorkingDate DATE,
        PreviousWorkingDate DATE, TaxReturnPeriodLabel STRING, TaxReturnDueDate DATE, StatutoryCloseDueDate DATE,
        SourceSystemCode STRING, LastLoadBatchId BIGINT""")


def _create(spark, tableFqn, columnsSql, comment):
    spark.sql(
        "CREATE TABLE IF NOT EXISTS %s (%s) USING DELTA COMMENT '%s'"
        % (tableFqn, columnsSql, comment.replace("'", ""))
    )


def ensureSchemas(spark, catalog):
    for schema in ("silver", "gold"):
        spark.sql("CREATE SCHEMA IF NOT EXISTS %s.%s" % (catalog, schema))


def ensureReferenceTables(spark, catalog):
    for legacy, cols in REF_TABLES.items():
        _create(spark, fqn(catalog, legacy), cols, "Legacy %s (sqlserver/staging/tables/50_ref_tables.sql)" % legacy)


def ensureErrorTables(spark, catalog):
    for legacy, cols in ERR_TABLES.items():
        _create(spark, fqn(catalog, legacy), cols, "Legacy %s (sqlserver/staging/tables/40_err_tables.sql)" % legacy)


def ensureDimension(spark, catalog, legacyName):
    if legacyName in DIMENSIONS:
        keyCol, _, cols = DIMENSIONS[legacyName]
        _create(spark, fqn(catalog, legacyName), "%s BIGINT NOT NULL, %s, %s" % (keyCol, cols, SCD1_AUDIT),
                "Legacy %s (SCD1, WWI_ReferenceData)" % legacyName)
    elif legacyName in SCD2_DIMENSIONS:
        keyCol, _, cols = SCD2_DIMENSIONS[legacyName]
        _create(spark, fqn(catalog, legacyName), "%s BIGINT NOT NULL, %s" % (keyCol, cols),
                "Legacy %s (SCD2, WWI_ReferenceData)" % legacyName)
    elif legacyName == DATE_DIMENSION[0]:
        _create(spark, fqn(catalog, legacyName), DATE_DIMENSION[2], "Legacy Dimension.Date (static, three regional calendars)")
    elif legacyName == FISCAL_CALENDAR[0]:
        _create(spark, fqn(catalog, legacyName), FISCAL_CALENDAR[2], "Legacy Dimension.Fiscal Calendar (outrigger on Date)")
    else:
        raise KeyError("unknown dimension %s" % legacyName)


def ownedDimensions():
    return list(DIMENSIONS) + list(SCD2_DIMENSIONS) + [DATE_DIMENSION[0], FISCAL_CALENDAR[0]]


def dimensionKeyColumn(legacyName):
    if legacyName in DIMENSIONS:
        return DIMENSIONS[legacyName][0]
    if legacyName in SCD2_DIMENSIONS:
        return SCD2_DIMENSIONS[legacyName][0]
    if legacyName == DATE_DIMENSION[0]:
        return DATE_DIMENSION[1]
    if legacyName == FISCAL_CALENDAR[0]:
        return FISCAL_CALENDAR[1]
    raise KeyError(legacyName)


def ensureAll(spark, catalog):
    ensureSchemas(spark, catalog)
    ensureReferenceTables(spark, catalog)
    ensureErrorTables(spark, catalog)
    for legacyName in ownedDimensions():
        ensureDimension(spark, catalog, legacyName)
