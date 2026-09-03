"""Emit the WWI_Sales domain packages (ssis/11_sales).

Commission is the classic example of the estate's regional divergence: three
plans, three calendars, three tax treatments, and three separate packages that
have been maintained independently since each region was acquired. They are
deliberately NOT refactored into one parameterised package - the duplication is
the point, and every attempt to merge them has been abandoned when the payroll
teams could not agree on a common accrual month.

    NA   - USD only, commission on gross invoiced amount including sales tax,
           calendar month, accelerators above quota.
    EU   - commission on the VAT-exclusive net amount, EUR reporting currency,
           calendar month, statutory cap per country.
    APAC - GST-exclusive, 4-4-5 reporting periods, plan currency per country
           with a month-average FX conversion.

Run:  python3 ssis/11_sales/build_sales_packages.py
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import project  # noqa: E402
from patterns import (CONN_DW, CONN_FILES, CONN_STAGING, exec_proc,  # noqa: E402
                      log_package_start, log_package_success, log_row_count,
                      new_package, truncate)
from ssisgen import (Column, DataFlow, DataFlowTask, ExecuteSql, Expression,  # noqa: E402
                     date_col, int_col, money_col, str_col)

PROJECT_NAME = "WWI_Sales"
CONNECTIONS = ["WWI_Staging_DB", "WWI_DW_Destination_DB", "WWI_Archive_Files", "WWI_Reject_Files"]


def bool_col(name):
    return Column(name, "bool")


def rate_col(name):
    return Column(name, "numeric", precision=18, scale=8)


# ---------------------------------------------------------------------------
# SLS_NA_Load_Commission
# ---------------------------------------------------------------------------

NA_COMMISSION_SQL = """
SELECT  sl.SaleLineId
,       sl.InvoiceNumber
,       sl.InvoiceDate
,       sl.SalespersonPersonId
,       sl.CustomerId
,       sl.StockItemId
,       sl.TerritoryCode
,       sl.QuantitySold
,       sl.ExtendedPrice
,       sl.TaxAmount
,       sl.LineProfit
        /* NA pays on the gross invoiced amount, tax included. Payroll has
           refused to change this because the plan documents say "invoiced". */
,       sl.ExtendedPrice + sl.TaxAmount                 AS CommissionableAmount
,       cp.PlanCode
,       cp.BaseRatePercent
,       cp.AcceleratorRatePercent
,       cp.AcceleratorThresholdAmount
,       CONVERT(char(7), sl.InvoiceDate, 126)           AS CommissionPeriod
,       'USD'                                           AS PlanCurrencyCode
FROM    stg.SaleLine AS sl
INNER JOIN stg.CommissionPlan AS cp
        ON  cp.SalespersonPersonId = sl.SalespersonPersonId
        AND cp.RegionCode = 'NA'
        AND sl.InvoiceDate BETWEEN cp.EffectiveFrom AND ISNULL(cp.EffectiveTo, '9999-12-31')
WHERE   sl.RegionCode = 'NA'
AND     sl.CurrencyCode = 'USD'
AND     sl.LoadBatchId = ?
AND     sl.LineTypeCode NOT IN ('SAMPLE', 'INTERNAL')
""".strip()


def build_sls_na_load_commission():
    pkg = new_package(
        "SLS_NA_Load_Commission",
        "North American commission calculation. USD only, commission is earned on the gross "
        "invoiced amount including sales tax, and the accelerator rate applies to the portion of "
        "the month's commissionable amount above the plan threshold. House-account sales are paid "
        "at half rate, which is a rule that predates the data warehouse.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("CommissionPeriod", "1900-01", "string"), ("UnplannedRepCount", 0, "int")],
    )
    pkg.add_parameter("CommissionMonth", "1900-01", dtype="string",
                      description="Calendar commission month, YYYY-MM.")
    pkg.add_parameter("HouseAccountRatePercent", 50, dtype="int",
                      description="Percentage of the plan rate paid on house accounts.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.CommissionNa"))
    unplanned = pkg.add(ExecuteSql(
        "Find Reps Without A Plan",
        CONN_STAGING,
        "SELECT COUNT(DISTINCT sl.SalespersonPersonId) AS UnplannedReps "
        "FROM   stg.SaleLine AS sl "
        "WHERE  sl.RegionCode = N'NA' AND sl.LoadBatchId = ? "
        "AND    NOT EXISTS (SELECT 1 FROM stg.CommissionPlan AS cp "
        "                   WHERE cp.SalespersonPersonId = sl.SalespersonPersonId "
        "                     AND cp.RegionCode = N'NA');",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::UnplannedRepCount")],
    ))

    columns = [
        int_col("SaleLineId"), str_col("InvoiceNumber", 30), date_col("InvoiceDate"),
        int_col("SalespersonPersonId"), int_col("CustomerId"), int_col("StockItemId"),
        str_col("TerritoryCode", 10), int_col("QuantitySold"), money_col("ExtendedPrice"),
        money_col("TaxAmount"), money_col("LineProfit"), money_col("CommissionableAmount"),
        str_col("PlanCode", 12), money_col("BaseRatePercent"), money_col("AcceleratorRatePercent"),
        money_col("AcceleratorThresholdAmount"), str_col("CommissionPeriod", 7),
        str_col("PlanCurrencyCode", 3),
    ]
    flow = DataFlow("Calculate NA Commission")
    flow.oledb_source("stg SaleLine NA", CONN_STAGING, NA_COMMISSION_SQL, columns, timeout=1800)
    flow.lookup("Lookup House Account Flag", CONN_DW,
                "SELECT [WWI Customer ID] AS CustomerId, [Is House Account] AS IsHouseAccount "
                "FROM Dimension.Customer WHERE [Valid To] > SYSDATETIME();",
                ["CustomerId"], [bool_col("IsHouseAccount")], no_match="IG")
    flow.derived_column("Apply NA Plan Rules", [
        ("BaseCommissionAmount",
         "CommissionableAmount * BaseRatePercent / 100", money_col("BaseCommissionAmount")),
        ("AcceleratorCommissionAmount",
         "CommissionableAmount > AcceleratorThresholdAmount ? "
         "(CommissionableAmount - AcceleratorThresholdAmount) * AcceleratorRatePercent / 100 : "
         "(DT_NUMERIC,18,2)0",
         money_col("AcceleratorCommissionAmount")),
        ("HouseAccountFactor",
         "ISNULL(IsHouseAccount) ? (DT_NUMERIC,18,2)1 : "
         "(IsHouseAccount ? (DT_NUMERIC,18,2)@[$Package::HouseAccountRatePercent] / 100 : "
         "(DT_NUMERIC,18,2)1)",
         money_col("HouseAccountFactor")),
    ])
    flow.derived_column("Derive Payable Commission", [
        ("CommissionAmount",
         "(BaseCommissionAmount + AcceleratorCommissionAmount) * HouseAccountFactor",
         money_col("CommissionAmount")),
        ("RegionCode", '(DT_WSTR,4)"NA"', str_col("RegionCode", 4)),
    ])
    flow.row_count("Count NA Commission Rows", "User::RowsRead")
    flow.oledb_destination("work CommissionNa", CONN_STAGING, "[work].[CommissionNa]",
                           batch_size=50000)
    calc = pkg.add(DataFlowTask(flow))

    post = pkg.add(exec_proc(
        "Post NA Commission",
        "EXEC Integration.usp_PostCommission @BatchId = ?, @RegionCode = N'NA', "
        "@CommissionPeriod = ?, @RowsUpdated = ? OUTPUT;",
        connection=CONN_DW,
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("$Package::CommissionMonth", 1, "NVARCHAR"),
        ],
    ))
    counts = pkg.add(log_row_count("Fact.Sale"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, unplanned)
    pkg.link(unplanned, calc, value="Completion")
    pkg.link(calc, post)
    pkg.link(post, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# SLS_EU_Load_Commission
# ---------------------------------------------------------------------------

EU_COMMISSION_SQL = """
SELECT  sl.SaleLineId
,       sl.InvoiceNumber
,       sl.InvoiceDate
,       sl.SalespersonPersonId
,       sl.CustomerId
,       sl.CountryCode
,       sl.TerritoryCode
,       sl.CurrencyCode
,       sl.ExtendedPrice
,       sl.VatAmount
,       sl.VatRatePercent
        /* EU pays on the VAT-exclusive net amount. Where the source system has
           already stored a net amount we trust it, otherwise we back the VAT
           out of the gross using the line's own rate. */
,       CASE
            WHEN sl.NetAmount IS NOT NULL THEN sl.NetAmount
            WHEN ISNULL(sl.VatRatePercent, 0) > 0
                THEN sl.ExtendedPrice / (1 + (sl.VatRatePercent / 100))
            ELSE sl.ExtendedPrice - ISNULL(sl.VatAmount, 0)
        END                                             AS NetCommissionableAmount
,       cp.PlanCode
,       cp.BaseRatePercent
,       cp.StatutoryCapAmount
,       CONVERT(char(7), sl.InvoiceDate, 126)           AS CommissionPeriod
,       fx.ConversionRate                               AS EurConversionRate
FROM    stg.SaleLine AS sl
INNER JOIN stg.CommissionPlan AS cp
        ON  cp.SalespersonPersonId = sl.SalespersonPersonId
        AND cp.RegionCode = 'EU'
        AND sl.InvoiceDate BETWEEN cp.EffectiveFrom AND ISNULL(cp.EffectiveTo, '9999-12-31')
LEFT OUTER JOIN stg.FxRate AS fx
        ON  fx.CurrencyCode = sl.CurrencyCode
        AND fx.QuoteCurrencyCode = 'EUR'
        AND fx.RateTypeCode = 'AVERAGE'
        AND fx.RateDate = EOMONTH(sl.InvoiceDate)
WHERE   sl.RegionCode = 'EU'
AND     sl.LoadBatchId = ?
AND     sl.LineTypeCode NOT IN ('SAMPLE', 'INTERNAL')
""".strip()


def build_sls_eu_load_commission():
    pkg = new_package(
        "SLS_EU_Load_Commission",
        "European commission calculation. Commission is earned on the VAT-exclusive net amount, "
        "converted to EUR at the month-average rate, and capped at the statutory per-country cap "
        "where one exists. Countries operating a works-council agreement accrue in the month of "
        "cash receipt rather than invoice, so those lines are held until payment is matched.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("CappedRepCount", 0, "int"), ("HeldOnCashCount", 0, "int")],
    )
    pkg.add_parameter("CommissionMonth", "1900-01", dtype="string",
                      description="Calendar commission month, YYYY-MM.")
    pkg.add_parameter("CashBasisCountries", "DE,AT", dtype="string",
                      description="Comma-separated countries accruing commission on cash receipt.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.CommissionEu"))

    columns = [
        int_col("SaleLineId"), str_col("InvoiceNumber", 30), date_col("InvoiceDate"),
        int_col("SalespersonPersonId"), int_col("CustomerId"), str_col("CountryCode", 2),
        str_col("TerritoryCode", 10), str_col("CurrencyCode", 3), money_col("ExtendedPrice"),
        money_col("VatAmount"), money_col("VatRatePercent"), money_col("NetCommissionableAmount"),
        str_col("PlanCode", 12), money_col("BaseRatePercent"), money_col("StatutoryCapAmount"),
        str_col("CommissionPeriod", 7), rate_col("EurConversionRate"),
    ]
    flow = DataFlow("Calculate EU Commission")
    flow.oledb_source("stg SaleLine EU", CONN_STAGING, EU_COMMISSION_SQL, columns, timeout=1800)
    flow.derived_column("Convert To EUR", [
        ("NetAmountEur",
         "ISNULL(EurConversionRate) || EurConversionRate == 0 ? NetCommissionableAmount : "
         "NetCommissionableAmount * (DT_NUMERIC,18,2)EurConversionRate",
         money_col("NetAmountEur")),
        ("IsCashBasisCountry",
         'FINDSTRING(@[$Package::CashBasisCountries], CountryCode, 1) > 0 ? (DT_BOOL)1 : (DT_BOOL)0',
         bool_col("IsCashBasisCountry")),
    ])
    flow.derived_column("Apply EU Plan Rules", [
        ("RawCommissionAmount", "NetAmountEur * BaseRatePercent / 100",
         money_col("RawCommissionAmount")),
        ("CommissionAmount",
         "StatutoryCapAmount > 0 && (NetAmountEur * BaseRatePercent / 100) > StatutoryCapAmount ? "
         "StatutoryCapAmount : NetAmountEur * BaseRatePercent / 100",
         money_col("CommissionAmount")),
        ("RegionCode", '(DT_WSTR,4)"EU"', str_col("RegionCode", 4)),
    ])
    flow.conditional_split("Split Cash Basis Lines", [
        ("Accrual", "IsCashBasisCountry == (DT_BOOL)0"),
        ("HeldOnCash", "IsCashBasisCountry == (DT_BOOL)1"),
    ])
    flow.row_count("Count EU Commission Rows", "User::RowsRead")
    flow.oledb_destination("work CommissionEu", CONN_STAGING, "[work].[CommissionEu]",
                           batch_size=50000)
    flow.branch_destination("work CommissionEuHeld", CONN_STAGING, "[work].[CommissionEuHeld]",
                            "Split Cash Basis Lines", "HeldOnCash")
    calc = pkg.add(DataFlowTask(flow))

    release = pkg.add(ExecuteSql(
        "Release Cash Basis Lines With Payment",
        CONN_STAGING,
        "INSERT INTO work.CommissionEu "
        "    (SaleLineId, SalespersonPersonId, CommissionPeriod, NetAmountEur, CommissionAmount, "
        "     RegionCode, CountryCode) "
        "SELECT h.SaleLineId, h.SalespersonPersonId, CONVERT(char(7), p.PaymentDate, 126), "
        "       h.NetAmountEur, h.CommissionAmount, h.RegionCode, h.CountryCode "
        "FROM   work.CommissionEuHeld AS h "
        "INNER JOIN stg.CustomerPayment AS p "
        "        ON  p.InvoiceNumber = h.InvoiceNumber "
        "        AND p.PaymentStatusCode = N'CLEARED';",
    ))
    cap_report = pkg.add(ExecuteSql(
        "Count Capped Representatives",
        CONN_STAGING,
        "SELECT COUNT(DISTINCT SalespersonPersonId) AS CappedReps FROM work.CommissionEu "
        "WHERE RawCommissionAmount > CommissionAmount;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::CappedRepCount")],
    ))
    post = pkg.add(exec_proc(
        "Post EU Commission",
        "EXEC Integration.usp_PostCommission @BatchId = ?, @RegionCode = N'EU', "
        "@CommissionPeriod = ?, @RowsUpdated = ? OUTPUT;",
        connection=CONN_DW,
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("$Package::CommissionMonth", 1, "NVARCHAR"),
        ],
    ))
    counts = pkg.add(log_row_count("Fact.Sale"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, calc)
    pkg.link(calc, release)
    pkg.link(release, cap_report, value="Completion")
    pkg.link(cap_report, post)
    pkg.link(post, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# SLS_APAC_Load_Commission
# ---------------------------------------------------------------------------

APAC_COMMISSION_SQL = """
SELECT  sl.SaleLineId
,       sl.InvoiceNumber
,       sl.InvoiceDate
,       sl.SalespersonPersonId
,       sl.CustomerId
,       sl.CountryCode
,       sl.TerritoryCode
,       sl.CurrencyCode
,       sl.ExtendedPrice
,       sl.GstAmount
,       sl.ExtendedPrice - ISNULL(sl.GstAmount, 0)      AS GstExclusiveAmount
,       cp.PlanCode
,       cp.BaseRatePercent
,       cp.PlanCurrencyCode
,       cp.TeamSplitPercent
        /* APAC reports on a 4-4-5 calendar, so the commission period comes from
           the date dimension and not from the invoice month. */
,       d.FiscalPeriod445                               AS CommissionPeriod
,       d.FiscalYear445                                 AS CommissionFiscalYear
,       d.FiscalWeek445                                 AS CommissionFiscalWeek
FROM    stg.SaleLine AS sl
INNER JOIN stg.CommissionPlan AS cp
        ON  cp.SalespersonPersonId = sl.SalespersonPersonId
        AND cp.RegionCode = 'APAC'
        AND sl.InvoiceDate BETWEEN cp.EffectiveFrom AND ISNULL(cp.EffectiveTo, '9999-12-31')
INNER JOIN stg.FiscalCalendar445 AS d
        ON  d.CalendarDate = CAST(sl.InvoiceDate AS date)
WHERE   sl.RegionCode = 'APAC'
AND     sl.LoadBatchId = ?
AND     sl.LineTypeCode NOT IN ('SAMPLE', 'INTERNAL')
""".strip()


def build_sls_apac_load_commission():
    pkg = new_package(
        "SLS_APAC_Load_Commission",
        "Asia-Pacific commission calculation on the 4-4-5 reporting calendar. Commission is earned "
        "on the GST-exclusive amount, converted into the plan currency of the representative's "
        "home country at the period-average rate, and team-selling splits are applied before the "
        "amount is posted. Because the 4-4-5 period boundary rarely lines up with the calendar "
        "month, the last invoices of a month regularly fall into the next commission period.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("PeriodBoundaryCount", 0, "int"), ("MissingCalendarCount", 0, "int")],
    )
    pkg.add_parameter("FiscalPeriod445", "1900-P01", dtype="string",
                      description="4-4-5 commission period identifier.")
    pkg.add_parameter("TeamSplitEnabled", "True", dtype="bool",
                      description="Applies the plan's team-selling split percentages.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.CommissionApac"))
    calendar_gap = pkg.add(ExecuteSql(
        "Check 445 Calendar Coverage",
        CONN_STAGING,
        "SELECT COUNT(*) AS MissingCalendarDays FROM ("
        "    SELECT DISTINCT CAST(InvoiceDate AS date) AS CalendarDate "
        "    FROM   stg.SaleLine WHERE RegionCode = N'APAC' AND LoadBatchId = ?"
        ") AS d "
        "WHERE NOT EXISTS (SELECT 1 FROM stg.FiscalCalendar445 AS f "
        "                  WHERE f.CalendarDate = d.CalendarDate);",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::MissingCalendarCount")],
    ))

    columns = [
        int_col("SaleLineId"), str_col("InvoiceNumber", 30), date_col("InvoiceDate"),
        int_col("SalespersonPersonId"), int_col("CustomerId"), str_col("CountryCode", 2),
        str_col("TerritoryCode", 10), str_col("CurrencyCode", 3), money_col("ExtendedPrice"),
        money_col("GstAmount"), money_col("GstExclusiveAmount"), str_col("PlanCode", 12),
        money_col("BaseRatePercent"), str_col("PlanCurrencyCode", 3), money_col("TeamSplitPercent"),
        str_col("CommissionPeriod", 8), int_col("CommissionFiscalYear"), int_col("CommissionFiscalWeek"),
    ]
    flow = DataFlow("Calculate APAC Commission")
    flow.oledb_source("stg SaleLine APAC", CONN_STAGING, APAC_COMMISSION_SQL, columns, timeout=1800)
    flow.lookup("Lookup Period Average Rate", CONN_STAGING,
                "SELECT CurrencyCode, QuoteCurrencyCode AS PlanCurrencyCode, ConversionRate "
                "FROM stg.FxRate WHERE RateTypeCode = N'AVERAGE';",
                ["CurrencyCode", "PlanCurrencyCode"], [rate_col("ConversionRate")], no_match="RD")
    flow.derived_column("Apply APAC Plan Rules", [
        ("PlanCurrencyAmount",
         "GstExclusiveAmount * (DT_NUMERIC,18,2)ConversionRate", money_col("PlanCurrencyAmount")),
        ("SplitFactor",
         "@[$Package::TeamSplitEnabled] ? TeamSplitPercent / 100 : (DT_NUMERIC,18,2)1",
         money_col("SplitFactor")),
    ])
    flow.derived_column("Derive APAC Commission", [
        ("CommissionAmount",
         "PlanCurrencyAmount * BaseRatePercent / 100 * SplitFactor", money_col("CommissionAmount")),
        ("RegionCode", '(DT_WSTR,4)"APAC"', str_col("RegionCode", 4)),
        ("IsPeriodBoundaryLine",
         'DATEPART(month, InvoiceDate) != (DT_I4)SUBSTRING(CommissionPeriod, 7, 2) ? '
         '(DT_BOOL)1 : (DT_BOOL)0',
         bool_col("IsPeriodBoundaryLine")),
    ])
    flow.row_count("Count APAC Commission Rows", "User::RowsRead")
    flow.oledb_destination("work CommissionApac", CONN_STAGING, "[work].[CommissionApac]",
                           batch_size=50000)
    flow.reject_destination("Reject Missing FX", CONN_STAGING, "[err].[CommissionApacReject]",
                            "Lookup Period Average Rate", "Lookup No Match Output")
    calc = pkg.add(DataFlowTask(flow))

    boundary = pkg.add(ExecuteSql(
        "Count Period Boundary Lines",
        CONN_STAGING,
        "SELECT COUNT(*) AS BoundaryLines FROM work.CommissionApac WHERE IsPeriodBoundaryLine = 1;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::PeriodBoundaryCount")],
    ))
    post = pkg.add(exec_proc(
        "Post APAC Commission",
        "EXEC Integration.usp_PostCommission @BatchId = ?, @RegionCode = N'APAC', "
        "@CommissionPeriod = ?, @RowsUpdated = ? OUTPUT;",
        connection=CONN_DW,
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("$Package::FiscalPeriod445", 1, "NVARCHAR"),
        ],
    ))
    counts = pkg.add(log_row_count("Fact.Sale"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, calendar_gap)
    pkg.link(calendar_gap, calc, expression="@[User::MissingCalendarCount] == 0")
    pkg.link(calc, boundary, value="Completion")
    pkg.link(boundary, post)
    pkg.link(post, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# SLS_Load_QuotaAttainment
# ---------------------------------------------------------------------------


def build_sls_load_quotaattainment():
    pkg = new_package(
        "SLS_Load_QuotaAttainment",
        "Quota attainment by territory and representative. Attainment is measured against the "
        "region's own definition of bookings - NA measures invoiced revenue, EU measures net "
        "revenue after credit notes, APAC measures order intake - and the three are unioned into "
        "the regional sales performance aggregate.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("TerritoryCount", 0, "int"), ("MissingQuotaCount", 0, "int")],
    )
    pkg.add_parameter("AttainmentPeriod", "1900-01", dtype="string",
                      description="Period being measured; APAC rows use the mapped 4-4-5 period.")
    pkg.add_parameter("IncludePartialPeriod", "True", dtype="bool",
                      description="Include an in-flight period pro-rated to date.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.QuotaAttainment"))
    missing = pkg.add(ExecuteSql(
        "Find Territories Without Quota",
        CONN_STAGING,
        "SELECT COUNT(*) AS MissingQuotas FROM stg.SalesTerritory AS t "
        "WHERE t.IsActive = 1 "
        "AND NOT EXISTS (SELECT 1 FROM stg.SalesQuota AS q "
        "                WHERE q.TerritoryCode = t.TerritoryCode AND q.QuotaPeriod = ?);",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::AttainmentPeriod", 0, "NVARCHAR")],
        result_bindings=[("0", "User::MissingQuotaCount")],
    ))
    build = pkg.add(ExecuteSql(
        "Build Attainment By Region",
        CONN_STAGING,
        "INSERT INTO work.QuotaAttainment\n"
        "    (TerritoryCode, RegionCode, QuotaPeriod, QuotaAmount, ActualAmount, MeasureBasisCode)\n"
        "SELECT  t.TerritoryCode, N'NA', q.QuotaPeriod, q.QuotaAmount,\n"
        "        ISNULL(SUM(sl.ExtendedPrice + sl.TaxAmount), 0), N'INVOICED'\n"
        "FROM    stg.SalesTerritory AS t\n"
        "INNER JOIN stg.SalesQuota AS q ON q.TerritoryCode = t.TerritoryCode AND q.QuotaPeriod = ?\n"
        "LEFT OUTER JOIN stg.SaleLine AS sl\n"
        "        ON  sl.TerritoryCode = t.TerritoryCode AND sl.RegionCode = N'NA'\n"
        "        AND CONVERT(char(7), sl.InvoiceDate, 126) = q.QuotaPeriod\n"
        "WHERE   t.RegionCode = N'NA'\n"
        "GROUP BY t.TerritoryCode, q.QuotaPeriod, q.QuotaAmount\n"
        "UNION ALL\n"
        "SELECT  t.TerritoryCode, N'EU', q.QuotaPeriod, q.QuotaAmount,\n"
        "        ISNULL(SUM(sl.NetAmount), 0) - ISNULL(SUM(cn.CreditAmount), 0), N'NET_OF_CREDITS'\n"
        "FROM    stg.SalesTerritory AS t\n"
        "INNER JOIN stg.SalesQuota AS q ON q.TerritoryCode = t.TerritoryCode AND q.QuotaPeriod = ?\n"
        "LEFT OUTER JOIN stg.SaleLine AS sl\n"
        "        ON  sl.TerritoryCode = t.TerritoryCode AND sl.RegionCode = N'EU'\n"
        "        AND CONVERT(char(7), sl.InvoiceDate, 126) = q.QuotaPeriod\n"
        "LEFT OUTER JOIN stg.CreditNote AS cn\n"
        "        ON  cn.InvoiceNumber = sl.InvoiceNumber\n"
        "WHERE   t.RegionCode = N'EU'\n"
        "GROUP BY t.TerritoryCode, q.QuotaPeriod, q.QuotaAmount\n"
        "UNION ALL\n"
        "SELECT  t.TerritoryCode, N'APAC', q.QuotaPeriod, q.QuotaAmount,\n"
        "        ISNULL(SUM(ol.OrderLineAmount), 0), N'ORDER_INTAKE'\n"
        "FROM    stg.SalesTerritory AS t\n"
        "INNER JOIN stg.SalesQuota AS q ON q.TerritoryCode = t.TerritoryCode AND q.QuotaPeriod = ?\n"
        "LEFT OUTER JOIN stg.OrderLine AS ol\n"
        "        ON  ol.TerritoryCode = t.TerritoryCode\n"
        "LEFT OUTER JOIN stg.FiscalCalendar445 AS f\n"
        "        ON  f.CalendarDate = CAST(ol.OrderDate AS date)\n"
        "        AND f.FiscalPeriod445 = q.QuotaPeriod\n"
        "WHERE   t.RegionCode = N'APAC'\n"
        "GROUP BY t.TerritoryCode, q.QuotaPeriod, q.QuotaAmount;",
        parameter_bindings=[
            ("$Package::AttainmentPeriod", 0, "NVARCHAR"),
            ("$Package::AttainmentPeriod", 1, "NVARCHAR"),
            ("$Package::AttainmentPeriod", 2, "NVARCHAR"),
        ],
    ))

    columns = [
        str_col("TerritoryCode", 10), str_col("RegionCode", 4), str_col("QuotaPeriod", 8),
        money_col("QuotaAmount"), money_col("ActualAmount"), str_col("MeasureBasisCode", 16),
    ]
    flow = DataFlow("Publish Quota Attainment")
    flow.oledb_source(
        "work QuotaAttainment", CONN_STAGING,
        "SELECT TerritoryCode, RegionCode, QuotaPeriod, QuotaAmount, ActualAmount, MeasureBasisCode "
        "FROM work.QuotaAttainment;", columns, timeout=600)
    flow.derived_column("Derive Attainment Metrics", [
        ("AttainmentPercent",
         "QuotaAmount == 0 ? (DT_NUMERIC,18,2)0 : ActualAmount * 100 / QuotaAmount",
         money_col("AttainmentPercent")),
        ("AttainmentBandCode",
         'QuotaAmount == 0 ? (DT_WSTR,8)"NOQUOTA" : '
         '(ActualAmount * 100 / QuotaAmount >= 120 ? (DT_WSTR,8)"OVER120" : '
         '(ActualAmount * 100 / QuotaAmount >= 100 ? (DT_WSTR,8)"AT" : '
         '(ActualAmount * 100 / QuotaAmount >= 80 ? (DT_WSTR,8)"NEAR" : (DT_WSTR,8)"UNDER")))',
         str_col("AttainmentBandCode", 8)),
    ])
    flow.row_count("Count Attainment Rows", "User::RowsInserted")
    flow.oledb_destination("Aggregate Regional Sales Performance", CONN_DW,
                           "[Aggregate].[Regional Sales Performance]", batch_size=20000)
    publish = pkg.add(DataFlowTask(flow))

    counts = pkg.add(log_row_count("Aggregate.Regional Sales Performance"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, missing)
    pkg.link(missing, build, value="Completion")
    pkg.link(build, publish)
    pkg.link(publish, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# SLS_Load_PromotionRedemption
# ---------------------------------------------------------------------------

PROMOTION_SQL = """
SELECT  p.PromotionId
,       p.PromotionCode
,       p.PromotionName
,       p.RegionCode
,       p.StartDate
,       p.EndDate
,       p.DiscountTypeCode
,       p.DiscountValue
,       p.BudgetAmount
,       r.SaleLineId
,       r.InvoiceDate
,       r.CustomerId
,       r.StockItemId
,       r.RedeemedAmount
,       r.RedemptionChannelCode
        /* Attribution window differs by region: NA gives credit for 30 days
           after the promotion ends, EU only within the promotion window
           because of consumer-protection rules, APAC for 14 days. */
,       CASE p.RegionCode
            WHEN 'NA'   THEN DATEADD(DAY, 30, p.EndDate)
            WHEN 'APAC' THEN DATEADD(DAY, 14, p.EndDate)
            ELSE p.EndDate
        END                                             AS AttributionEndDate
FROM    stg.Promotion AS p
INNER JOIN stg.PromotionRedemption AS r
        ON  r.PromotionId = p.PromotionId
WHERE   p.LoadBatchId = ?
""".strip()


def build_sls_load_promotionredemption():
    pkg = new_package(
        "SLS_Load_PromotionRedemption",
        "Attributes promotion redemptions to promotions and refreshes the promotion effectiveness "
        "aggregate. Redemptions outside the region's attribution window are counted as spill and "
        "reported separately so marketing cannot claim them, and over-budget promotions are "
        "flagged for the commercial review.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("SpillRedemptionCount", 0, "int"), ("OverBudgetPromotionCount", 0, "int")],
    )
    pkg.add_parameter("AttributionMode", "REGIONAL", dtype="string",
                      description="REGIONAL uses the per-region window; STRICT uses the promotion window only.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.PromotionRedemption"))

    columns = [
        int_col("PromotionId"), str_col("PromotionCode", 20), str_col("PromotionName", 100),
        str_col("RegionCode", 4), date_col("StartDate"), date_col("EndDate"),
        str_col("DiscountTypeCode", 10), money_col("DiscountValue"), money_col("BudgetAmount"),
        int_col("SaleLineId"), date_col("InvoiceDate"), int_col("CustomerId"), int_col("StockItemId"),
        money_col("RedeemedAmount"), str_col("RedemptionChannelCode", 10),
        date_col("AttributionEndDate"),
    ]
    flow = DataFlow("Attribute Redemptions")
    flow.oledb_source("stg Promotion Redemptions", CONN_STAGING, PROMOTION_SQL, columns, timeout=1200)
    flow.derived_column("Classify Attribution", [
        ("IsInWindow",
         'InvoiceDate >= StartDate && InvoiceDate <= (@[$Package::AttributionMode] == "STRICT" ? '
         'EndDate : AttributionEndDate) ? (DT_BOOL)1 : (DT_BOOL)0',
         bool_col("IsInWindow")),
        ("DiscountCostAmount",
         'DiscountTypeCode == "PCT" ? RedeemedAmount * DiscountValue / 100 : DiscountValue',
         money_col("DiscountCostAmount")),
    ])
    flow.conditional_split("Split Spill", [
        ("Attributed", "IsInWindow == (DT_BOOL)1"),
        ("Spill", "IsInWindow == (DT_BOOL)0"),
    ])
    flow.aggregate("Summarise Promotion", ["PromotionId", "PromotionCode", "RegionCode"],
                   [("RedeemedAmount", "RedeemedAmount", "SUM"),
                    ("DiscountCostAmount", "DiscountCostAmount", "SUM"),
                    ("SaleLineId", "RedemptionCount", "COUNT"),
                    ("CustomerId", "RedeemingCustomerCount", "COUNTDISTINCT")])
    flow.row_count("Count Promotion Rows", "User::RowsInserted")
    flow.oledb_destination("Aggregate Promotion Effectiveness", CONN_DW,
                           "[Aggregate].[Promotion Effectiveness]", batch_size=10000)
    flow.branch_destination("work PromotionSpill", CONN_STAGING, "[work].[PromotionSpill]",
                            "Split Spill", "Spill")
    attribute = pkg.add(DataFlowTask(flow))

    budget = pkg.add(ExecuteSql(
        "Flag Over Budget Promotions",
        CONN_DW,
        "UPDATE pe SET [Budget Status] = N'OVER' "
        "FROM Aggregate.[Promotion Effectiveness] AS pe "
        "INNER JOIN (SELECT PromotionId, BudgetAmount FROM stg.Promotion) AS b "
        "        ON b.PromotionId = pe.[Promotion ID] "
        "WHERE pe.[Discount Cost Amount] > b.BudgetAmount;",
    ))
    spill = pkg.add(ExecuteSql(
        "Count Spill Redemptions",
        CONN_STAGING,
        "SELECT COUNT(*) AS SpillRedemptions FROM work.PromotionSpill;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::SpillRedemptionCount")],
    ))
    counts = pkg.add(log_row_count("Aggregate.Promotion Effectiveness"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, attribute)
    pkg.link(attribute, budget)
    pkg.link(budget, spill, value="Completion")
    pkg.link(spill, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# SLS_Export_PartnerFeed
# ---------------------------------------------------------------------------


def build_sls_export_partnerfeed():
    pkg = new_package(
        "SLS_Export_PartnerFeed",
        "Builds the outbound partner sales feed. Partners receive only the lines for their own "
        "accounts, EU partner rows are stripped of any personal data the customer has not "
        "consented to share, and amounts are restated into the partner's settlement currency. The "
        "feed is written to the outbound share and a copy is kept for the audit trail.",
        source_system="WWIOLTP",
        connections=(CONN_DW, CONN_FILES),
        extra_variables=[("PartnerCode", "", "string"), ("ExportRowCount", 0, "int"),
                         ("OutboundFilePath", "partner_feed.csv", "string"),
                         ("ArchiveFilePath", "partner_feed.csv", "string")],
    )
    pkg.add_parameter("PartnerScope", "ALL", dtype="string",
                      description="ALL, or a single partner code for a targeted re-send.")
    pkg.add_parameter("SuppressUnconsentedEuRows", "True", dtype="bool",
                      description="EU rows without a sharing consent are dropped from the feed.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.PartnerFeedRow"))
    build = pkg.add(ExecuteSql(
        "Build Partner Feed Rows",
        CONN_DW,
        "INSERT INTO work.PartnerFeedRow\n"
        "    (PartnerCode, InvoiceNumber, InvoiceDate, CustomerReference, StockItemCode,\n"
        "     Quantity, NetAmount, SettlementCurrencyCode, RegionCode)\n"
        "SELECT  pa.[Partner Code]\n"
        ",       s.[WWI Invoice ID]\n"
        ",       s.[Invoice Date Key]\n"
        ",       CASE WHEN c.[Region Code] = N'EU' AND ISNULL(c.[Share Consent Flag], 0) = 0\n"
        "             THEN N'REDACTED' ELSE c.[Customer Reference] END\n"
        ",       si.[Stock Item Code]\n"
        ",       s.Quantity\n"
        ",       s.[Total Excluding Tax] * ISNULL(fx.ConversionRate, 1)\n"
        ",       pa.[Settlement Currency Code]\n"
        ",       c.[Region Code]\n"
        "FROM    Fact.Sale AS s\n"
        "INNER JOIN Dimension.Customer AS c ON c.[Customer Key] = s.[Customer Key]\n"
        "INNER JOIN Dimension.[Stock Item] AS si ON si.[Stock Item Key] = s.[Stock Item Key]\n"
        "INNER JOIN Dimension.Partner AS pa ON pa.[Partner Key] = c.[Partner Key]\n"
        "LEFT OUTER JOIN work.FxRevaluationRate AS fx\n"
        "        ON  fx.CurrencyCode = s.[Currency Code]\n"
        "        AND fx.QuoteCurrencyCode = pa.[Settlement Currency Code]\n"
        "        AND fx.RateTypeCode = N'AVERAGE'\n"
        "WHERE   (? = N'ALL' OR pa.[Partner Code] = ?)\n"
        "AND     s.[Invoice Date Key] >= DATEADD(DAY, -1, CAST(SYSDATETIME() AS date));",
        parameter_bindings=[
            ("$Package::PartnerScope", 0, "NVARCHAR"),
            ("$Package::PartnerScope", 1, "NVARCHAR"),
        ],
    ))
    suppress = pkg.add(ExecuteSql(
        "Suppress Unconsented EU Rows",
        CONN_STAGING,
        "DELETE FROM work.PartnerFeedRow "
        "WHERE RegionCode = N'EU' AND CustomerReference = N'REDACTED';",
    ))
    count_rows = pkg.add(ExecuteSql(
        "Count Feed Rows",
        CONN_STAGING,
        "SELECT COUNT(*) AS FeedRows FROM work.PartnerFeedRow;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::ExportRowCount")],
    ))
    name_file = pkg.add(Expression(
        "Build Outbound File Name",
        '@[User::OutboundFilePath] = "partner_feed_" + '
        '(DT_WSTR,4)YEAR(GETDATE()) + RIGHT("0" + (DT_WSTR,2)MONTH(GETDATE()), 2) + '
        'RIGHT("0" + (DT_WSTR,2)DAY(GETDATE()), 2) + ".csv"',
    ))

    columns = [
        str_col("PartnerCode", 10), str_col("InvoiceNumber", 30), date_col("InvoiceDate"),
        str_col("CustomerReference", 40), str_col("StockItemCode", 20), int_col("Quantity"),
        money_col("NetAmount"), str_col("SettlementCurrencyCode", 3), str_col("RegionCode", 4),
    ]
    flow = DataFlow("Write Partner Feed")
    flow.oledb_source(
        "work PartnerFeedRow", CONN_STAGING,
        "SELECT PartnerCode, InvoiceNumber, InvoiceDate, CustomerReference, StockItemCode, "
        "Quantity, NetAmount, SettlementCurrencyCode, RegionCode FROM work.PartnerFeedRow "
        "ORDER BY PartnerCode, InvoiceNumber;", columns, timeout=900)
    flow.row_count("Count Exported Rows", "User::RowsRead")
    flow.oledb_destination("work PartnerFeedArchive", CONN_STAGING, "[work].[PartnerFeedArchive]",
                           batch_size=20000)
    export = pkg.add(DataFlowTask(flow))

    counts = pkg.add(log_row_count("file:partner_feed.csv"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, build)
    pkg.link(build, suppress, expression="@[$Package::SuppressUnconsentedEuRows]")
    pkg.link(build, count_rows, expression="!@[$Package::SuppressUnconsentedEuRows]")
    pkg.link(suppress, count_rows)
    pkg.link(count_rows, name_file, expression="@[User::ExportRowCount] > 0")
    pkg.link(name_file, export)
    pkg.link(export, counts)
    pkg.link(count_rows, counts, expression="@[User::ExportRowCount] == 0")
    pkg.link(counts, done)
    return pkg


BUILDERS = [
    build_sls_na_load_commission,
    build_sls_eu_load_commission,
    build_sls_apac_load_commission,
    build_sls_load_quotaattainment,
    build_sls_load_promotionredemption,
    build_sls_export_partnerfeed,
]


def main():
    written = []
    names = []
    for builder in BUILDERS:
        pkg = builder()
        written.append(pkg.write(os.path.join(HERE, pkg.name + ".dtsx")))
        names.append(pkg.name)
    written.extend(project.write_project(HERE, PROJECT_NAME, names, CONNECTIONS))
    for path in written:
        print(os.path.relpath(path, REPO_ROOT))


if __name__ == "__main__":
    main()
