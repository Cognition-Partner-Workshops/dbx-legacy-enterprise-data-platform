"""Emit the WWI_Customer360 packages (ssis/14_customer_360).

Customer 360 is the newest mart in the estate and the one with the most
accumulated exceptions: identity resolution is a hand-rolled match-key cascade,
households are assembled from a standardised address that differs per region,
and the EU rows are subject to consent and retention rules that NA and APAC do
not apply.

The mart is also one half of a data-flow loop: the build packages write
Aggregate.Customer 360 and C360_Publish_Segments reads it and writes
Dimension.Customer Segment, which the aggregate refresh reads again. The loop
used to be broken only by the two halves landing in different scheduling
windows. The resolution is in the orchestration rather than in these packages:
Master_Daily_ETL, Master_Customer_Sync and Master_Month_End each run the
builders in one sequence container and C360_Publish_Segments in a second
container behind a Success precedence constraint, so the consume half is
declared to run after the produce half instead of by accident of the schedule.
Nothing in the packages themselves changed - the split is a container boundary,
not a rewrite of the mart.

Run:  python3 ssis/14_customer_360/build_customer360_packages.py
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import project  # noqa: E402
from patterns import (CONN_DW, CONN_STAGING, exec_proc, log_package_start,  # noqa: E402
                      log_package_success, log_row_count, new_package, truncate)
from ssisgen import (Column, DataFlow, DataFlowTask, ExecuteSql, Expression,  # noqa: E402
                     date_col, int_col, money_col, str_col)

PROJECT_NAME = "WWI_Customer360"
CONNECTIONS = ["WWI_Staging_DB", "WWI_DW_Destination_DB", "WWI_Reject_Files"]


def bool_col(name):
    return Column(name, "bool")


# ---------------------------------------------------------------------------
# C360_Build_CustomerProfile
# ---------------------------------------------------------------------------

PROFILE_SQL = """
SELECT  c.[Customer Key]                                AS CustomerKey
,       c.[WWI Customer ID]                             AS CustomerId
,       c.Customer                                      AS CustomerName
,       c.Category                                      AS CustomerCategory
,       c.[Buying Group]                                AS BuyingGroup
,       c.[Postal Code]                                 AS PostalCode
,       c.[Region Code]                                 AS RegionCode
,       c.[Country Code]                                AS CountryCode
,       c.[Primary Contact Email]                       AS ContactEmail
,       c.[Primary Contact Phone]                       AS ContactPhone
,       c.[Marketing Consent Flag]                      AS MarketingConsentFlag
,       c.[Consent Captured Date]                       AS ConsentCapturedDate
,       s.FirstOrderDate
,       s.LastOrderDate
,       s.LifetimeOrderCount
,       s.LifetimeNetAmount
,       p.LastPaymentDate
,       p.OpenBalanceAmount
,       p.AveragePaymentDays
FROM    Dimension.Customer AS c
LEFT OUTER JOIN work.CustomerSalesSummary AS s
        ON  s.CustomerKey = c.[Customer Key]
LEFT OUTER JOIN work.CustomerPaymentSummary AS p
        ON  p.CustomerKey = c.[Customer Key]
WHERE   c.[Valid To] > SYSDATETIME()
AND     c.[WWI Customer ID] > 0
""".strip()


def build_c360_build_customerprofile():
    pkg = new_package(
        "C360_Build_CustomerProfile",
        "Builds the customer 360 profile from the customer dimension, the sales history and the "
        "payment history. Identity resolution runs a match-key cascade - exact tax registration, "
        "then normalised email, then name plus standardised postal code - and the survivorship "
        "rules keep the most recently updated non-null attribute from the winning record.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("ProfileCount", 0, "int"), ("DuplicateClusterCount", 0, "int")],
    )
    pkg.add_parameter("MatchThresholdScore", 85, dtype="int",
                      description="Fuzzy match score at or above which two customers are merged.")
    pkg.add_parameter("RebuildIdentityGraph", "False", dtype="bool",
                      description="Rebuilds the whole identity graph rather than the daily delta.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.CustomerProfile"))
    # Standardisation is regional: NA uses the USPS-style five plus four, EU
    # keeps the national format with a country prefix and APAC strips spaces
    # because half of the source systems supply them and half do not.
    standardise = pkg.add(ExecuteSql(
        "Standardise Addresses",
        CONN_STAGING,
        "UPDATE work.CustomerAddressStandardised\n"
        "SET    StandardPostalCode =\n"
        "       CASE RegionCode\n"
        "            WHEN N'NA'   THEN LEFT(REPLACE(REPLACE(RawPostalCode, N'-', N''), N' ', N''), 5)\n"
        "            WHEN N'EU'   THEN CountryCode + N'-' + UPPER(REPLACE(RawPostalCode, N' ', N''))\n"
        "            WHEN N'APAC' THEN UPPER(REPLACE(RawPostalCode, N' ', N''))\n"
        "            ELSE UPPER(RawPostalCode)\n"
        "       END\n"
        ",      StandardEmail = LOWER(LTRIM(RTRIM(RawEmail)))\n"
        ",      StandardisedAtUtc = SYSUTCDATETIME();",
    ))
    identity = pkg.add(ExecuteSql(
        "Resolve Identity Graph",
        CONN_STAGING,
        "MERGE work.CustomerIdentityGraph AS target\n"
        "USING (\n"
        "    SELECT  a.CustomerId\n"
        "    ,       MIN(b.CustomerId) AS SurvivingCustomerId\n"
        "    ,       MAX(CASE\n"
        "                WHEN a.TaxRegistrationNumber = b.TaxRegistrationNumber THEN 100\n"
        "                WHEN a.StandardEmail = b.StandardEmail                 THEN 95\n"
        "                WHEN a.CustomerNameNormalised = b.CustomerNameNormalised\n"
        "                     AND a.StandardPostalCode = b.StandardPostalCode   THEN 88\n"
        "                ELSE 0 END) AS MatchScore\n"
        "    FROM    work.CustomerAddressStandardised AS a\n"
        "    INNER JOIN work.CustomerAddressStandardised AS b\n"
        "            ON  b.CustomerId <= a.CustomerId\n"
        "            AND (a.TaxRegistrationNumber = b.TaxRegistrationNumber\n"
        "                 OR a.StandardEmail = b.StandardEmail\n"
        "                 OR (a.CustomerNameNormalised = b.CustomerNameNormalised\n"
        "                     AND a.StandardPostalCode = b.StandardPostalCode))\n"
        "    GROUP BY a.CustomerId\n"
        ") AS source\n"
        "    ON  target.CustomerId = source.CustomerId\n"
        "WHEN MATCHED AND source.MatchScore >= ? THEN\n"
        "    UPDATE SET target.SurvivingCustomerId = source.SurvivingCustomerId,\n"
        "               target.MatchScore = source.MatchScore,\n"
        "               target.ResolvedAtUtc = SYSUTCDATETIME()\n"
        "WHEN NOT MATCHED BY TARGET AND source.MatchScore >= ? THEN\n"
        "    INSERT (CustomerId, SurvivingCustomerId, MatchScore, ResolvedAtUtc)\n"
        "    VALUES (source.CustomerId, source.SurvivingCustomerId, source.MatchScore,\n"
        "            SYSUTCDATETIME());",
        parameter_bindings=[
            ("$Package::MatchThresholdScore", 0, "LONG"),
            ("$Package::MatchThresholdScore", 1, "LONG"),
        ],
    ))

    columns = [
        int_col("CustomerKey"), int_col("CustomerId"), str_col("CustomerName", 100),
        str_col("CustomerCategory", 50), str_col("BuyingGroup", 50), str_col("PostalCode", 12),
        str_col("RegionCode", 4), str_col("CountryCode", 2), str_col("ContactEmail", 256),
        str_col("ContactPhone", 20), bool_col("MarketingConsentFlag"), date_col("ConsentCapturedDate"),
        date_col("FirstOrderDate"), date_col("LastOrderDate"), int_col("LifetimeOrderCount"),
        money_col("LifetimeNetAmount"), date_col("LastPaymentDate"), money_col("OpenBalanceAmount"),
        money_col("AveragePaymentDays"),
    ]
    flow = DataFlow("Build Customer Profile")
    flow.oledb_source("Dimension Customer", CONN_DW, PROFILE_SQL, columns, timeout=1800)
    flow.lookup("Lookup Surviving Identity", CONN_STAGING,
                "SELECT CustomerId, SurvivingCustomerId, MatchScore FROM work.CustomerIdentityGraph;",
                ["CustomerId"], [int_col("SurvivingCustomerId"), int_col("MatchScore")],
                no_match="IG")
    flow.derived_column("Apply Regional Consent Rules", [
        # EU withholds contact details without an explicit consent; NA operates
        # opt-out so absent consent still allows contact; APAC needs a consent
        # captured within the last two years.
        ("IsContactable",
         'RegionCode == "EU" ? (ISNULL(MarketingConsentFlag) ? (DT_BOOL)0 : MarketingConsentFlag) : '
         '(RegionCode == "APAC" ? (ISNULL(ConsentCapturedDate) ? (DT_BOOL)0 : '
         '(DATEDIFF("Dy", ConsentCapturedDate, GETDATE()) <= 730 ? (DT_BOOL)1 : (DT_BOOL)0)) : '
         '(DT_BOOL)1)',
         bool_col("IsContactable")),
        ("MaskedContactEmail",
         'RegionCode == "EU" && (ISNULL(MarketingConsentFlag) || MarketingConsentFlag == (DT_BOOL)0) '
         '? (DT_WSTR,256)"WITHHELD" : ContactEmail',
         str_col("MaskedContactEmail", 256)),
    ])
    flow.derived_column("Derive Profile Attributes", [
        ("MasterCustomerId",
         "ISNULL(SurvivingCustomerId) ? CustomerId : SurvivingCustomerId",
         int_col("MasterCustomerId")),
        ("TenureDays", 'ISNULL(FirstOrderDate) ? 0 : DATEDIFF("Dy", FirstOrderDate, GETDATE())',
         int_col("TenureDays")),
        ("AverageOrderValue",
         "LifetimeOrderCount == 0 ? (DT_NUMERIC,18,2)0 : LifetimeNetAmount / LifetimeOrderCount",
         money_col("AverageOrderValue")),
    ])
    flow.row_count("Count Profiles", "User::RowsInserted")
    flow.oledb_destination("work CustomerProfile", CONN_STAGING, "[work].[CustomerProfile]",
                           batch_size=50000)
    build = pkg.add(DataFlowTask(flow))

    households = pkg.add(ExecuteSql(
        "Assign Households",
        CONN_STAGING,
        "UPDATE p\n"
        "SET    p.HouseholdKey = h.HouseholdKey\n"
        "FROM   work.CustomerProfile AS p\n"
        "INNER JOIN (SELECT DENSE_RANK() OVER (ORDER BY StandardPostalCode, HouseholdNameKey)\n"
        "                       AS HouseholdKey,\n"
        "                   CustomerId\n"
        "            FROM   work.CustomerAddressStandardised) AS h\n"
        "        ON h.CustomerId = p.CustomerId;",
    ))
    publish = pkg.add(exec_proc(
        "Publish Customer 360 Profile",
        "EXEC Integration.usp_PublishCustomer360Profile @BatchId = ?, @RowsUpdated = ? OUTPUT;",
        connection=CONN_DW,
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    clusters = pkg.add(ExecuteSql(
        "Count Duplicate Clusters",
        CONN_STAGING,
        "SELECT COUNT(*) AS DuplicateClusters FROM ("
        "    SELECT SurvivingCustomerId FROM work.CustomerIdentityGraph "
        "    GROUP BY SurvivingCustomerId HAVING COUNT(*) > 1) AS d;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::DuplicateClusterCount")],
    ))
    counts = pkg.add(log_row_count("Customer360.CustomerProfile"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, standardise)
    pkg.link(standardise, identity)
    pkg.link(identity, build)
    pkg.link(build, households)
    pkg.link(households, publish)
    pkg.link(publish, clusters, value="Completion")
    pkg.link(clusters, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# C360_Build_RollingMetrics
# ---------------------------------------------------------------------------


def build_c360_build_rollingmetrics():
    pkg = new_package(
        "C360_Build_RollingMetrics",
        "Recomputes the rolling twelve-month customer metrics: revenue, order frequency, average "
        "basket, returns rate, days since last order and the recency/frequency/monetary deciles. "
        "The window is anchored on the region's own period end - calendar month for NA and EU, the "
        "4-4-5 period for APAC - so the same customer can appear in two different windows if they "
        "trade in more than one region.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("MetricRowCount", 0, "int"), ("InactiveCustomerCount", 0, "int")],
    )
    pkg.add_parameter("WindowMonths", 12, dtype="int",
                      description="Length of the rolling window in months.")
    pkg.add_parameter("InactiveThresholdDays", 270, dtype="int",
                      description="Days without an order before a customer is treated as inactive.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.CustomerRollingMetric"))
    build = pkg.add(ExecuteSql(
        "Aggregate Rolling Window",
        CONN_DW,
        "INSERT INTO work.CustomerRollingMetric\n"
        "    (CustomerKey, RegionCode, WindowStartDate, WindowEndDate, OrderCount, NetRevenue,\n"
        "     ReturnAmount, DistinctItemCount, LastOrderDate)\n"
        "SELECT  s.[Customer Key]\n"
        ",       c.[Region Code]\n"
        ",       DATEADD(MONTH, -?, CAST(SYSDATETIME() AS date))\n"
        ",       CAST(SYSDATETIME() AS date)\n"
        ",       COUNT(DISTINCT s.[WWI Invoice ID])\n"
        ",       SUM(s.[Total Excluding Tax])\n"
        ",       SUM(CASE WHEN s.Quantity < 0 THEN ABS(s.[Total Excluding Tax]) ELSE 0 END)\n"
        ",       COUNT(DISTINCT s.[Stock Item Key])\n"
        ",       MAX(s.[Invoice Date Key])\n"
        "FROM    Fact.Sale AS s\n"
        "INNER JOIN Dimension.Customer AS c ON c.[Customer Key] = s.[Customer Key]\n"
        "WHERE   s.[Invoice Date Key] >= DATEADD(MONTH, -?, CAST(SYSDATETIME() AS date))\n"
        "GROUP BY s.[Customer Key], c.[Region Code];",
        parameter_bindings=[
            ("$Package::WindowMonths", 0, "LONG"),
            ("$Package::WindowMonths", 1, "LONG"),
        ],
    ))
    apac_window = pkg.add(ExecuteSql(
        "Realign APAC Window To 445",
        CONN_STAGING,
        "UPDATE m "
        "SET    m.WindowEndDate = f.PeriodEndDate, m.WindowStartDate = f.PeriodStartDate "
        "FROM   work.CustomerRollingMetric AS m "
        "INNER JOIN (SELECT TOP (1) PeriodStartDate, PeriodEndDate FROM stg.FiscalCalendar445Period "
        "            WHERE PeriodEndDate <= CAST(SYSDATETIME() AS date) "
        "            ORDER BY PeriodEndDate DESC) AS f ON 1 = 1 "
        "WHERE  m.RegionCode = N'APAC';",
    ))

    columns = [
        int_col("CustomerKey"), str_col("RegionCode", 4), date_col("WindowStartDate"),
        date_col("WindowEndDate"), int_col("OrderCount"), money_col("NetRevenue"),
        money_col("ReturnAmount"), int_col("DistinctItemCount"), date_col("LastOrderDate"),
    ]
    flow = DataFlow("Derive Rolling Metrics")
    flow.oledb_source(
        "work CustomerRollingMetric", CONN_STAGING,
        "SELECT CustomerKey, RegionCode, WindowStartDate, WindowEndDate, OrderCount, NetRevenue, "
        "ReturnAmount, DistinctItemCount, LastOrderDate FROM work.CustomerRollingMetric;",
        columns, timeout=1800)
    flow.derived_column("Derive Ratios", [
        ("AverageBasketAmount",
         "OrderCount == 0 ? (DT_NUMERIC,18,2)0 : NetRevenue / OrderCount",
         money_col("AverageBasketAmount")),
        ("ReturnRatePercent",
         "NetRevenue == 0 ? (DT_NUMERIC,18,2)0 : ReturnAmount * 100 / NetRevenue",
         money_col("ReturnRatePercent")),
        ("DaysSinceLastOrder",
         'ISNULL(LastOrderDate) ? 9999 : DATEDIFF("Dy", LastOrderDate, WindowEndDate)',
         int_col("DaysSinceLastOrder")),
    ])
    flow.derived_column("Derive Activity Status", [
        ("ActivityStatusCode",
         '(ISNULL(LastOrderDate) ? 9999 : DATEDIFF("Dy", LastOrderDate, WindowEndDate)) '
         '> @[$Package::InactiveThresholdDays] ? (DT_WSTR,10)"INACTIVE" : '
         '(OrderCount >= 12 ? (DT_WSTR,10)"FREQUENT" : (DT_WSTR,10)"ACTIVE")',
         str_col("ActivityStatusCode", 10)),
        ("OrdersPerMonth",
         "OrderCount / (DT_NUMERIC,18,2)@[$Package::WindowMonths]", money_col("OrdersPerMonth")),
    ])
    flow.row_count("Count Metric Rows", "User::RowsInserted")
    flow.oledb_destination("Customer360 RollingMetric", CONN_DW,
                           "[Customer360].[CustomerRollingMetric]", batch_size=50000)
    derive = pkg.add(DataFlowTask(flow))

    deciles = pkg.add(ExecuteSql(
        "Assign RFM Deciles",
        CONN_DW,
        "UPDATE m\n"
        "SET    m.[Recency Decile] = d.RecencyDecile\n"
        ",      m.[Frequency Decile] = d.FrequencyDecile\n"
        ",      m.[Monetary Decile] = d.MonetaryDecile\n"
        "FROM   Customer360.CustomerRollingMetric AS m\n"
        "INNER JOIN (SELECT [Customer Key],\n"
        "                   NTILE(10) OVER (PARTITION BY [Region Code]\n"
        "                                   ORDER BY [Days Since Last Order] DESC) AS RecencyDecile,\n"
        "                   NTILE(10) OVER (PARTITION BY [Region Code]\n"
        "                                   ORDER BY [Order Count]) AS FrequencyDecile,\n"
        "                   NTILE(10) OVER (PARTITION BY [Region Code]\n"
        "                                   ORDER BY [Net Revenue]) AS MonetaryDecile\n"
        "            FROM   Customer360.CustomerRollingMetric) AS d\n"
        "        ON d.[Customer Key] = m.[Customer Key];",
    ))
    inactive = pkg.add(ExecuteSql(
        "Count Inactive Customers",
        CONN_DW,
        "SELECT COUNT(*) AS InactiveCustomers FROM Customer360.CustomerRollingMetric "
        "WHERE [Activity Status Code] = N'INACTIVE';",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::InactiveCustomerCount")],
    ))
    counts = pkg.add(log_row_count("Customer360.CustomerRollingMetric"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, build)
    pkg.link(build, apac_window)
    pkg.link(apac_window, derive)
    pkg.link(derive, deciles)
    pkg.link(deciles, inactive, value="Completion")
    pkg.link(inactive, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# C360_Build_LoyaltyOverlay
# ---------------------------------------------------------------------------


def build_c360_build_loyaltyoverlay():
    pkg = new_package(
        "C360_Build_LoyaltyOverlay",
        "Overlays the loyalty programme onto the customer profile. Points are accrued on the "
        "region's own qualifying amount, three tier ladders are maintained because each region "
        "launched its own scheme, and points older than the regional expiry are written off before "
        "the balance is published.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("TierChangeCount", 0, "int"), ("ExpiredPointsAmount", 0, "decimal")],
    )
    pkg.add_parameter("PointsExpiryMonthsNa", 24, dtype="int",
                      description="NA points expiry window in months.")
    pkg.add_parameter("PointsExpiryMonthsEu", 36, dtype="int",
                      description="EU points expiry window in months.")
    pkg.add_parameter("PointsExpiryMonthsApac", 18, dtype="int",
                      description="APAC points expiry window in months.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.LoyaltyOverlay"))
    expire = pkg.add(ExecuteSql(
        "Expire Aged Points",
        CONN_STAGING,
        "UPDATE work.LoyaltyPointLedger\n"
        "SET    PointStatusCode = N'EXPIRED', ExpiredAtUtc = SYSUTCDATETIME()\n"
        "WHERE  PointStatusCode = N'ACTIVE'\n"
        "AND    ((RegionCode = N'NA'   AND AccruedDate < DATEADD(MONTH, -?, CAST(SYSDATETIME() AS date)))\n"
        "     OR (RegionCode = N'EU'   AND AccruedDate < DATEADD(MONTH, -?, CAST(SYSDATETIME() AS date)))\n"
        "     OR (RegionCode = N'APAC' AND AccruedDate < DATEADD(MONTH, -?, CAST(SYSDATETIME() AS date))));",
        parameter_bindings=[
            ("$Package::PointsExpiryMonthsNa", 0, "LONG"),
            ("$Package::PointsExpiryMonthsEu", 1, "LONG"),
            ("$Package::PointsExpiryMonthsApac", 2, "LONG"),
        ],
    ))
    accrue = pkg.add(ExecuteSql(
        "Accrue Points",
        CONN_STAGING,
        "INSERT INTO work.LoyaltyPointLedger\n"
        "    (CustomerId, RegionCode, AccruedDate, QualifyingAmount, PointsAccrued,\n"
        "     PointStatusCode, SourceReference)\n"
        "SELECT  m.CustomerId\n"
        ",       m.RegionCode\n"
        ",       m.InvoiceDate\n"
        # NA accrues on the gross amount, EU on the VAT-exclusive amount and
        # APAC on the GST-exclusive amount at a different points rate.
        ",       CASE m.RegionCode\n"
        "            WHEN N'NA'   THEN m.GrossAmount\n"
        "            WHEN N'EU'   THEN m.NetAmount\n"
        "            WHEN N'APAC' THEN m.GrossAmount - ISNULL(m.GstAmount, 0)\n"
        "            ELSE m.NetAmount END\n"
        ",       CASE m.RegionCode\n"
        "            WHEN N'NA'   THEN FLOOR(m.GrossAmount)\n"
        "            WHEN N'EU'   THEN FLOOR(m.NetAmount * 1.5)\n"
        "            WHEN N'APAC' THEN FLOOR((m.GrossAmount - ISNULL(m.GstAmount, 0)) * 2)\n"
        "            ELSE FLOOR(m.NetAmount) END\n"
        ",       N'ACTIVE'\n"
        ",       m.InvoiceNumber\n"
        "FROM    work.LoyaltyQualifyingSale AS m\n"
        "WHERE   NOT EXISTS (SELECT 1 FROM work.LoyaltyPointLedger AS l\n"
        "                    WHERE l.SourceReference = m.InvoiceNumber);",
    ))
    tiers = pkg.add(ExecuteSql(
        "Recalculate Tier Ladders",
        CONN_STAGING,
        "INSERT INTO work.LoyaltyOverlay\n"
        "    (CustomerId, RegionCode, ActivePoints, ExpiredPoints, PreviousTierCode, TierCode)\n"
        "SELECT  l.CustomerId\n"
        ",       l.RegionCode\n"
        ",       SUM(CASE WHEN l.PointStatusCode = N'ACTIVE' THEN l.PointsAccrued ELSE 0 END)\n"
        ",       SUM(CASE WHEN l.PointStatusCode = N'EXPIRED' THEN l.PointsAccrued ELSE 0 END)\n"
        ",       MAX(ISNULL(cur.TierCode, N'NONE'))\n"
        # Three tier ladders with three different thresholds - the schemes were
        # launched independently and marketing has never harmonised them.
        ",       CASE l.RegionCode\n"
        "            WHEN N'NA' THEN\n"
        "                CASE WHEN SUM(CASE WHEN l.PointStatusCode = N'ACTIVE'\n"
        "                                   THEN l.PointsAccrued ELSE 0 END) >= 50000 THEN N'PLATINUM'\n"
        "                     WHEN SUM(CASE WHEN l.PointStatusCode = N'ACTIVE'\n"
        "                                   THEN l.PointsAccrued ELSE 0 END) >= 20000 THEN N'GOLD'\n"
        "                     WHEN SUM(CASE WHEN l.PointStatusCode = N'ACTIVE'\n"
        "                                   THEN l.PointsAccrued ELSE 0 END) >= 5000  THEN N'SILVER'\n"
        "                     ELSE N'BASE' END\n"
        "            WHEN N'EU' THEN\n"
        "                CASE WHEN SUM(CASE WHEN l.PointStatusCode = N'ACTIVE'\n"
        "                                   THEN l.PointsAccrued ELSE 0 END) >= 75000 THEN N'PREMIER'\n"
        "                     WHEN SUM(CASE WHEN l.PointStatusCode = N'ACTIVE'\n"
        "                                   THEN l.PointsAccrued ELSE 0 END) >= 30000 THEN N'PLUS'\n"
        "                     ELSE N'STANDARD' END\n"
        "            ELSE\n"
        "                CASE WHEN SUM(CASE WHEN l.PointStatusCode = N'ACTIVE'\n"
        "                                   THEN l.PointsAccrued ELSE 0 END) >= 40000 THEN N'DIAMOND'\n"
        "                     WHEN SUM(CASE WHEN l.PointStatusCode = N'ACTIVE'\n"
        "                                   THEN l.PointsAccrued ELSE 0 END) >= 15000 THEN N'JADE'\n"
        "                     ELSE N'MEMBER' END\n"
        "        END\n"
        "FROM    work.LoyaltyPointLedger AS l\n"
        "LEFT OUTER JOIN work.LoyaltyOverlayCurrent AS cur ON cur.CustomerId = l.CustomerId\n"
        "GROUP BY l.CustomerId, l.RegionCode;",
    ))
    measure = pkg.add(ExecuteSql(
        "Measure Tier Movement",
        CONN_STAGING,
        "SELECT SUM(CASE WHEN TierCode <> PreviousTierCode THEN 1 ELSE 0 END) AS TierChanges, "
        "       ISNULL(SUM(ExpiredPoints), 0) AS ExpiredPoints FROM work.LoyaltyOverlay;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::TierChangeCount"), ("1", "User::ExpiredPointsAmount")],
    ))

    columns = [
        int_col("CustomerId"), str_col("RegionCode", 4), int_col("ActivePoints"),
        int_col("ExpiredPoints"), str_col("PreviousTierCode", 12), str_col("TierCode", 12),
    ]
    flow = DataFlow("Publish Loyalty Overlay")
    flow.oledb_source(
        "work LoyaltyOverlay", CONN_STAGING,
        "SELECT CustomerId, RegionCode, ActivePoints, ExpiredPoints, PreviousTierCode, TierCode "
        "FROM work.LoyaltyOverlay;", columns, timeout=900)
    flow.derived_column("Derive Tier Movement", [
        ("TierMovementCode",
         'TierCode == PreviousTierCode ? (DT_WSTR,8)"SAME" : '
         '(PreviousTierCode == "NONE" ? (DT_WSTR,8)"NEW" : (DT_WSTR,8)"CHANGED")',
         str_col("TierMovementCode", 8)),
    ])
    flow.row_count("Count Loyalty Rows", "User::RowsInserted")
    flow.oledb_destination("Customer360 LoyaltyOverlay", CONN_DW,
                           "[Customer360].[LoyaltyOverlay]", batch_size=50000)
    publish = pkg.add(DataFlowTask(flow))

    counts = pkg.add(log_row_count("Customer360.LoyaltyOverlay"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, expire)
    pkg.link(expire, accrue)
    pkg.link(accrue, tiers)
    pkg.link(tiers, measure)
    pkg.link(measure, publish)
    pkg.link(publish, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# C360_Build_ChurnFlags
# ---------------------------------------------------------------------------


def build_c360_build_churnflags():
    pkg = new_package(
        "C360_Build_ChurnFlags",
        "Applies the churn-risk rule set to the customer profile. The rules are the ones the "
        "commercial team wrote in a spreadsheet and had implemented verbatim: order gap against "
        "the customer's own historic cadence, falling basket value, a rising returns rate, a "
        "lapsed loyalty tier and an unresolved credit hold. Each rule contributes points and the "
        "total is banded.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("HighRiskCount", 0, "int"), ("RuleSetVersion", "2019.3", "string")],
    )
    pkg.add_parameter("HighRiskScoreThreshold", 60, dtype="int",
                      description="Total rule score at or above which a customer is high risk.")
    pkg.add_parameter("OrderGapMultiplier", 2, dtype="int",
                      description="Multiple of the customer's own average order gap that signals risk.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.CustomerChurnFlag"))

    columns = [
        int_col("CustomerKey"), int_col("CustomerId"), str_col("RegionCode", 4),
        int_col("DaysSinceLastOrder"), money_col("AverageOrderGapDays"),
        money_col("AverageBasketAmount"), money_col("PriorAverageBasketAmount"),
        money_col("ReturnRatePercent"), str_col("TierCode", 12), str_col("PreviousTierCode", 12),
        bool_col("IsOnCreditHold"),
    ]
    flow = DataFlow("Score Churn Risk")
    flow.oledb_source(
        "work ChurnFeatureSet", CONN_STAGING,
        "SELECT  f.CustomerKey, f.CustomerId, f.RegionCode, f.DaysSinceLastOrder,\n"
        "        f.AverageOrderGapDays, f.AverageBasketAmount, f.PriorAverageBasketAmount,\n"
        "        f.ReturnRatePercent, ISNULL(l.TierCode, N'NONE') AS TierCode,\n"
        "        ISNULL(l.PreviousTierCode, N'NONE') AS PreviousTierCode,\n"
        "        ISNULL(f.IsOnCreditHold, 0) AS IsOnCreditHold\n"
        "FROM    work.ChurnFeatureSet AS f\n"
        "LEFT OUTER JOIN work.LoyaltyOverlay AS l ON l.CustomerId = f.CustomerId;",
        columns, timeout=1200)
    flow.derived_column("Apply Churn Rules", [
        ("RuleOrderGapScore",
         "AverageOrderGapDays > 0 && DaysSinceLastOrder > "
         "AverageOrderGapDays * @[$Package::OrderGapMultiplier] ? 30 : 0",
         int_col("RuleOrderGapScore")),
        ("RuleBasketDeclineScore",
         "PriorAverageBasketAmount > 0 && AverageBasketAmount < PriorAverageBasketAmount * 0.7 "
         "? 20 : 0", int_col("RuleBasketDeclineScore")),
        ("RuleReturnsScore", "ReturnRatePercent > 15 ? 15 : 0", int_col("RuleReturnsScore")),
        ("RuleTierLapseScore",
         'TierCode != PreviousTierCode && PreviousTierCode != "NONE" ? 15 : 0',
         int_col("RuleTierLapseScore")),
        ("RuleCreditHoldScore", "IsOnCreditHold ? 25 : 0", int_col("RuleCreditHoldScore")),
    ])
    flow.derived_column("Band Churn Risk", [
        ("ChurnScore",
         "RuleOrderGapScore + RuleBasketDeclineScore + RuleReturnsScore + RuleTierLapseScore "
         "+ RuleCreditHoldScore", int_col("ChurnScore")),
        ("ChurnRiskBandCode",
         "(RuleOrderGapScore + RuleBasketDeclineScore + RuleReturnsScore + RuleTierLapseScore "
         "+ RuleCreditHoldScore) >= @[$Package::HighRiskScoreThreshold] ? (DT_WSTR,8)\"HIGH\" : "
         "((RuleOrderGapScore + RuleBasketDeclineScore + RuleReturnsScore + RuleTierLapseScore "
         "+ RuleCreditHoldScore) >= 30 ? (DT_WSTR,8)\"MEDIUM\" : (DT_WSTR,8)\"LOW\")",
         str_col("ChurnRiskBandCode", 8)),
        ("RuleSetVersion", '(DT_WSTR,10)"2019.3"', str_col("RuleSetVersion", 10)),
    ])
    flow.conditional_split("Split Risk Bands", [
        ("HighRisk", 'ChurnRiskBandCode == "HIGH"'),
        ("Other", 'ChurnRiskBandCode != "HIGH"'),
    ])
    flow.multicast("Fan Out High Risk", ["Flag Output", "Work Output"])
    flow.row_count("Count Churn Rows", "User::RowsInserted")
    flow.oledb_destination("Customer360 ChurnFlag", CONN_DW, "[Customer360].[CustomerChurnFlag]",
                           batch_size=50000)
    flow.branch_destination("work ChurnHighRisk", CONN_STAGING, "[work].[CustomerChurnHighRisk]",
                            "Fan Out High Risk", "Work Output")
    score = pkg.add(DataFlowTask(flow))

    high = pkg.add(ExecuteSql(
        "Count High Risk Customers",
        CONN_STAGING,
        "SELECT COUNT(*) AS HighRisk FROM work.CustomerChurnHighRisk;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::HighRiskCount")],
    ))
    queue = pkg.add(ExecuteSql(
        "Queue High Risk For Outreach",
        CONN_STAGING,
        "INSERT INTO work.CustomerOutreachQueue "
        "    (CustomerId, RegionCode, QueueReasonCode, QueuedAtUtc) "
        "SELECT h.CustomerId, h.RegionCode, N'CHURN_HIGH', SYSUTCDATETIME() "
        "FROM   work.CustomerChurnHighRisk AS h "
        "WHERE  NOT EXISTS (SELECT 1 FROM work.CustomerOutreachQueue AS q "
        "                   WHERE q.CustomerId = h.CustomerId "
        "                     AND q.QueueReasonCode = N'CHURN_HIGH');",
    ))
    counts = pkg.add(log_row_count("Customer360.CustomerChurnFlag"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, score)
    pkg.link(score, high)
    pkg.link(high, queue, expression="@[User::HighRiskCount] > 0")
    pkg.link(high, counts, expression="@[User::HighRiskCount] == 0")
    pkg.link(queue, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# C360_Publish_Segments
# ---------------------------------------------------------------------------


def build_c360_publish_segments():
    pkg = new_package(
        "C360_Publish_Segments",
        "Publishes the derived customer segments used by marketing and the account teams. Segment "
        "assignment combines the RFM deciles, the loyalty tier and the churn band; EU customers "
        "without marketing consent are published into a suppressed segment so downstream systems "
        "cannot target them, and the previous assignment is retained so segment migration can be "
        "reported.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("SegmentCount", 0, "int"), ("SuppressedCount", 0, "int"),
                         ("MigratedCount", 0, "int")],
    )
    pkg.add_parameter("SegmentModelVersion", "SEG-2021A", dtype="string",
                      description="Segment model version stamped onto every published row.")
    pkg.add_parameter("PublishSuppressedRows", "True", dtype="bool",
                      description="Publish suppressed EU rows as a segment rather than dropping them.")

    start = pkg.add(log_package_start(pkg))
    snapshot = pkg.add(ExecuteSql(
        "Snapshot Previous Segments",
        CONN_STAGING,
        "TRUNCATE TABLE work.CustomerSegmentPrevious; "
        "INSERT INTO work.CustomerSegmentPrevious (CustomerId, SegmentCode, AssignedAtUtc) "
        "SELECT CustomerId, SegmentCode, AssignedAtUtc FROM work.CustomerSegment;",
    ))
    clear = pkg.add(truncate("work.CustomerSegment"))
    assign = pkg.add(ExecuteSql(
        "Assign Segments",
        CONN_DW,
        "INSERT INTO work.CustomerSegment\n"
        "    (CustomerId, RegionCode, SegmentCode, SegmentModelVersion, IsSuppressed, AssignedAtUtc)\n"
        "SELECT  p.[WWI Customer ID]\n"
        ",       p.[Region Code]\n"
        ",       CASE\n"
        "            WHEN p.[Region Code] = N'EU' AND ISNULL(p.[Is Contactable], 0) = 0\n"
        "                THEN N'SUPPRESSED'\n"
        "            WHEN ch.[Churn Risk Band Code] = N'HIGH' AND m.[Monetary Decile] >= 8\n"
        "                THEN N'AT_RISK_HIGH_VALUE'\n"
        "            WHEN ch.[Churn Risk Band Code] = N'HIGH'\n"
        "                THEN N'AT_RISK'\n"
        "            WHEN m.[Monetary Decile] >= 9 AND m.[Frequency Decile] >= 8\n"
        "                THEN N'CHAMPION'\n"
        "            WHEN m.[Recency Decile] >= 8 AND m.[Frequency Decile] <= 3\n"
        "                THEN N'NEW_PROMISING'\n"
        "            WHEN lo.[Tier Code] IN (N'PLATINUM', N'PREMIER', N'DIAMOND')\n"
        "                THEN N'LOYAL_PREMIUM'\n"
        "            WHEN m.[Activity Status Code] = N'INACTIVE'\n"
        "                THEN N'DORMANT'\n"
        "            ELSE N'CORE'\n"
        "        END\n"
        ",       ?\n"
        ",       CASE WHEN p.[Region Code] = N'EU' AND ISNULL(p.[Is Contactable], 0) = 0\n"
        "             THEN 1 ELSE 0 END\n"
        ",       SYSUTCDATETIME()\n"
        "FROM    Customer360.CustomerProfile AS p\n"
        "LEFT OUTER JOIN Customer360.CustomerRollingMetric AS m\n"
        "        ON m.[Customer Key] = p.[Customer Key]\n"
        "LEFT OUTER JOIN Customer360.CustomerChurnFlag AS ch\n"
        "        ON ch.[Customer Key] = p.[Customer Key]\n"
        "LEFT OUTER JOIN Customer360.LoyaltyOverlay AS lo\n"
        "        ON lo.[WWI Customer ID] = p.[WWI Customer ID];",
        parameter_bindings=[("$Package::SegmentModelVersion", 0, "NVARCHAR")],
    ))
    drop_suppressed = pkg.add(ExecuteSql(
        "Drop Suppressed Rows",
        CONN_STAGING,
        "DELETE FROM work.CustomerSegment WHERE IsSuppressed = 1;",
    ))
    migration = pkg.add(ExecuteSql(
        "Measure Segment Migration",
        CONN_STAGING,
        "SELECT (SELECT COUNT(*) FROM work.CustomerSegment) AS Segments, "
        "       (SELECT COUNT(*) FROM work.CustomerSegment WHERE IsSuppressed = 1) AS Suppressed, "
        "       (SELECT COUNT(*) FROM work.CustomerSegment AS c "
        "        INNER JOIN work.CustomerSegmentPrevious AS pr ON pr.CustomerId = c.CustomerId "
        "        WHERE pr.SegmentCode <> c.SegmentCode) AS Migrated;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::SegmentCount"), ("1", "User::SuppressedCount"),
                         ("2", "User::MigratedCount")],
    ))

    columns = [
        int_col("CustomerId"), str_col("RegionCode", 4), str_col("SegmentCode", 24),
        str_col("SegmentModelVersion", 12), bool_col("IsSuppressed"), date_col("AssignedAtUtc"),
    ]
    flow = DataFlow("Publish Segments")
    flow.oledb_source(
        "work CustomerSegment", CONN_STAGING,
        "SELECT CustomerId, RegionCode, SegmentCode, SegmentModelVersion, IsSuppressed, "
        "AssignedAtUtc FROM work.CustomerSegment;", columns, timeout=900)
    flow.lookup("Lookup Previous Segment", CONN_STAGING,
                "SELECT CustomerId, SegmentCode AS PreviousSegmentCode "
                "FROM work.CustomerSegmentPrevious;",
                ["CustomerId"], [str_col("PreviousSegmentCode", 24)], no_match="IG")
    flow.derived_column("Derive Segment Migration", [
        ("SegmentMovementCode",
         'ISNULL(PreviousSegmentCode) ? (DT_WSTR,8)"NEW" : '
         '(PreviousSegmentCode == SegmentCode ? (DT_WSTR,8)"SAME" : (DT_WSTR,8)"MOVED")',
         str_col("SegmentMovementCode", 8)),
    ])
    flow.row_count("Count Segment Rows", "User::RowsInserted")
    flow.oledb_destination("Customer360 CustomerSegment", CONN_DW,
                           "[Customer360].[CustomerSegment]", batch_size=50000)
    publish = pkg.add(DataFlowTask(flow))

    counts = pkg.add(log_row_count("Customer360.CustomerSegment"))
    done = pkg.add(log_package_success())

    pkg.link(start, snapshot)
    pkg.link(snapshot, clear)
    pkg.link(clear, assign)
    pkg.link(assign, migration, expression="@[$Package::PublishSuppressedRows]")
    pkg.link(assign, drop_suppressed, expression="!@[$Package::PublishSuppressedRows]")
    pkg.link(drop_suppressed, migration)
    pkg.link(migration, publish)
    pkg.link(publish, counts)
    pkg.link(counts, done)
    return pkg


BUILDERS = [
    build_c360_build_customerprofile,
    build_c360_build_rollingmetrics,
    build_c360_build_loyaltyoverlay,
    build_c360_build_churnflags,
    build_c360_publish_segments,
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
