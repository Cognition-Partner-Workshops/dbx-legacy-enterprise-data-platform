"""Regional tax rules: NA sales tax, EU VAT, APAC GST as one pure function.

Spec (docs/domain-model/business-domains.md, "Tax"):

    Tax regimes differ: NA sales/use tax, EU VAT, APAC GST. NA tax is on top
    and stacks state / county / city / district rates; EU VAT is on top with
    one rate per country and the reverse charge for registered cross-border
    customers; APAC GST is *included* in the price, with GST-free supplies.

    The three tax reference tables have three different shapes, so the loads
    cannot share a lookup. APAC's inclusive treatment means the APAC path
    derives the net amount by division and **truncates** where the other two
    round; that is a deliberate reproduction of the original behaviour, and
    it is why a systematic sub-cent residual shows up on APAC rows.

    Reverse charge is the EU path that has no equivalent elsewhere: the tax
    is not charged and the customer accounts for it, which requires the
    customer's VAT registration to have been captured. Where it was not, the
    row is still loaded.

Legacy sources reproduced:

- NA: ``Integration.usp_LoadFactSale`` step 8 ("NA: sales tax is state +
  county"), SSIS ``FACT_NA_Load_Sale`` "Calculate NA Measures" (tax on the
  discounted net), ``WWI_FIN.PKG_TAX.determine_tax`` (additive jurisdiction
  rows, ``ROUND(amt * rate / 100, 2)``).
- EU: SSIS ``FACT_EU_Load_Sale`` "Calculate EU Measures" (Article 138
  reverse charge -> zero rate), ``usp_LoadFactSale`` ("VAT is recomputed from
  the net so that rounding matches the statutory invoice"), ``PKG_TAX``
  (``VAT_RC`` regime, tax 0).
- APAC: SSIS ``FACT_APAC_Load_Sale`` ("GST-inclusive pricing: the tax is
  extracted from the gross rather than added to the net"), ``PKG_TAX``
  ("APAC prices are tax inclusive").

Rates are *fractions* (0.0825 == 8.25 %), as carried in the ``tax_rate``
column of ``ref_tax_rate_*``; the caller sums the NA jurisdiction components
(``ref_tax_rate_na.combined_tax_rate``) before calling.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DecimalType, StringType

from sales_lakehouse.silver.rules.dq import tagDq

MONEY = DecimalType(19, 4)
RATE = DecimalType(19, 8)

TREATMENT_SALES_TAX = "SALES_TAX"
TREATMENT_VAT = "VAT"
TREATMENT_REVERSE_CHARGE = "REVERSE_CHARGE"
TREATMENT_REVERSE_CHARGE_NO_VATREG = "REVERSE_CHARGE_NO_VATREG"
TREATMENT_GST_INCLUSIVE = "GST_INCLUSIVE"
TREATMENT_GST_EXCLUSIVE = "GST_EXCLUSIVE"
TREATMENT_UNMAPPED_REGION = "UNMAPPED_REGION"

OUTPUT_COLUMNS: tuple[str, ...] = (
    "net_amount_local",
    "tax_amount_local",
    "gross_amount_local",
    "tax_treatment_code",
    "tax_residual_local",
)


def roundHalfUpCents(amount: Column) -> Column:
    """T-SQL ``ROUND(x, 2)`` / Oracle ``ROUND(x, 2)``: half away from zero."""
    return F.round(amount, 2).cast(MONEY)


def truncateCents(amount: Column) -> Column:
    """T-SQL ``ROUND(x, 2, 1)``: drop everything below the cent, toward zero."""
    scaled = amount * F.lit(100).cast(DecimalType(3, 0))
    truncated = F.when(scaled < 0, F.ceil(scaled)).otherwise(F.floor(scaled))
    return (truncated / F.lit(100).cast(DecimalType(3, 0))).cast(MONEY)


def _optional(df: DataFrame, name: str | None, dataType) -> Column:
    if name is not None and name in df.columns:
        return F.col(name).cast(dataType)
    return F.lit(None).cast(dataType)


def applyTax(
    df: DataFrame,
    regionCol: str = "region_code",
    grossCol: str = "gross_amount",
    netCol: str = "net_amount",
    taxRateCol: str = "tax_rate",
    isReverseChargeCol: str = "is_reverse_charge",
    vatRegCol: str = "vat_registration_number",
) -> DataFrame:
    """Add ``net_amount_local``, ``tax_amount_local``, ``gross_amount_local``,
    ``tax_treatment_code`` and ``tax_residual_local`` (all ``decimal(19,4)``
    except the code).

    Input columns that the frame does not carry are treated as NULL, so a
    NA frame needs only the net and the rate, an APAC frame only the gross.

    - NA (tax-exclusive): ``net = coalesce(net, gross)``;
      ``tax = round_half_up(net * rate, 2)``; ``gross = net + tax``.
    - EU (VAT on top): where the source carries the net, tax is recomputed
      from it; where it only carries the gross, ``net = round(gross / (1 +
      rate), 2)`` and ``tax = gross - net``. A reverse-charged line has tax 0.
    - APAC (GST-inclusive): ``net = truncate(gross / (1 + rate), 2)``;
      ``tax = gross - net``; the truncated sub-cent amount is kept in
      ``tax_residual_local``. A line carrying only a net is treated as
      GST-exclusive like the SSIS ``IsPriceInclusive = false`` branch.
    """
    base = df.drop(*[c for c in OUTPUT_COLUMNS if c in df.columns])

    region = F.upper(F.trim(F.col(regionCol).cast(StringType())))
    gross = _optional(base, grossCol, MONEY)
    net = _optional(base, netCol, MONEY)
    rawRate = _optional(base, taxRateCol, RATE)
    # LEGACY QUIRK: every legacy path treats a missing rate as 0 % rather than
    # failing (SSIS "ISNULL(StandardVatRatePercent) ? 0", usp_LoadFactSale
    # "ISNULL([Tax Rate], 0)"). Reproduced; the row is tagged WARN.
    rate = F.coalesce(rawRate, F.lit(0).cast(RATE))
    isReverseCharge = F.coalesce(_optional(base, isReverseChargeCol, BooleanType()), F.lit(False))
    vatReg = F.trim(_optional(base, vatRegCol, StringType()))
    vatRegMissing = vatReg.isNull() | (vatReg == "")
    one = F.lit(1).cast(RATE)
    zero = F.lit(0).cast(MONEY)

    isNa = region == "NA"
    isEu = region == "EU"
    isApac = region == "APAC"

    # ---- NA: tax on top of the (discounted) net, components already summed
    naNet = F.coalesce(net, gross)
    naTax = roundHalfUpCents(naNet * rate)
    naGross = (naNet + naTax).cast(MONEY)

    # ---- EU: VAT on top; zero-rated when reverse charged
    euReverse = isEu & isReverseCharge
    euNetFromGross = roundHalfUpCents(gross / (one + rate))
    euNet = F.when(euReverse, F.coalesce(net, gross)).otherwise(F.coalesce(net, euNetFromGross))
    euTax = (
        F.when(euReverse, zero)
        .when(net.isNotNull(), roundHalfUpCents(net * rate))
        .otherwise((gross - euNetFromGross).cast(MONEY))
    )
    euGross = (euNet + euTax).cast(MONEY)

    # ---- APAC: GST backed out of the inclusive gross
    apacExclusive = gross.isNull() & net.isNotNull()
    apacQuotient = gross / (one + rate)
    # LEGACY QUIRK: truncation residual is deliberate. The APAC load truncates
    # gross / (1 + rate) to the cent where NA and EU round, so gross - net
    # over-states the tax by the dropped fraction (business-domains.md: "a
    # systematic sub-cent residual shows up on APAC rows").
    apacInclusiveNet = truncateCents(apacQuotient)
    apacResidual = (apacQuotient - apacInclusiveNet).cast(MONEY)
    apacExclusiveTax = roundHalfUpCents(net * rate)
    apacNet = F.when(apacExclusive, net).otherwise(apacInclusiveNet)
    apacTax = F.when(apacExclusive, apacExclusiveTax).otherwise((gross - apacInclusiveNet).cast(MONEY))
    apacGross = F.when(apacExclusive, (net + apacExclusiveTax).cast(MONEY)).otherwise(gross)

    # ---- unmapped region: pass the amounts through untouched, tag FAIL
    otherNet = F.coalesce(net, gross)

    netOut = F.when(isNa, naNet).when(isEu, euNet).when(isApac, apacNet).otherwise(otherNet).cast(MONEY)
    taxOut = F.when(isNa, naTax).when(isEu, euTax).when(isApac, apacTax).otherwise(zero).cast(MONEY)
    grossOut = F.when(isNa, naGross).when(isEu, euGross).when(isApac, apacGross).otherwise(otherNet).cast(MONEY)
    residualOut = F.when(isApac & ~apacExclusive, apacResidual).otherwise(zero).cast(MONEY)
    treatment = (
        F.when(isNa, F.lit(TREATMENT_SALES_TAX))
        .when(euReverse & vatRegMissing, F.lit(TREATMENT_REVERSE_CHARGE_NO_VATREG))
        .when(euReverse, F.lit(TREATMENT_REVERSE_CHARGE))
        .when(isEu, F.lit(TREATMENT_VAT))
        .when(isApac & apacExclusive, F.lit(TREATMENT_GST_EXCLUSIVE))
        .when(isApac, F.lit(TREATMENT_GST_INCLUSIVE))
        .otherwise(F.lit(TREATMENT_UNMAPPED_REGION))
    )

    out = (
        base.withColumn("net_amount_local", netOut)
        .withColumn("tax_amount_local", taxOut)
        .withColumn("gross_amount_local", grossOut)
        .withColumn("tax_treatment_code", treatment)
        .withColumn("tax_residual_local", residualOut)
    )
    # LEGACY QUIRK: a reverse-charged line whose customer VAT registration was
    # never captured is still loaded (business-domains.md: "Where it was not,
    # the row is still loaded"); it is only tagged.
    out = tagDq(out, F.col("tax_treatment_code") == TREATMENT_REVERSE_CHARGE_NO_VATREG, "WARN", "TAX_RC_NO_VATREG")
    out = tagDq(out, (isNa | isEu | isApac) & rawRate.isNull() & ~euReverse, "WARN", "TAX_RATE_MISSING")
    out = tagDq(out, F.col("tax_treatment_code") == TREATMENT_UNMAPPED_REGION, "FAIL", "TAX_REGION_UNMAPPED")
    return out
