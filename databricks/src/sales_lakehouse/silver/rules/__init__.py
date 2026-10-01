"""Region-parameterised, pure-function tax / FX / fiscal-calendar rules.

Public API (other workstreams code against these exact signatures):

- ``tax.applyTax``
- ``fx.applyFx``
- ``fiscal.resolveFiscalPeriod``, ``fiscal.regionCalendarCode``,
  ``fiscal.naFiscalPeriodFor``

The only I/O any function performs is reading a silver reference table via
``cfg.fqn("silver", ...)``.
"""
from sales_lakehouse.silver.rules.fiscal import naFiscalPeriodFor, regionCalendarCode, resolveFiscalPeriod
from sales_lakehouse.silver.rules.fx import applyFx
from sales_lakehouse.silver.rules.tax import applyTax

__all__ = ["applyFx", "applyTax", "naFiscalPeriodFor", "regionCalendarCode", "resolveFiscalPeriod"]
