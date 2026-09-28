"""Period lock: ``etl.period_lock`` plus the checks that stop a locked period
from being reloaded.

Legacy FIN_Close_PeriodLock flipped ``etl.Configuration`` rows
(``Finance.PeriodStatus.<LedgerCode>`` = 'Closed'); Master_Finance_Close read
``Finance.PeriodStatus`` before starting. Here the lock lives in a dedicated
Delta table (one row per ledger/period, status Locked/Unlocked) and the
configuration rows are still updated so session 00's gate keeps working.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dbx_etl_common import naming

LOCKED = "Locked"
UNLOCKED = "Unlocked"


class PeriodLockedError(RuntimeError):
    pass


def periodLockTable(catalog: str) -> str:
    return naming.table(catalog, "etl", "period_lock")


def lockedLedgers(spark, catalog: str, accountingPeriod: str, ledgerCodes: list[str] | None = None) -> list[str]:
    df = spark.table(periodLockTable(catalog)).where(
        (F.col("AccountingPeriod") == F.lit(accountingPeriod)) & (F.col("LockStatusCode") == F.lit(LOCKED))
    )
    if ledgerCodes:
        df = df.where(F.col("LedgerCode").isin(*ledgerCodes))
    return sorted(r[0] for r in df.select("LedgerCode").distinct().collect())


def lockedLedgersFrom(locks: DataFrame, accountingPeriod: str, ledgerCodes: list[str] | None = None) -> list[str]:
    """Pure variant of lockedLedgers for tests / already-loaded frames."""
    df = locks.where((F.col("AccountingPeriod") == F.lit(accountingPeriod)) & (F.col("LockStatusCode") == F.lit(LOCKED)))
    if ledgerCodes:
        df = df.where(F.col("LedgerCode").isin(*ledgerCodes))
    return sorted(r[0] for r in df.select("LedgerCode").distinct().collect())


def assertPeriodOpen(spark, catalog: str, accountingPeriod: str, packageName: str, ledgerCodes: list[str] | None = None) -> None:
    """Raise PeriodLockedError when any in-scope ledger is locked for the period.
    Every FIN_* load calls this before writing, so a locked period cannot be
    reloaded by re-running the job with an old BusinessDate/AccountingPeriod."""
    locked = lockedLedgers(spark, catalog, accountingPeriod, ledgerCodes)
    if locked:
        raise PeriodLockedError(
            f"{packageName}: accounting period {accountingPeriod} is locked for ledger(s) {', '.join(locked)}; "
            "reload refused. Unlock via etl.period_lock before re-running."
        )


def openPeriodsFromConfiguration(spark, catalog: str, environmentCode: str | None = None) -> DataFrame:
    """Finance.OpenPeriod.<LedgerCode> rows -> (LedgerCode, AccountingPeriod)."""
    cfg = spark.table(naming.table(catalog, "etl", "configuration")).where(F.col("ConfigurationKey").like("Finance.OpenPeriod.%"))
    if "EnvironmentCode" in cfg.columns:
        envs = ["ALL"] + ([environmentCode] if environmentCode else [])
        cfg = cfg.where(F.col("EnvironmentCode").isin(*envs))
    return cfg.select(
        F.regexp_replace(F.col("ConfigurationKey"), "^Finance\\.OpenPeriod\\.", "").alias("LedgerCode"),
        F.col("ConfigurationValue").alias("AccountingPeriod"),
    ).dropDuplicates(["LedgerCode"])


def knownVariancesFromConfiguration(spark, catalog: str) -> DataFrame:
    cfg = spark.table(naming.table(catalog, "etl", "configuration")).where(F.col("ConfigurationKey").like("Finance.KnownVariance.%"))
    return cfg.select(
        F.regexp_replace(F.col("ConfigurationKey"), "^Finance\\.KnownVariance\\.", "").alias("AccountCode"),
        F.col("ConfigurationValue").alias("ExplanationCode"),
    ).dropDuplicates(["AccountCode"])


def lockLedgers(spark, catalog: str, ledgerCodes: list[str], accountingPeriod: str, batchId: int, packageName: str, notes: str | None = None) -> int:
    """'Lock Ledgers' + 'Lock APAC 445 Period': upsert one Locked row per ledger
    and flip Finance.PeriodStatus.<LedgerCode> = 'Closed' like the legacy package."""
    if not ledgerCodes:
        return 0
    rows = [(l, accountingPeriod, LOCKED, int(batchId), packageName, notes) for l in ledgerCodes]
    src = spark.createDataFrame(
        rows, "LedgerCode STRING, AccountingPeriod STRING, LockStatusCode STRING, LockedByBatchId BIGINT, LockedByPackage STRING, Notes STRING"
    ).withColumn("LockedAtUtc", F.current_timestamp())
    src.createOrReplaceTempView("_fin_period_lock_src")
    spark.sql(
        f"""
        MERGE INTO {periodLockTable(catalog)} AS t
        USING _fin_period_lock_src AS s
          ON t.LedgerCode = s.LedgerCode AND t.AccountingPeriod = s.AccountingPeriod
        WHEN MATCHED THEN UPDATE SET
            t.LockStatusCode = s.LockStatusCode, t.LockedByBatchId = s.LockedByBatchId,
            t.LockedByPackage = s.LockedByPackage, t.LockedAtUtc = s.LockedAtUtc,
            t.UnlockedByBatchId = NULL, t.UnlockedAtUtc = NULL, t.Notes = s.Notes
        WHEN NOT MATCHED THEN INSERT
            (LedgerCode, AccountingPeriod, LockStatusCode, LockedByBatchId, LockedByPackage, LockedAtUtc, Notes)
            VALUES (s.LedgerCode, s.AccountingPeriod, s.LockStatusCode, s.LockedByBatchId, s.LockedByPackage, s.LockedAtUtc, s.Notes)
        """
    )
    cfgTable = naming.table(catalog, "etl", "configuration")
    if spark.catalog.tableExists(cfgTable):
        keys = ", ".join(f"'Finance.PeriodStatus.{l}'" for l in ledgerCodes)
        spark.sql(f"UPDATE {cfgTable} SET ConfigurationValue = 'Closed' WHERE ConfigurationKey IN ({keys})")
    return len(ledgerCodes)


def apac445PeriodEnded(spark, catalog: str, accountingPeriod: str) -> bool:
    """'Lock APAC 445 Period' predicate against gold.dim_date (Fiscal Period 445 /
    Is Fiscal Period End). When the date dimension is not there yet the APAC
    ledger is locked on the calendar month like NA/EU."""
    dimDate = naming.table(catalog, "gold", "dim_date")
    if not spark.catalog.tableExists(dimDate):
        return True
    cols = {c.lower(): c for c in spark.table(dimDate).columns}
    period = cols.get("fiscalperiod445") or cols.get("fiscal_period_445")
    isEnd = cols.get("isfiscalperiodend") or cols.get("is_fiscal_period_end")
    dateCol = cols.get("date") or cols.get("calendardate")
    if not (period and isEnd and dateCol):
        return True
    return (
        spark.table(dimDate)
        .where((F.col(period) == F.lit(accountingPeriod)) & (F.col(isEnd) == F.lit(True)) & (F.col(dateCol) <= F.current_date()))
        .limit(1)
        .count()
        > 0
    )
