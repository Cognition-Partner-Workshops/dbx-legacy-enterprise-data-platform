import datetime as dt
from decimal import Decimal

from pyspark.sql import Row

import finance_rules as rules

COLS = ["GlJournalLineId", "JournalNumber", "JournalLineNumber", "LedgerCode", "CostCentreCode", "AccountCode", "PostingDate",
        "AccountingPeriod", "CurrencyCode", "EnteredDebitAmount", "EnteredCreditAmount", "FunctionalDebitAmount",
        "FunctionalCreditAmount", "SourceSubledgerCode", "SourceDocumentNumber", "JournalStatusCode", "LoadBatchId"]


def lines(spark):
    d = lambda x: Decimal(str(x)) if x is not None else None  # noqa: E731
    rows = [
        (1, "J1", 1, "EU01", "CC1", "2100", dt.date(2024, 3, 5), "2024-03", "EUR", d(100), d(0), d(110), d(0), "AP", "D1", "POSTED", 5),
        (2, "J1", 2, "EU01", "CC1", "5000", dt.date(2024, 3, 5), "2024-03", "EUR", d(0), d(100), None, None, "AP", "D1", "POSTED", 5),
        (3, "J2", 1, "APAC01", "CC2", "2100", dt.date(2024, 4, 1), "2024-04", "AUD", d(50), d(0), d(50), d(0), "AP", "D2", "POSTED", 5),  # other period -> held
        (4, "J3", 1, "NA01", "CC3", "2100", dt.date(2024, 3, 9), "2024-03", "USD", d(70), d(0), d(70), d(0), "AR", "D3", "UNPOSTED", 5),  # not posted
        (5, "J4", 1, "NA01", "CC3", "2100", dt.date(2024, 3, 9), "2024-03", "USD", d(70), d(0), d(70), d(0), "AR", "D4", "POSTED", 5),
        (6, "J4", 2, "NA01", "CC3", "4000", dt.date(2024, 3, 9), "2024-03", "USD", d(0), d(69), d(0), d(69), "AR", "D4", "POSTED", 5),  # unbalanced by 1
        (7, "J5", 1, "NA01", "CC3", "2100", dt.date(2024, 3, 9), "2024-03", "USD", d(70), d(0), d(70), d(0), "AR", "D5", "POSTED", 4),  # other batch
        (10, "J2", 2, "APAC01", "CC2", "4000", dt.date(2024, 4, 1), "2024-04", "AUD", d(0), d(50), d(0), d(50), "AP", "D2", "POSTED", 5),
        (11, "J3", 2, "NA01", "CC3", "4000", dt.date(2024, 3, 9), "2024-03", "USD", d(0), d(70), d(0), d(70), "AR", "D3", "UNPOSTED", 5),
    ]
    schema = ("GlJournalLineId BIGINT, JournalNumber STRING, JournalLineNumber INT, LedgerCode STRING, CostCentreCode STRING, AccountCode STRING, "
              "PostingDate DATE, AccountingPeriod STRING, CurrencyCode STRING, EnteredDebitAmount DECIMAL(19,4), EnteredCreditAmount DECIMAL(19,4), "
              "FunctionalDebitAmount DECIMAL(19,4), FunctionalCreditAmount DECIMAL(19,4), SourceSubledgerCode STRING, SourceDocumentNumber STRING, "
              "JournalStatusCode STRING, LoadBatchId BIGINT")
    return spark.createDataFrame(rows, schema)


def openPeriods(spark):
    return spark.createDataFrame([Row(LedgerCode="EU01", AccountingPeriod="2024-03"), Row(LedgerCode="APAC01", AccountingPeriod="2024-04"),
                                  Row(LedgerCode="NA01", AccountingPeriod="2024-03")])


def test_unbalanced_journal_count(spark):
    assert rules.unbalancedJournalCount(lines(spark), 5) == 1
    assert rules.unbalancedJournalCount(lines(spark), 6) == 0


def test_gl_posted_lines_and_derivations(spark):
    posted = rules.deriveGlPostingAttributes(rules.glPostedLines(lines(spark), openPeriods(spark), 5))
    rows = {r["GlJournalLineId"]: r for r in posted.collect()}
    assert set(rows) == {1, 2, 3, 5, 6, 10}
    assert rows[1]["TaxRegimeCode"] == "VAT" and rows[3]["TaxRegimeCode"] == "GST" and rows[5]["TaxRegimeCode"] == "SALESTAX"
    # functional amounts fall back to entered amounts
    assert rows[2]["FunctionalCreditAmount"] == Decimal("100") and rows[2]["NetAmount"] == Decimal("-100")
    assert rows[1]["NetAmount"] == Decimal("110") and rows[1]["PostingSide"] == "DR" and rows[2]["PostingSide"] == "CR"
    assert rows[1]["SubledgerSourceKey"] == "AP|D1"


def test_split_held_lines(spark):
    posted = rules.deriveGlPostingAttributes(rules.glPostedLines(lines(spark), openPeriods(spark), 5))
    postable, held = rules.splitHeldLines(posted, "2024-03")
    assert {r["GlJournalLineId"] for r in postable.collect()} == {1, 2, 5, 6}
    heldRows = held.collect()
    assert sorted(r["GlJournalLineId"] for r in heldRows) == [3, 10] and heldRows[0]["RequestedAccountingPeriod"] == "2024-03"
