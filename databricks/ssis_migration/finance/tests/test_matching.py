from datetime import date

from pyspark.sql import functions as F

from finance.matching import matchPayments

PAY = "payment_id decimal(12,0), payment_number string, supplier_id decimal(12,0), region_code string, payment_date date, payment_amount decimal(18,5), payment_status_code string"
INV = (
    "invoice_id decimal(12,0), invoice_number string, supplier_id decimal(12,0), open_amount decimal(18,5), invoice_date date, due_date date, "
    "discount_percent decimal(9,4), discount_due_date date, is_on_hold boolean, invoice_status_code string"
)
REM = "payment_id decimal(12,0), invoice_id decimal(12,0), apply_seq int, remit_amount decimal(18,5)"


def test_three_pass_matching_and_tolerance(spark, typed):
    d = date(2024, 12, 1)
    payments = typed(
        [
            (1, "P1", 10, "NA", d, 500.0, "PAID"),  # pass 1: remittance names invoice 101
            (2, "P2", 10, "NA", d, 300.0, "PAID"),  # pass 2: exact amount invoice 102
            (
                3,
                "P3",
                20,
                "EU",
                d,
                250.0,
                "PAID",
            ),  # pass 3: FIFO over 201 (100, older) then 202 (200) -> 150 partial
            (
                4,
                "P4",
                30,
                "NA",
                d,
                100.01,
                "PAID",
            ),  # pass 2 net of discount? no: pays 100 invoice + 0.01 residual inside NA tolerance
            (5, "P5", 30, "EU", d, 100.05, "PAID"),  # 0.05 residual outside EU tolerance -> UNAPPLIED_CASH
            (6, "P6", 10, "NA", d, 999.0, "VOID"),  # void, ignored
            (
                7,
                "P7",
                40,
                "NA",
                d,
                98.0,
                "PAID",
            ),  # pass 2 exact net of 2% eligible discount on invoice 401 (100)
        ],
        PAY,
    )
    invoices = typed(
        [
            (101, "I101", 10, 500.0, d, d, None, None, False, "APPR"),
            (102, "I102", 10, 300.0, d, d, None, None, False, "APPR"),
            (103, "I103", 10, 300.0, d, d, None, None, True, "APPR"),  # on hold - excluded
            (201, "I201", 20, 100.0, date(2024, 10, 1), date(2024, 10, 31), None, None, False, "APPR"),
            (202, "I202", 20, 200.0, date(2024, 11, 1), date(2024, 11, 30), None, None, False, "APPR"),
            (301, "I301", 30, 100.0, d, d, None, None, False, "APPR"),
            (302, "I302", 30, 100.0, d, d, None, None, False, "CANC"),  # cancelled - excluded
            (401, "I401", 40, 100.0, d, d, 2.0, date(2024, 12, 31), False, "APPR"),
        ],
        INV,
    )
    remits = typed([(1, 101, 1, 500.0)], REM)

    matches, unapplied = matchPayments(payments, invoices, remits)
    m = {(int(r["payment_id"]), int(r["invoice_id"])): r for r in matches.collect()}

    assert m[(1, 101)]["match_pass"] == 1 and float(m[(1, 101)]["matched_amount"]) == 500.0
    assert m[(2, 102)]["match_pass"] == 2 and float(m[(2, 102)]["matched_amount"]) == 300.0
    assert m[(3, 201)]["match_pass"] == 3 and float(m[(3, 201)]["matched_amount"]) == 100.0
    assert m[(3, 202)]["match_pass"] == 3 and float(m[(3, 202)]["matched_amount"]) == 150.0
    assert float(m[(7, 401)]["matched_amount"]) == 98.0 and float(m[(7, 401)]["discount_amount"]) == 2.0
    assert (6, 101) not in m and all(int(r["invoice_id"]) not in (103, 302) for r in matches.collect())

    u = {int(r["payment_id"]): r for r in unapplied.collect()}
    assert u[4]["within_tolerance"] and u[4]["reject_reason_code"] == "TOLERANCE_WRITEOFF"
    assert not u[5]["within_tolerance"] and u[5]["reject_reason_code"] == "UNAPPLIED_CASH"
    assert 1 not in u and 2 not in u

    # an invoice is never over-applied
    perInvoice = (
        matches.groupBy("invoice_id").agg(F.sum("matched_amount").alias("a")).join(invoices, "invoice_id")
    )
    assert perInvoice.where(F.col("a") > F.col("open_amount")).count() == 0
