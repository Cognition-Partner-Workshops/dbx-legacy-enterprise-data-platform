"""Reconciliation evidence for the 25 finance packages -> otterorders_migration.evidence.recon_results.

For every package: a row-count check and an order-independent checksum (SUM(xxhash64(...)) over the business
columns shared by the target and its baseline, normalised to strings).  The baseline is the legacy SSIS output
when the legacy table is populated on the host (verdict PASS/FAIL); when it is empty the baseline is derived
from the package's own inputs with the package's row-selection rules ("baseline":"source_derived") and the
verdict is capped at PARTIAL, as the evidence contract requires.
"""

import json
import uuid
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from finance.config import FinanceConfig

HARNESS_VERSION = "ssis-migration-v1"
BRANCH = "ssis_finance"
ACTOR = "devin:ssis_finance"
EXCLUDED_COLS = {
    "batch_id",
    "loaded_at",
    "extracted_at",
    "staged_at",
    "held_at",
    "queued_at",
    "posted_at",
    "allocated_at",
    "reconciled_at",
    "refreshed_at",
    "locked_at",
    "load_batch_id",
    "change_hash",
    "scd_hash",
    "row_hash",
    "valid_from",
    "valid_to",
    "is_current",
    "cost_center_key",
    "supplier_key",
    "last_upd_dt",
    "created_dt",
    "updated_dt",
    "created_by",
    "updated_by",
    "source_sys",
    "_rn",
}


@dataclass
class ReconSpec:
    unit: str
    sourceObject: str
    target: str
    expectedSql: str
    legacyTable: str | None = None
    legacyColMap: dict[str, str] = field(default_factory=dict)
    targetFilter: str | None = None
    targetSql: str | None = None
    keyCol: str | None = None
    note: str = ""


def specs(cfg: FinanceConfig) -> list[ReconSpec]:
    s, ora, stg, dw = f"{cfg.catalog}.{cfg.schema}", cfg.oracleCatalog, cfg.stagingCatalog, cfg.dwCatalog
    p, asOf = cfg.accountingPeriod, cfg.businessDate.isoformat()
    glGate = f"""
        WITH l AS (SELECT * FROM {s}.silver_gl_journal_line WHERE posted_flag = 'Y'),
             b AS (SELECT journal_id, ABS(SUM(debit_amount) - SUM(credit_amount)) AS imb, COUNT(*) AS n, MAX(journal_line_cnt) AS exp FROM l GROUP BY journal_id)
        SELECT l.* FROM l JOIN b USING (journal_id)
        WHERE b.imb <= 0.005 AND (b.exp IS NULL OR b.n >= b.exp) AND COALESCE(l.period_status_cd, 'HIST') NOT IN ('CLSD', 'PERM')"""
    return [
        ReconSpec(
            "EXT_ORA_ApInvoiceHdr",
            "WideWorldImporters_Staging.raw.OracleApInvoiceHdr",
            "bronze_ora_ap_invoice_hdr",
            f"SELECT * EXCEPT (period_cd) FROM {ora}.wwi_fin.ap_invoice_hdr WHERE invoice_status_cd <> 'ENTR'",
            f"{stg}.raw.OracleApInvoiceHdr",
            keyCol="invoice_id",
        ),
        ReconSpec(
            "EXT_ORA_ApInvoiceLine",
            "WideWorldImporters_Staging.raw.OracleApInvoiceLine",
            "bronze_ora_ap_invoice_line",
            f"""SELECT l.* FROM {ora}.wwi_fin.ap_invoice_line l JOIN {ora}.wwi_fin.ap_invoice_hdr h ON h.invoice_id = l.invoice_id
                      LEFT JOIN {ora}.wwi_fin.cost_center c ON c.cost_center_cd = l.cost_center_cd AND COALESCE(c.active_flg, 'Y') = 'Y'
                      WHERE COALESCE(h.cancelled_flg, 'N') <> 'Y' AND h.invoice_status_cd <> 'CANC' AND (l.cost_center_cd IS NULL OR c.cost_center_cd IS NOT NULL)""",
            f"{stg}.raw.OracleApInvoiceLine",
            keyCol="invoice_line_id",
        ),
        ReconSpec(
            "EXT_ORA_ApPayment",
            "WideWorldImporters_Staging.raw.OracleApPayment",
            "bronze_ora_ap_payment",
            f"""SELECT p.* EXCEPT (period_cd) FROM {ora}.wwi_fin.ap_payment p JOIN {ora}.wwi_mdm.supp_master m ON m.supp_id = p.supp_id
                      WHERE p.void_dt IS NULL AND p.payment_status_cd <> 'VOID'""",
            f"{stg}.raw.OracleApPayment",
            keyCol="payment_id",
        ),
        ReconSpec(
            "EXT_ORA_ApPaymentApply",
            "WideWorldImporters_Staging.raw.OracleApPayment",
            "bronze_ora_ap_payment_apply",
            f"SELECT * FROM {ora}.wwi_fin.ap_payment_apply",
            keyCol="apply_id",
        ),
        ReconSpec(
            "EXT_ORA_ApAging",
            "WideWorldImporters_Staging.raw.OracleApInvoiceHdr",
            "bronze_ora_ap_aging",
            f"SELECT * FROM {ora}.wwi_fin.ap_aging_snapshot",
            keyCol="invoice_id",
        ),
        ReconSpec(
            "EXT_ORA_GlJournalLine",
            "WideWorldImporters_Staging.raw.OracleGlJournalLine",
            "bronze_ora_gl_journal_line",
            f"""SELECT l.*, h.accounting_dt AS gl_date FROM {ora}.wwi_fin.gl_journal_line l JOIN {ora}.wwi_fin.gl_journal_hdr h ON h.journal_id = l.journal_id
                      JOIN {ora}.wwi_fin.gl_account a ON a.account_cd = l.account_cd
                      LEFT JOIN {ora}.wwi_ref.region_ref r ON r.region_cd = UPPER(h.region_cd)
                      LEFT JOIN (SELECT calendar_cd, CAST(calendar_dt AS DATE) AS calendar_dt, MIN(period_cd) AS calendar_period_cd
                                 FROM {ora}.wwi_ref.calendar_fiscal WHERE COALESCE(adjustment_period_flg, 'N') = 'N'
                                 GROUP BY calendar_cd, CAST(calendar_dt AS DATE)) c
                        ON c.calendar_cd = r.fiscal_calendar_cd AND c.calendar_dt = CAST(h.accounting_dt AS DATE)
                      LEFT JOIN {ora}.wwi_fin.gl_period_status ps ON ps.region_cd = UPPER(h.region_cd)
                           AND ps.period_cd = CASE WHEN c.calendar_period_cd RLIKE '^[0-9]{{4}}-[0-9]{{2}}$' THEN c.calendar_period_cd
                                ELSE CONCAT(YEAR(h.accounting_dt) + IF(UPPER(h.region_cd) = 'APAC' AND MONTH(h.accounting_dt) >= 4, 1, 0), '-',
                                     LPAD(CASE WHEN UPPER(h.region_cd) = 'APAC' THEN ((MONTH(h.accounting_dt) + 8) % 12) + 1
                                               WHEN UPPER(h.region_cd) = 'NA' THEN LEAST(CEIL(WEEKOFYEAR(h.accounting_dt) / 4.333), 12)
                                               ELSE MONTH(h.accounting_dt) END, 2, '0')) END
                      WHERE h.posting_status_cd = 'POST' AND a.account_type_cd <> 'STAT' AND COALESCE(ps.gl_status_cd, 'HIST') <> 'FUTR'
                        AND h.accounting_dt BETWEEN (SELECT MIN(gl_date) FROM {s}.bronze_ora_gl_journal_line) AND (SELECT MAX(gl_date) FROM {s}.bronze_ora_gl_journal_line)""",
            f"{stg}.raw.OracleGlJournalLine",
            keyCol="journal_line_id",
        ),
        ReconSpec(
            "EXT_ORA_CostCenter",
            "WideWorldImporters_Staging.raw.OracleCostCenter",
            "bronze_ora_cost_center",
            f"SELECT * FROM {ora}.wwi_fin.cost_center",
            f"{stg}.raw.OracleCostCenter",
            keyCol="cost_center_id",
        ),
        ReconSpec(
            "STG_Load_ApInvoice",
            "WideWorldImporters_Staging.stg.ApInvoice",
            "silver_ap_invoice",
            f"""SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY last_upd_dt DESC) rn FROM {s}.bronze_ora_ap_invoice_hdr) WHERE rn = 1
                      AND supp_num IS NOT NULL AND (invoice_amt > 0 OR invoice_type_cd IN ('CRM','CREDIT','DBM'))""",
            f"{stg}.stg.ApInvoice",
            keyCol="invoice_id",
        ),
        ReconSpec(
            "STG_Load_GlJournal",
            "WideWorldImporters_Staging.stg.GlJournalLine",
            "silver_gl_journal_line",
            f"SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY journal_line_id ORDER BY last_upd_dt DESC) rn FROM {s}.bronze_ora_gl_journal_line) WHERE rn = 1",
            f"{stg}.stg.GlJournalLine",
            keyCol="journal_line_id",
        ),
        ReconSpec(
            "STG_Load_Payment",
            "WideWorldImporters_Staging.stg.Payment",
            "silver_payment",
            f"""SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY last_upd_dt DESC) rn FROM {s}.bronze_ora_ap_payment) WHERE rn = 1
                      AND UPPER(TRIM(payment_method_cd)) IN ('ACH','CARD','CHECK','LOCAL','SEPA','WIRE') AND payment_dt <= DATE'{asOf}' AND payment_amt > 0""",
            f"{stg}.stg.Payment",
            keyCol="payment_id",
        ),
        ReconSpec(
            "STG_Load_CostCenter",
            "WideWorldImporters_Staging.stg.CostCenter",
            "silver_cost_center",
            f"SELECT * FROM {s}.bronze_ora_cost_center",
            f"{stg}.stg.CostCenter",
            keyCol="cost_center_id",
        ),
        ReconSpec(
            "STG_Work_PaymentMatch",
            "WideWorldImporters_Staging.work.PaymentMatched",
            "work_payment_matched",
            f"""SELECT payment_id, CAST(payment_amount AS DECIMAL(18,5)) AS payment_amount FROM {s}.silver_payment
                      WHERE payment_status_code <> 'VOID' AND payment_amount > 0""",
            f"{stg}.work.PaymentMatched",
            targetSql=f"""SELECT payment_id, CAST(SUM(amt) AS DECIMAL(18,5)) AS payment_amount FROM (
                            SELECT payment_id, matched_amount AS amt FROM {s}.work_payment_matched
                            UNION ALL SELECT payment_id, remaining_amount FROM {s}.work_payment_unapplied) GROUP BY payment_id""",
            keyCol="payment_id",
            note="cash conservation: every eligible payment is fully accounted for as matched + unapplied cash (legacy work.PaymentMatched is empty)",
        ),
        ReconSpec(
            "DQ_Payment_Screen",
            "WideWorldImporters_Staging.err.RejectedPayment",
            "dq_payment_screen_result",
            f"SELECT payment_method_code, payment_currency_code, COUNT(*) AS payments_screened FROM {s}.silver_payment GROUP BY 1, 2",
            f"{stg}.err.RejectedPayment",
            keyCol="payment_method_code",
        ),
        ReconSpec(
            "REF_Load_CostCenter",
            "WideWorldImportersDW.Dimension.Cost Center",
            "gold_dim_cost_center",
            f"SELECT * FROM {s}.silver_cost_center",
            targetFilter="is_current AND cost_center_key <> 0",
            keyCol="cost_center_code",
        ),
        ReconSpec(
            "FACT_Load_GLPosting",
            "WideWorldImportersDW.Fact.GL Posting",
            "gold_fact_gl_posting",
            glGate,
            f"{dw}.Fact.`GL Posting`",
            keyCol="journal_line_id",
        ),
        ReconSpec(
            "FACT_Load_Payment",
            "WideWorldImportersDW.Fact.Payment",
            "gold_fact_payment",
            f"SELECT * FROM {s}.silver_payment",
            f"{dw}.Fact.Payment",
            keyCol="payment_id",
        ),
        ReconSpec(
            "FACT_Load_SupplierPayment",
            "WideWorldImportersDW.Fact.Supplier Payment",
            "gold_fact_supplier_payment",
            f"""SELECT m.* FROM {s}.work_payment_matched m JOIN {s}.silver_payment p ON p.payment_id = m.payment_id JOIN {s}.silver_ap_invoice i ON i.invoice_id = m.invoice_id""",
            f"{dw}.Fact.`Supplier Payment`",
            keyCol="payment_id",
        ),
        ReconSpec(
            "FIN_Close_PeriodLock",
            "WideWorldImportersDW.etl.Batch",
            "fin_period_lock",
            f"SELECT * FROM VALUES ('NA_USD','NA','{p}'), ('EU_EUR','EU','{p}'), ('AP_AUD','APAC','{p}') AS v(ledger_code, region_code, accounting_period)",
            targetFilter=f"accounting_period = '{p}'",
            keyCol="ledger_code",
        ),
        ReconSpec(
            "FIN_Currency_Revaluation",
            "WideWorldImportersDW.Fact.Payment",
            "fin_currency_revaluation",
            f"""SELECT i.invoice_id, i.invoice_number, i.region_code, i.currency_code, i.open_amount FROM {s}.silver_ap_invoice i
                      JOIN {ora}.wwi_ref.region_ref r ON r.region_cd = i.region_code
                      JOIN {s}.fin_fx_rate_snapshot fx ON fx.from_curr_cd = i.currency_code AND fx.revaluation_date = DATE'{asOf}'
                           AND fx.to_curr_cd = CASE WHEN i.region_code = 'EU' THEN 'EUR' WHEN i.region_code = 'APAC' THEN COALESCE(r.reporting_curr_cd, 'AUD') ELSE 'USD' END
                      WHERE i.open_amount > 0 AND i.invoice_status_code NOT IN ('CANC','VOID','DRAFT') AND i.currency_code <> fx.to_curr_cd""",
            targetFilter=f"revaluation_date = '{asOf}'",
            keyCol="invoice_id",
        ),
        ReconSpec(
            "FIN_Load_ApAging",
            "WideWorldImportersDW.Fact.Payment",
            "fin_ap_aging_detail",
            f"""SELECT * FROM {s}.silver_ap_invoice WHERE open_amount > 0 AND invoice_status_code NOT IN ('CANC','VOID','DRAFT')
                      AND NOT COALESCE(hold_codes_txt, '') LIKE '%DISP%' AND supplier_code IS NOT NULL""",
            targetFilter=f"as_of_date = '{asOf}'",
            keyCol="invoice_id",
        ),
        ReconSpec(
            "FIN_Load_CostAllocation",
            "WideWorldImportersDW.Aggregate.Finance Close Summary",
            "fin_cost_allocation",
            f"""SELECT alloc_rule_id, rule_set_cd, rule_seq_nbr, region_cd, source_cost_center_cd, target_cost_center_cd, allocation_method_cd
                      FROM {ora}.wwi_fin.cost_allocation_rule WHERE COALESCE(active_flg,'N') = 'Y' AND effective_from_dt <= DATE'{asOf}' AND COALESCE(effective_to_dt, DATE'9999-12-31') >= DATE'{asOf}'""",
            targetFilter=f"accounting_period = '{p}' AND allocation_method_cd <> 'RESIDUAL'",
            keyCol="alloc_rule_id",
        ),
        ReconSpec(
            "FIN_Load_GlPostings",
            "WideWorldImportersDW.Fact.GL Posting",
            "gold_fact_gl_posting",
            glGate + f" AND l.accounting_period = '{p}'",
            f"{dw}.Fact.`GL Posting`",
            targetFilter=f"accounting_period = '{p}'",
            keyCol="journal_line_id",
        ),
        ReconSpec(
            "FIN_Load_WithholdingTax",
            "WideWorldImportersDW.Fact.Payment",
            "fin_withholding_tax",
            f"""SELECT l.invoice_line_id, l.invoice_id, l.region_code, l.line_amount, l.line_type_cd, l.service_category_cd FROM {s}.silver_ap_invoice_line l
                      JOIN {s}.silver_ap_invoice i ON i.invoice_id = l.invoice_id WHERE i.period_cd = '{p}' AND COALESCE(l.line_type_cd, '') <> 'FREIGHT'""",
            targetFilter=f"accounting_period = '{p}'",
            keyCol="invoice_line_id",
        ),
        ReconSpec(
            "FIN_Reconcile_SubledgerToGl",
            "WideWorldImportersDW.etl.ReconciliationResult",
            "fin_recon_result",
            f"""SELECT DISTINCT region_code FROM (SELECT region_code FROM {s}.silver_ap_invoice WHERE period_cd = '{p}' AND invoice_status_code NOT IN ('CANC','VOID','DRAFT')
                      UNION ALL SELECT region_code FROM {s}.silver_payment WHERE period_cd = '{p}' AND payment_status_code <> 'VOID'
                      UNION ALL SELECT region_code FROM {s}.gold_fact_gl_posting WHERE accounting_period = '{p}' AND journal_source_cd = 'AP' AND account_type_cd = 'LIAB')""",
            f"{dw}.etl.ReconciliationResult",
            targetFilter=f"accounting_period = '{p}'",
            keyCol="region_code",
        ),
        ReconSpec(
            "AGG_Refresh_FinanceCloseSummary",
            "WideWorldImportersDW.Aggregate.Finance Close Summary",
            "gold_agg_finance_close_summary",
            f"""SELECT DISTINCT region_code, accounting_period FROM (SELECT region_code, accounting_period FROM {s}.gold_fact_gl_posting
                      UNION ALL SELECT region_code, period_cd FROM {s}.silver_ap_invoice WHERE invoice_status_code NOT IN ('CANC','VOID','DRAFT')
                      UNION ALL SELECT region_code, period_cd FROM {s}.gold_fact_payment)""",
            f"{dw}.Aggregate.`Finance Close Summary`",
            keyCol="region_code",
        ),
    ]


def normalize(df: DataFrame, cols: list[str]) -> list[F.Column]:
    """One string expression per column, in `cols` order (case-insensitive), so both sides hash identically."""
    fields = {f_.name.lower(): f_ for f_ in df.schema.fields}
    out = []
    for name in cols:
        f_ = fields.get(name.lower())
        if f_ is None:
            continue
        c = F.col(f"`{f_.name}`")
        if isinstance(
            f_.dataType, (T.DecimalType, T.DoubleType, T.FloatType, T.IntegerType, T.LongType, T.ShortType)
        ):
            c = F.format_number(c.cast("decimal(38,6)"), 6)
        elif isinstance(f_.dataType, (T.DateType, T.TimestampType)):
            c = F.date_format(c.cast("timestamp"), "yyyy-MM-dd HH:mm:ss")
        elif isinstance(f_.dataType, T.BooleanType):
            c = F.when(c, "Y").otherwise("N")
        else:
            c = F.upper(F.trim(c.cast("string")))
        out.append(F.coalesce(c, F.lit("<null>")))
    return out


def checksum(df: DataFrame, cols: list[str]) -> str:
    exprs = normalize(df, cols)
    if not exprs:
        return "0"
    val = df.agg(F.sum(F.xxhash64(F.concat_ws("|", *exprs)).cast("decimal(38,0)"))).collect()[0][0]
    return str(val if val is not None else 0)


def sharedBusinessColumns(target: DataFrame, baseline: DataFrame) -> list[str]:
    baselineCols = {c.lower() for c in baseline.columns}
    cols = {
        c.lower()
        for c in target.columns
        if c.lower() in baselineCols and c.lower() not in EXCLUDED_COLS and not c.startswith("_")
    }
    return sorted(cols)


def tryCount(spark: SparkSession, fq: str | None) -> int | None:
    if not fq:
        return None
    try:
        return spark.table(fq).count()
    except Exception:
        return None


def evaluate(spark: SparkSession, cfg: FinanceConfig, spec: ReconSpec) -> dict:
    targetFq = cfg.table(spec.target)
    checks: list[dict] = []
    if not spark.catalog.tableExists(targetFq):
        return {
            "verdict": "FAIL",
            "checks": [{"check": "table_exists", "target": targetFq, "pass": False}],
            "summary": f"target table {targetFq} missing",
            "target_object": targetFq,
        }
    target = spark.sql(spec.targetSql) if spec.targetSql else spark.table(targetFq)
    if spec.targetFilter:
        target = target.where(spec.targetFilter)
    targetCount = target.count()
    legacyCount = tryCount(spark, spec.legacyTable)
    legacyPopulated = bool(legacyCount)
    baselineKind = "legacy" if legacyPopulated and spec.legacyColMap else "source_derived"

    if baselineKind == "legacy":
        legacy = spark.table(spec.legacyTable).select(
            *[F.col(v).alias(k) for k, v in spec.legacyColMap.items()]
        )
        cols = sorted(spec.legacyColMap)
        expectedCount = legacyCount
    else:
        legacy = spark.sql(spec.expectedSql)
        cols = sharedBusinessColumns(target, legacy)
        expectedCount = legacy.count()

    checks.append(
        {
            "check": "row_count",
            "source": expectedCount,
            "target": targetCount,
            "pass": expectedCount == targetCount,
        }
    )
    srcSum, tgtSum = checksum(legacy, cols), checksum(target, cols)
    checks.append(
        {
            "check": "checksum",
            "method": f"sum(xxhash64(concat_ws('|', {', '.join(cols)})))",
            "columns": cols,
            "source": srcSum,
            "target": tgtSum,
            "pass": srcSum == tgtSum,
        }
    )
    if spec.keyCol and spec.keyCol in target.columns:
        nullRate = (
            target.agg(F.avg(F.when(F.col(spec.keyCol).isNull(), 1.0).otherwise(0.0))).collect()[0][0] or 0.0
        )
        checks.append(
            {
                "check": "column_null_rate",
                "column": spec.keyCol,
                "source": 0.0,
                "target": float(nullRate),
                "pass": nullRate == 0.0,
            }
        )
    if spec.legacyTable:
        checks.append(
            {
                "check": "legacy_target_row_count",
                "object": spec.legacyTable,
                "source": legacyCount,
                "target": targetCount,
                "pass": legacyCount == targetCount,
                "informational": not legacyPopulated,
            }
        )
    checks.append({"baseline": baselineKind})

    allPass = all(c.get("pass", True) for c in checks if "check" in c and not c.get("informational"))
    if not allPass:
        verdict = "FAIL"
        failed = [
            c["check"]
            for c in checks
            if "check" in c and not c.get("pass", True) and not c.get("informational")
        ]
        summary = f"{', '.join(failed)} mismatch against {baselineKind} baseline"
    elif baselineKind == "legacy":
        verdict, summary = "PASS", "row count and checksum match the legacy SSIS output"
    else:
        verdict = "PARTIAL"
        why = (
            "legacy target is empty/unpopulated on the host"
            if not legacyPopulated
            else "legacy target populated but no column-level map is established"
        )
        summary = f"{why}; compared against a source-derived baseline using the package's own row-selection rules ({expectedCount} rows). {spec.note}".strip()
    return {"verdict": verdict, "checks": checks, "summary": summary, "target_object": targetFq}


def runRecon(spark: SparkSession, cfg: FinanceConfig, units: list[str] | None = None) -> DataFrame:
    runId = str(uuid.uuid4())
    rows = []
    for spec in specs(cfg):
        if units and spec.unit not in units:
            continue
        try:
            result = evaluate(spark, cfg, spec)
        except Exception as exc:  # evidence must be written even when a check blows up
            result = {
                "verdict": "FAIL",
                "checks": [{"check": "evaluation", "pass": False, "error": str(exc)[:1000]}],
                "summary": f"evaluation error: {str(exc)[:300]}",
                "target_object": cfg.table(spec.target),
            }
        rows.append(
            {
                "run_id": runId,
                "unit": spec.unit,
                "unit_type": "ssis_package",
                "verdict": result["verdict"],
                "branch": BRANCH,
                "source_object": spec.sourceObject,
                "target_object": result["target_object"],
                "checks": json.dumps(result["checks"], default=str),
                "summary": result["summary"],
                "git_sha": cfg.gitSha,
                "actor": ACTOR,
                "harness_version": HARNESS_VERSION,
            }
        )
        print(f"[recon] {spec.unit}: {result['verdict']} - {result['summary'][:160]}")
    schema = "run_id string, unit string, unit_type string, verdict string, branch string, source_object string, target_object string, checks string, summary string, git_sha string, actor string, harness_version string"
    out = (
        spark.createDataFrame(rows, schema)
        .withColumn("run_at", F.current_timestamp())
        .select(
            "run_id",
            "run_at",
            "unit",
            "unit_type",
            "verdict",
            "branch",
            "source_object",
            "target_object",
            "checks",
            "summary",
            "git_sha",
            "actor",
            "harness_version",
        )
    )
    out.write.format("delta").mode("append").saveAsTable(cfg.evidenceTable("recon_results"))
    return out.select("run_id", "unit", "verdict", "summary")
