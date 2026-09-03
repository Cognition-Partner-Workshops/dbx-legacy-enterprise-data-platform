/* ============================================================================
 * Object      : WWI_FIN.V_GL_JOURNAL_EXTRACT (view)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.GL_JOURNAL_HDR, WWI_FIN.GL_JOURNAL_LINE,
 *               WWI_FIN.GL_ACCOUNT, WWI_FIN.COST_CENTER,
 *               WWI_FIN.GL_PERIOD_STATUS, WWI_FIN.FN_CONVERT_AMOUNT
 * Called by   : SSIS EXT_ORA_GlJournalLine (incremental on JOURNAL_LINE_ID)
 * History     : 1998 original; 2004 statistical lines excluded; 2015 the
 *               unposted lines were included again for the flash close.
 * Notes       : Journal lines carry the account and cost centre as codes, not
 *               ids, so the chart of accounts is joined on ACCOUNT_CD. A
 *               statistical line is one posted to an ACCOUNT_TYPE_CD of STAT.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_FIN.V_GL_JOURNAL_EXTRACT AS
SELECT jl.JOURNAL_LINE_ID,
       jh.JOURNAL_ID,
       jh.JOURNAL_NBR                                      AS JOURNAL_NUM,
       jh.LEDGER_CD,
       jh.JOURNAL_SOURCE_CD,
       jh.JOURNAL_CATEGORY_CD,
       jh.REGION_CD,
       jh.LEGAL_ENTITY_CD                                  AS ORG_CD,
       jh.PERIOD_CD,
       ps.GL_STATUS_CD                                     AS PERIOD_STATUS_CD,
       jh.ACCOUNTING_DT                                    AS GL_DATE,
       jh.POSTING_STATUS_CD,
       CASE WHEN jh.POSTING_STATUS_CD = 'POST' THEN 'Y' ELSE 'N' END AS POSTED_FLAG,
       jh.POSTED_DT,
       jh.REVERSAL_FLG                                     AS REVERSAL_FLAG,
       jh.REVERSAL_OF_JOURNAL_ID                           AS REVERSED_JOURNAL_ID,
       jh.ACCRUAL_FLG                                      AS ACCRUAL_FLAG,
       jl.LINE_NBR                                         AS LINE_NUM,
       ga.GL_ACCOUNT_ID,
       jl.ACCOUNT_CD,
       ga.ACCOUNT_NAME,
       ga.ACCOUNT_TYPE_CD,
       jl.COST_CENTER_CD,
       cc.COST_CENTER_ID,
       cc.COST_CENTER_NAME,
       jl.ENTERED_CURR_CD                                  AS CURRENCY_CD,
       NVL(jl.ENTERED_DEBIT_AMT, 0)                        AS DEBIT_AMT,
       NVL(jl.ENTERED_CREDIT_AMT, 0)                       AS CREDIT_AMT,
       NVL(jl.ENTERED_DEBIT_AMT, 0) - NVL(jl.ENTERED_CREDIT_AMT, 0) AS NET_AMT,
       NVL(jl.ACCOUNTED_DEBIT_AMT,
           WWI_FIN.FN_CONVERT_AMOUNT(NVL(jl.ENTERED_DEBIT_AMT, 0), jl.ENTERED_CURR_CD,
                                     'USD', jh.ACCOUNTING_DT, 'CORP'))  AS BASE_DEBIT_AMT_USD,
       NVL(jl.ACCOUNTED_CREDIT_AMT,
           WWI_FIN.FN_CONVERT_AMOUNT(NVL(jl.ENTERED_CREDIT_AMT, 0), jl.ENTERED_CURR_CD,
                                     'USD', jh.ACCOUNTING_DT, 'CORP'))  AS BASE_CREDIT_AMT_USD,
       jl.LINE_DESC,
       jl.SOURCE_DOC_TYPE_CD                               AS SRC_DOC_TYPE_CD,
       jl.SOURCE_DOC_ID                                    AS SRC_DOC_ID,
       jh.CREATED_BY,
       jh.CREATED_DT,
       NVL(jh.UPDATED_DT, jh.CREATED_DT)                   AS LAST_UPD_DT
  FROM WWI_FIN.GL_JOURNAL_LINE jl
  JOIN WWI_FIN.GL_JOURNAL_HDR jh
    ON jh.JOURNAL_ID = jl.JOURNAL_ID
  JOIN WWI_FIN.GL_ACCOUNT ga
    ON ga.ACCOUNT_CD = jl.ACCOUNT_CD
  LEFT OUTER JOIN WWI_FIN.COST_CENTER cc
    ON cc.COST_CENTER_CD = jl.COST_CENTER_CD
  LEFT OUTER JOIN WWI_FIN.GL_PERIOD_STATUS ps
    ON ps.LEDGER_CD = jh.LEDGER_CD
   AND ps.PERIOD_CD = jh.PERIOD_CD
 WHERE ga.ACCOUNT_TYPE_CD <> 'STAT'
/
