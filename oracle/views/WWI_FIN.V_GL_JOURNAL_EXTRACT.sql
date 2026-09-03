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
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_FIN.V_GL_JOURNAL_EXTRACT AS
SELECT jl.JOURNAL_LINE_ID,
       jh.JOURNAL_ID,
       jh.JOURNAL_NUM,
       jh.JOURNAL_SOURCE_CD,
       jh.JOURNAL_CATEGORY_CD,
       jh.REGION_CD,
       jh.ORG_CD,
       jh.PERIOD_CD,
       ps.STATUS_CD                                        AS PERIOD_STATUS_CD,
       jh.GL_DATE,
       jh.POSTED_FLAG,
       jh.POSTED_DT,
       jh.REVERSAL_FLAG,
       jh.REVERSED_JOURNAL_ID,
       jh.ACCRUAL_FLAG,
       jl.LINE_NUM,
       jl.GL_ACCOUNT_ID,
       ga.ACCOUNT_CD,
       ga.ACCOUNT_NAME,
       ga.ACCOUNT_TYPE_CD,
       jl.COST_CENTER_ID,
       cc.COST_CENTER_CD,
       cc.COST_CENTER_NAME,
       jl.CURRENCY_CD,
       NVL(jl.DEBIT_AMT, 0)                                AS DEBIT_AMT,
       NVL(jl.CREDIT_AMT, 0)                               AS CREDIT_AMT,
       NVL(jl.DEBIT_AMT, 0) - NVL(jl.CREDIT_AMT, 0)        AS NET_AMT,
       NVL(jl.BASE_DEBIT_AMT,
           WWI_FIN.FN_CONVERT_AMOUNT(NVL(jl.DEBIT_AMT, 0), jl.CURRENCY_CD, 'USD',
                                     jh.GL_DATE, 'CORP'))  AS BASE_DEBIT_AMT_USD,
       NVL(jl.BASE_CREDIT_AMT,
           WWI_FIN.FN_CONVERT_AMOUNT(NVL(jl.CREDIT_AMT, 0), jl.CURRENCY_CD, 'USD',
                                     jh.GL_DATE, 'CORP'))  AS BASE_CREDIT_AMT_USD,
       jl.LINE_DESC,
       jl.SRC_DOC_TYPE_CD,
       jl.SRC_DOC_ID,
       jh.CREATED_BY,
       jh.CREATED_DT,
       jh.LAST_UPD_DT
  FROM WWI_FIN.GL_JOURNAL_LINE jl
  JOIN WWI_FIN.GL_JOURNAL_HDR jh
    ON jh.JOURNAL_ID = jl.JOURNAL_ID
  JOIN WWI_FIN.GL_ACCOUNT ga
    ON ga.GL_ACCOUNT_ID = jl.GL_ACCOUNT_ID
  LEFT OUTER JOIN WWI_FIN.COST_CENTER cc
    ON cc.COST_CENTER_ID = jl.COST_CENTER_ID
  LEFT OUTER JOIN WWI_FIN.GL_PERIOD_STATUS ps
    ON ps.PERIOD_CD = jh.PERIOD_CD
   AND ps.REGION_CD = jh.REGION_CD
 WHERE NVL(ga.STATISTICAL_FLAG, 'N') = 'N'
/
