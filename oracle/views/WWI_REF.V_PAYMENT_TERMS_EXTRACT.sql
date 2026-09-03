/* ============================================================================
 * Object      : WWI_REF.V_PAYMENT_TERMS_EXTRACT (view)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.PAYMENT_TERMS, WWI_REF.STATUS_CODE_REF,
 *               WWI_FIN.FN_DUE_DATE
 * Called by   : SSIS EXT_ORA_PaymentTerms (weekly full refresh)
 * Notes       : Terms live in WWI_FIN but the DW consumes them as reference
 *               data, so the extract view was put in WWI_REF in 2004. The
 *               sample due dates are produced by calling FN_DUE_DATE with a
 *               fixed anchor date per region so the DW can sanity-check the
 *               terms it received.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_REF.V_PAYMENT_TERMS_EXTRACT AS
SELECT t.PAYMENT_TERMS_CD,
       t.TERMS_NAME,
       t.NET_DAYS_NUM,
       t.DISCOUNT_DAYS_NUM,
       t.DISCOUNT_PCT,
       t.PROXIMO_FLAG,
       t.PROXIMO_DAY_NUM,
       t.REGION_CD,
       t.ACTIVE_FLAG,
       sc.STATUS_NAME                                         AS ACTIVE_STATUS_NAME,
       WWI_FIN.FN_DUE_DATE(TRUNC(SYSDATE, 'MM'), t.PAYMENT_TERMS_CD, 'NA')   AS SAMPLE_DUE_DT_NA,
       WWI_FIN.FN_DUE_DATE(TRUNC(SYSDATE, 'MM'), t.PAYMENT_TERMS_CD, 'EU')   AS SAMPLE_DUE_DT_EU,
       WWI_FIN.FN_DUE_DATE(TRUNC(SYSDATE, 'MM'), t.PAYMENT_TERMS_CD, 'APAC') AS SAMPLE_DUE_DT_APAC,
       CASE
           WHEN NVL(t.DISCOUNT_PCT, 0) > 0 THEN 'DISCOUNT'
           WHEN NVL(t.PROXIMO_FLAG, 'N') = 'Y' THEN 'PROXIMO'
           ELSE 'NET'
       END                                                    AS TERMS_CLASS_CD,
       t.LAST_UPD_DT
  FROM WWI_FIN.PAYMENT_TERMS t
  LEFT OUTER JOIN WWI_REF.STATUS_CODE_REF sc
    ON sc.CODE_SET_CD = 'ACTIVE_FLAG'
   AND sc.STATUS_CD   = t.ACTIVE_FLAG
/
