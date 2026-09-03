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
 *
 *               Proximo terms are TERM_BASIS_CD EOM/DOM; the day of month is
 *               DUE_DAY_OF_MONTH_NBR. The first discount tier is the one the
 *               DW carries.
 *               Reads WWI_FIN; the SELECT grants live in
 *               oracle/ddl/05_grant_privileges.sql.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_REF.V_PAYMENT_TERMS_EXTRACT AS
SELECT t.PAYMENT_TERMS_CD,
       t.TERMS_DESC                                           AS TERMS_NAME,
       t.TERM_BASIS_CD,
       t.NET_DAYS                                             AS NET_DAYS_NUM,
       t.DISCOUNT_1_DAYS                                      AS DISCOUNT_DAYS_NUM,
       t.DISCOUNT_1_PCT                                       AS DISCOUNT_PCT,
       t.DISCOUNT_2_DAYS                                      AS DISCOUNT_2_DAYS_NUM,
       t.DISCOUNT_2_PCT,
       t.DISCOUNT_BASIS_CD,
       CASE WHEN t.TERM_BASIS_CD IN ('EOM', 'DOM') THEN 'Y' ELSE 'N' END
                                                              AS PROXIMO_FLAG,
       t.DUE_DAY_OF_MONTH_NBR                                 AS PROXIMO_DAY_NUM,
       t.MONTHS_FORWARD_NBR,
       t.REGION_CD,
       t.ACTIVE_FLG                                           AS ACTIVE_FLAG,
       t.EFFECTIVE_FROM_DT,
       t.EFFECTIVE_TO_DT,
       t.LEGACY_TERMS_CD,
       sc.STATUS_NAME                                         AS ACTIVE_STATUS_NAME,
       WWI_FIN.FN_DUE_DATE(TRUNC(SYSDATE, 'MM'), t.PAYMENT_TERMS_CD, 'NA')   AS SAMPLE_DUE_DT_NA,
       WWI_FIN.FN_DUE_DATE(TRUNC(SYSDATE, 'MM'), t.PAYMENT_TERMS_CD, 'EU')   AS SAMPLE_DUE_DT_EU,
       WWI_FIN.FN_DUE_DATE(TRUNC(SYSDATE, 'MM'), t.PAYMENT_TERMS_CD, 'APAC') AS SAMPLE_DUE_DT_APAC,
       CASE
           WHEN NVL(t.DISCOUNT_1_PCT, 0) > 0            THEN 'DISCOUNT'
           WHEN t.TERM_BASIS_CD IN ('EOM', 'DOM')       THEN 'PROXIMO'
           WHEN t.TERM_BASIS_CD IN ('IMMED', 'PREPAY')  THEN t.TERM_BASIS_CD
           ELSE 'NET'
       END                                                    AS TERMS_CLASS_CD,
       NVL(t.UPDATED_DT, t.CREATED_DT)                        AS LAST_UPD_DT
  FROM WWI_FIN.PAYMENT_TERMS t
  LEFT OUTER JOIN WWI_REF.STATUS_CODE_REF sc
    ON sc.ENTITY_CD  = 'PAYMENT_TERMS'
   AND sc.STATUS_CD  = t.ACTIVE_FLG
/
