/* ============================================================================
 * Object      : WWI_MDM.V_SUPPLIER_BANK_MASKED (view)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.SUPP_BANK_ACCOUNT, WWI_MDM.SUPP_MASTER
 * Called by   : WWI_FIN.PKG_AP_PAYMENT, treasury reporting, and the supplier
 *               statement export (PRC_Export_SupplierStatement)
 * History     : 2009 created after the audit finding that the payment run
 *               report printed full account numbers.
 * Notes       : Masking rules differ by region because the auditors asked for
 *               different things in each jurisdiction. This view is the only
 *               approved way to read bank details outside the payment package.
 *
 *               The account number itself is only held encrypted
 *               (ACCOUNT_NBR_ENC) plus a last-four column, so the display
 *               string is built from ACCOUNT_NBR_LAST4 and the IBAN.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_MDM.V_SUPPLIER_BANK_MASKED AS
SELECT b.SUPP_BANK_ID                                    AS BANK_ACCT_ID,
       b.SUPP_ID,
       s.SUPP_NBR                                        AS SUPP_NUM,
       s.SUPP_NAME,
       s.REGION_CD,
       b.BANK_NAME,
       b.BANK_COUNTRY_CD,
       CASE s.REGION_CD
           WHEN 'EU'   THEN SUBSTR(b.IBAN_TXT, 1, 4) || RPAD('*', 12, '*')
                            || NVL(b.ACCOUNT_NBR_LAST4, SUBSTR(b.IBAN_TXT, -4))
           WHEN 'APAC' THEN RPAD('*', 8, '*') || SUBSTR(b.ACCOUNT_NBR_LAST4, -3)
           ELSE RPAD('*', 6, '*') || b.ACCOUNT_NBR_LAST4
       END                                               AS ACCT_DISPLAY_TXT,
       CASE WHEN s.REGION_CD = 'EU' THEN b.BIC_CD ELSE NULL END AS SWIFT_CD,
       b.ACCOUNT_CURR_CD                                 AS CURRENCY_CD,
       b.PRIMARY_FLG                                     AS PRIMARY_FLAG,
       b.ACTIVE_FLG                                      AS ACTIVE_FLAG,
       b.VALIDATED_FLG                                   AS VERIFIED_FLAG,
       b.VALIDATED_DT                                    AS VERIFIED_DT,
       CASE
           WHEN NVL(b.VALIDATED_FLG, 'N') = 'N'                        THEN 'UNVERIFIED'
           WHEN b.VALIDATED_DT < ADD_MONTHS(TRUNC(SYSDATE), -24)       THEN 'REVERIFY_DUE'
           ELSE 'OK'
       END                                               AS VERIFICATION_STATUS_CD,
       NVL(b.UPDATED_DT, b.CREATED_DT)                   AS LAST_UPD_DT,
       NVL(b.UPDATED_BY, b.CREATED_BY)                   AS LAST_UPD_BY
  FROM WWI_MDM.SUPP_BANK_ACCOUNT b
  JOIN WWI_MDM.SUPP_MASTER s
    ON s.SUPP_ID = b.SUPP_ID
 WHERE NVL(b.ACTIVE_FLG, 'N') = 'Y'
/
