/* ============================================================================
 * Object      : WWI_MDM.V_SUPPLIER_BANK_MASKED (view)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.SUPP_BANK_ACCOUNT, WWI_MDM.SUPP_MASTER,
 *               WWI_REF.COUNTRY_REF
 * Called by   : WWI_FIN.PKG_AP_PAYMENT, treasury reporting, and the supplier
 *               statement export (PRC_Export_SupplierStatement)
 * History     : 2009 created after the audit finding that the payment run
 *               report printed full account numbers.
 * Notes       : Masking rules differ by region because the auditors asked for
 *               different things in each jurisdiction. This view is the only
 *               approved way to read bank details outside the payment package.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_MDM.V_SUPPLIER_BANK_MASKED AS
SELECT b.BANK_ACCT_ID,
       b.SUPP_ID,
       s.SUPP_NUM,
       s.SUPP_NAME,
       s.REGION_CD,
       b.BANK_NAME,
       CASE s.REGION_CD
           WHEN 'EU'   THEN SUBSTR(b.IBAN_MASKED, 1, 4) || RPAD('*', 12, '*')
                            || SUBSTR(b.IBAN_MASKED, -4)
           WHEN 'APAC' THEN RPAD('*', 8, '*') || SUBSTR(b.ACCT_NUM_MASKED, -3)
           ELSE RPAD('*', 6, '*') || SUBSTR(b.ACCT_NUM_MASKED, -4)
       END                                              AS ACCT_DISPLAY_TXT,
       CASE WHEN s.REGION_CD = 'EU' THEN b.SWIFT_CD ELSE NULL END AS SWIFT_CD,
       b.CURRENCY_CD,
       b.ACTIVE_FLAG,
       b.VERIFIED_FLAG,
       b.VERIFIED_DT,
       CASE
           WHEN NVL(b.VERIFIED_FLAG, 'N') = 'N'                       THEN 'UNVERIFIED'
           WHEN b.VERIFIED_DT < ADD_MONTHS(TRUNC(SYSDATE), -24)       THEN 'REVERIFY_DUE'
           ELSE 'OK'
       END                                              AS VERIFICATION_STATUS_CD,
       b.LAST_UPD_DT,
       b.LAST_UPD_BY
  FROM WWI_MDM.SUPP_BANK_ACCOUNT b
  JOIN WWI_MDM.SUPP_MASTER s
    ON s.SUPP_ID = b.SUPP_ID
 WHERE NVL(b.ACTIVE_FLAG, 'N') = 'Y'
/
