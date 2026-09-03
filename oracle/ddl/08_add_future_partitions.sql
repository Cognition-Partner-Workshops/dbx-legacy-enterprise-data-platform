/* =====================================================================
 * Object       : Partition maintenance for the range-partitioned ERP tables
 * Schema       : WWI_PROC, WWI_FIN, WWI_REF, WWI_AUDIT
 * Deploy order : 08  - after oracle/tables/* have been created
 * Depends on   : oracle/tables/WWI_PROC.PURCHASE_ORDER_HDR.sql and the other
 *                partitioned tables listed below
 * Called by    : DBA deployment runbook, and again every December when the
 *                next calendar year's partitions are cut
 *
 * The partitioned tables are created with partitions up to the end of 2024
 * plus a PMAX catch-all. This script splits PMAX to add the following year.
 * It is run by hand. Twice in the estate's history it was forgotten and the
 * whole year landed in PMAX, which is why PMAX is still large on
 * AP_INVOICE_HDR.
 * ===================================================================== */

/* --- procurement ---------------------------------------------------- */
ALTER TABLE WWI_PROC.PURCHASE_ORDER_HDR
    SPLIT PARTITION PO_HDR_PMAX AT (TO_DATE('2026-01-01', 'YYYY-MM-DD'))
    INTO (PARTITION PO_HDR_2025 TABLESPACE WWI_HIST_DATA, PARTITION PO_HDR_PMAX)
/

ALTER TABLE WWI_PROC.PURCHASE_ORDER_LINE
    SPLIT PARTITION PO_LINE_PMAX AT (TO_DATE('2026-01-01', 'YYYY-MM-DD'))
    INTO (PARTITION PO_LINE_2025 TABLESPACE WWI_HIST_DATA, PARTITION PO_LINE_PMAX)
/

ALTER TABLE WWI_PROC.PO_RECEIPT_LINE
    SPLIT PARTITION RCPT_LINE_PMAX AT (TO_DATE('2026-01-01', 'YYYY-MM-DD'))
    INTO (PARTITION RCPT_LINE_2025 TABLESPACE WWI_HIST_DATA, PARTITION RCPT_LINE_PMAX)
/

/* --- finance -------------------------------------------------------- */
ALTER TABLE WWI_FIN.AP_INVOICE_HDR
    SPLIT PARTITION AP_HDR_PMAX AT (202601)
    INTO (PARTITION AP_HDR_2025 TABLESPACE WWI_HIST_DATA, PARTITION AP_HDR_PMAX)
/

ALTER TABLE WWI_FIN.AP_INVOICE_LINE
    SPLIT PARTITION AP_LINE_PMAX AT (202601)
    INTO (PARTITION AP_LINE_2025 TABLESPACE WWI_HIST_DATA, PARTITION AP_LINE_PMAX)
/

ALTER TABLE WWI_FIN.GL_JOURNAL_LINE
    SPLIT PARTITION GLL_PMAX AT (202601)
    INTO (PARTITION GLL_2025 TABLESPACE WWI_HIST_DATA, PARTITION GLL_PMAX)
/

ALTER TABLE WWI_FIN.AP_AGING_SNAPSHOT
    SPLIT PARTITION AGE_PMAX AT (TO_DATE('2026-01-01', 'YYYY-MM-DD'))
    INTO (PARTITION AGE_2025 TABLESPACE WWI_HIST_DATA, PARTITION AGE_PMAX)
/

/* --- reference and audit -------------------------------------------- */
ALTER TABLE WWI_REF.FX_RATE_DAILY
    SPLIT PARTITION FX_PMAX AT (TO_DATE('2026-01-01', 'YYYY-MM-DD'))
    INTO (PARTITION FX_2025 TABLESPACE WWI_REF_DATA, PARTITION FX_PMAX)
/

ALTER TABLE WWI_AUDIT.CHANGE_LOG
    SPLIT PARTITION CHG_PMAX AT (TO_DATE('2026-01-01', 'YYYY-MM-DD'))
    INTO (PARTITION CHG_2025 TABLESPACE WWI_AUDIT_DATA, PARTITION CHG_PMAX)
/

/* Local indexes on the split partitions come back UNUSABLE on some releases;
   the runbook rebuilds them unconditionally. */
ALTER INDEX WWI_PROC.IX_PO_HDR_SUPPLIER   REBUILD
/
ALTER INDEX WWI_FIN.IX_AP_HDR_SUPPLIER    REBUILD
/
ALTER INDEX WWI_AUDIT.IX_CHANGE_LOG_TABLE REBUILD
/
