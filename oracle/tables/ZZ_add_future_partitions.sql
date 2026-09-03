/* =====================================================================
 * Object       : Partition maintenance for the range-partitioned ERP tables
 * Schema       : WWI_PROC, WWI_FIN
 * Deploy order : not deployed - hand-run maintenance, see below
 * Depends on   : oracle/tables/WWI_PROC.PURCHASE_ORDER_HDR.sql and the other
 *                partitioned tables listed below
 * Called by    : DBA deployment runbook, every December when the next calendar
 *                year's partitions are cut
 *
 * This is a handwritten maintenance runbook, not part of the estate build. The
 * deployment drivers skip it (deployment/oracle/Deploy-Oracle.ps1 and
 * deploy-oracle.sh exclude ZZ_*), because the tables it maintains already carry
 * partitions covering the estate's data and a PMAX catch-all.
 *
 * Only the fixed-range tables are listed. AP_AGING_SNAPSHOT, FX_RATE_DAILY and
 * CHANGE_LOG are INTERVAL partitioned: Oracle cuts their partitions on first
 * insert past the high bound, they have no PMAX tail, and splitting them
 * raises ORA-14080. Nothing has to be run for them.
 *
 * Splitting PMAX is the only supported way to add a year: the boundary is a
 * DATE, so it has to be a DATE expression, and the tail partition keeps the
 * PMAX name so next year's run finds it. UPDATE INDEXES keeps the local and
 * global indexes usable in the same statement rather than rebuilding them
 * blind afterwards.
 *
 * To cut a further year, copy a block and move both dates forward.
 * ===================================================================== */

/* --- procurement ---------------------------------------------------- */
ALTER TABLE WWI_PROC.PURCHASE_ORDER_HDR
    SPLIT PARTITION PO_HDR_PMAX AT (TO_DATE('2026-01-01', 'YYYY-MM-DD'))
    INTO (PARTITION PO_HDR_2025 TABLESPACE WWI_DATA, PARTITION PO_HDR_PMAX)
    UPDATE INDEXES
/

ALTER TABLE WWI_PROC.PURCHASE_ORDER_LINE
    SPLIT PARTITION PO_LINE_PMAX AT (TO_DATE('2026-01-01', 'YYYY-MM-DD'))
    INTO (PARTITION PO_LINE_2025 TABLESPACE WWI_DATA, PARTITION PO_LINE_PMAX)
    UPDATE INDEXES
/

ALTER TABLE WWI_PROC.PO_RECEIPT_LINE
    SPLIT PARTITION RCPT_LINE_PMAX AT (TO_DATE('2026-01-01', 'YYYY-MM-DD'))
    INTO (PARTITION RCPT_LINE_2025 TABLESPACE WWI_DATA, PARTITION RCPT_LINE_PMAX)
    UPDATE INDEXES
/

/* --- finance -------------------------------------------------------- */
ALTER TABLE WWI_FIN.AP_INVOICE_HDR
    SPLIT PARTITION AP_HDR_PMAX AT (TO_DATE('2026-01-01', 'YYYY-MM-DD'))
    INTO (PARTITION AP_HDR_2025 TABLESPACE WWI_FIN_DATA, PARTITION AP_HDR_PMAX)
    UPDATE INDEXES
/

ALTER TABLE WWI_FIN.AP_INVOICE_LINE
    SPLIT PARTITION AP_LINE_PMAX AT (TO_DATE('2026-01-01', 'YYYY-MM-DD'))
    INTO (PARTITION AP_LINE_2025 TABLESPACE WWI_FIN_DATA, PARTITION AP_LINE_PMAX)
    UPDATE INDEXES
/

ALTER TABLE WWI_FIN.GL_JOURNAL_LINE
    SPLIT PARTITION GL_JLINE_PMAX AT (TO_DATE('2026-01-01', 'YYYY-MM-DD'))
    INTO (PARTITION GL_JLINE_2025 TABLESPACE WWI_FIN_DATA, PARTITION GL_JLINE_PMAX)
    UPDATE INDEXES
/
