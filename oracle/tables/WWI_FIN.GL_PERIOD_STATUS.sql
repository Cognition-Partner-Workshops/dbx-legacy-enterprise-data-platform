/* =====================================================================
 * Object       : TABLE WWI_FIN.GL_PERIOD_STATUS
 * Schema       : WWI_FIN (Oracle ERP - WWIGERP)
 * Deploy order : 67
 * Depends on   : WWI_FIN.GL_ACCOUNT
 * Called by    : PKG_GL_POST (open-period check), month-end close checklist
 *
 * Open/closed status per ledger and accounting period. Each region runs its
 * own fiscal calendar (NA calendar year, EU calendar year with a 13th
 * adjustment period, APAC April-March), so the same PERIOD_CD does not mean
 * the same date range across ledgers - the date range on the row is
 * authoritative and the code is not.
 * ===================================================================== */

CREATE TABLE WWI_FIN.GL_PERIOD_STATUS
(
    LEDGER_CD               VARCHAR2(8)     NOT NULL,
    PERIOD_CD               VARCHAR2(7)     NOT NULL,
    FISCAL_YEAR_NBR         NUMBER(4)       NOT NULL,
    PERIOD_NBR              NUMBER(2)       NOT NULL,
    REGION_CD               VARCHAR2(4)     NOT NULL,
    PERIOD_START_DT         DATE            NOT NULL,
    PERIOD_END_DT           DATE            NOT NULL,
    ADJUSTMENT_PERIOD_FLG   VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    AP_STATUS_CD            VARCHAR2(4)     DEFAULT 'OPEN' NOT NULL,
    GL_STATUS_CD            VARCHAR2(4)     DEFAULT 'OPEN' NOT NULL,
    PO_STATUS_CD            VARCHAR2(4)     DEFAULT 'OPEN' NOT NULL,
    CLOSED_BY_CD            VARCHAR2(8),
    CLOSED_DT               DATE,
    REOPENED_CNT            NUMBER(3)       DEFAULT 0 NOT NULL,
    REOPEN_REASON_TXT       VARCHAR2(400),
    SOFT_CLOSE_DT           DATE,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_GL_PERIOD_STATUS PRIMARY KEY (LEDGER_CD, PERIOD_CD) USING INDEX TABLESPACE WWI_FIN_IDX,
    CONSTRAINT CK_GL_PERIOD_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC')),
    CONSTRAINT CK_GL_PERIOD_NBR CHECK (PERIOD_NBR BETWEEN 1 AND 13),
    CONSTRAINT CK_GL_PERIOD_AP CHECK (AP_STATUS_CD IN ('OPEN', 'CLSD', 'PERM', 'FUTR')),
    CONSTRAINT CK_GL_PERIOD_GL CHECK (GL_STATUS_CD IN ('OPEN', 'CLSD', 'PERM', 'FUTR')),
    CONSTRAINT CK_GL_PERIOD_PO CHECK (PO_STATUS_CD IN ('OPEN', 'CLSD', 'PERM', 'FUTR')),
    CONSTRAINT CK_GL_PERIOD_DATES CHECK (PERIOD_END_DT >= PERIOD_START_DT),
    CONSTRAINT CK_GL_PERIOD_ADJ CHECK (ADJUSTMENT_PERIOD_FLG IN ('Y', 'N'))
)
ORGANIZATION INDEX
TABLESPACE WWI_FIN_DATA
/

CREATE INDEX WWI_FIN.IX_GL_PERIOD_RANGE
    ON WWI_FIN.GL_PERIOD_STATUS (PERIOD_START_DT, PERIOD_END_DT) TABLESPACE WWI_FIN_IDX
/

COMMENT ON COLUMN WWI_FIN.GL_PERIOD_STATUS.REOPENED_CNT IS
    'Number of times a closed period was reopened. Audit asks about anything above 1.'
/
