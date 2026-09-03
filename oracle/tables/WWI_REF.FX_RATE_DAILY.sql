/* =====================================================================
 * Object       : TABLE WWI_REF.FX_RATE_DAILY
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 23
 * Depends on   : WWI_REF.CURRENCY_CODE
 * Called by    : FN_CONVERT_CURRENCY, PKG_GL_POST, reporting extracts
 *
 * Daily FX rates by rate type. Three regional treasury feeds load this table
 * with different conventions: NA loads a corporate rate at month start and
 * repeats it every day, EU loads the ECB daily reference rate, APAC loads a
 * bank rate with a bid/ask spread and stores the mid in RATE. Weekend and
 * holiday gaps are filled by NA and EU but not by APAC, so a lookup for a
 * Sunday in APAC returns nothing and the caller falls back to the last
 * available rate - or to 1.0 in the two places that forgot to.
 * ===================================================================== */

CREATE TABLE WWI_REF.FX_RATE_DAILY
(
    FROM_CURR_CD            VARCHAR2(3)     NOT NULL,
    TO_CURR_CD              VARCHAR2(3)     NOT NULL,
    RATE_DT                 DATE            NOT NULL,
    RATE_TYPE_CD            VARCHAR2(6)     NOT NULL,
    RATE                    NUMBER(18,8)    NOT NULL,
    INVERSE_RATE            NUMBER(18,8),
    BID_RATE                NUMBER(18,8),
    ASK_RATE                NUMBER(18,8),
    RATE_SOURCE_CD          VARCHAR2(12)    NOT NULL,
    FEED_REGION_CD          VARCHAR2(4),
    INTERPOLATED_FLG        VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    LOADED_TS               TIMESTAMP(6)    DEFAULT SYSTIMESTAMP NOT NULL,
    SUPERSEDED_FLG          VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_FX_RATE_DAILY PRIMARY KEY (FROM_CURR_CD, TO_CURR_CD, RATE_DT, RATE_TYPE_CD)
        USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_FX_RATE_POSITIVE CHECK (RATE > 0),
    CONSTRAINT CK_FX_RATE_TYPE CHECK (RATE_TYPE_CD IN ('CORP', 'SPOT', 'ECB', 'BANK', 'BUDGET')),
    CONSTRAINT CK_FX_RATE_FLAGS CHECK (
        INTERPOLATED_FLG IN ('Y', 'N') AND SUPERSEDED_FLG IN ('Y', 'N')),
    CONSTRAINT CK_FX_RATE_SELF CHECK (FROM_CURR_CD <> TO_CURR_CD)
)
PARTITION BY RANGE (RATE_DT)
INTERVAL (NUMTOYMINTERVAL(1, 'YEAR'))
(
    PARTITION FX_RATE_2019 VALUES LESS THAN (TO_DATE('2020-01-01', 'YYYY-MM-DD'))
        TABLESPACE WWI_HIST_DATA
)
/

CREATE INDEX WWI_REF.IX_FX_RATE_LOOKUP
    ON WWI_REF.FX_RATE_DAILY (RATE_DT, FROM_CURR_CD, TO_CURR_CD) LOCAL TABLESPACE WWI_IDX
/

CREATE INDEX WWI_REF.IX_FX_RATE_SOURCE
    ON WWI_REF.FX_RATE_DAILY (RATE_SOURCE_CD, RATE_DT) LOCAL TABLESPACE WWI_IDX
/

COMMENT ON COLUMN WWI_REF.FX_RATE_DAILY.INVERSE_RATE IS
    'Stored, not derived. A handful of legacy rows are not the true reciprocal of RATE.'
/
