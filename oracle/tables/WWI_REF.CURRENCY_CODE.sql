/* =====================================================================
 * Object       : TABLE WWI_REF.CURRENCY_CODE
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 22
 * Depends on   : WWI_REF.COUNTRY_REF
 * Called by    : FN_CONVERT_CURRENCY, every amount-bearing extract
 *
 * Currency reference including the pre-euro national currencies, which are
 * inactive but still referenced by pre-2002 history. MINOR_UNIT_DIGITS is used
 * for rounding and is wrong for the zero-decimal currencies in the oldest
 * rows, which is why JPY amounts occasionally carry decimals.
 * ===================================================================== */

CREATE TABLE WWI_REF.CURRENCY_CODE
(
    CURR_CD                 VARCHAR2(3)     NOT NULL,
    CURR_NUM_CD             VARCHAR2(3),
    CURR_NAME               VARCHAR2(80)    NOT NULL,
    CURR_SYMBOL             VARCHAR2(6),
    MINOR_UNIT_DIGITS       NUMBER(1)       DEFAULT 2 NOT NULL,
    ROUNDING_RULE_CD        VARCHAR2(8)     DEFAULT 'HALFUP' NOT NULL,
    PRIMARY_COUNTRY_CD      VARCHAR2(2),
    REGION_CD               VARCHAR2(4),
    EURO_LEGACY_FLG         VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    EURO_FIXED_RATE         NUMBER(18,8),
    EURO_CONVERSION_DT      DATE,
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    TRADING_ALLOWED_FLG     VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    DISPLAY_SEQ_NBR         NUMBER(4),
    RETIRED_DT              DATE,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_CURRENCY_CODE PRIMARY KEY (CURR_CD) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_CURRENCY_MINOR CHECK (MINOR_UNIT_DIGITS BETWEEN 0 AND 4),
    CONSTRAINT CK_CURRENCY_ROUNDING CHECK (
        ROUNDING_RULE_CD IN ('HALFUP', 'HALFEVEN', 'DOWN', 'UP')),
    CONSTRAINT CK_CURRENCY_EURO CHECK (
        EURO_LEGACY_FLG IN ('Y', 'N')
        AND (EURO_LEGACY_FLG = 'N' OR EURO_FIXED_RATE IS NOT NULL)),
    CONSTRAINT CK_CURRENCY_FLAGS CHECK (ACTIVE_FLG IN ('Y', 'N') AND TRADING_ALLOWED_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_REF_DATA
/

CREATE INDEX WWI_REF.IX_CURRENCY_ACTIVE
    ON WWI_REF.CURRENCY_CODE (ACTIVE_FLG, DISPLAY_SEQ_NBR) TABLESPACE WWI_IDX
/
