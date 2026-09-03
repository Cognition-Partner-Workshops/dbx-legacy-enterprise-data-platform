/* =====================================================================
 * Object       : TABLE WWI_REF.CALENDAR_FISCAL
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 32
 * Depends on   : WWI_REF.REGION_REF
 * Called by    : PKG_GL_POST, period-based extracts, aging snapshots
 *
 * Fiscal calendar, one row per calendar day per calendar code. Three calendars
 * coexist: NA_CAL (calendar year, 12 periods), EU_CAL445 (4-4-5 weeks, 12
 * periods plus a 13th adjustment period), APAC_APR (April-March fiscal year).
 * The same date therefore belongs to three different periods and quarters
 * depending on the calendar, which is why period-based comparisons across
 * regions need the calendar code and usually do not have it.
 * ===================================================================== */

CREATE TABLE WWI_REF.CALENDAR_FISCAL
(
    CALENDAR_CD             VARCHAR2(10)    NOT NULL,
    CALENDAR_DT             DATE            NOT NULL,
    FISCAL_YEAR_NBR         NUMBER(4)       NOT NULL,
    FISCAL_QUARTER_NBR      NUMBER(1)       NOT NULL,
    FISCAL_PERIOD_NBR       NUMBER(2)       NOT NULL,
    PERIOD_CD               VARCHAR2(7)     NOT NULL,
    FISCAL_WEEK_NBR         NUMBER(2),
    DAY_OF_PERIOD_NBR       NUMBER(2),
    PERIOD_START_DT         DATE            NOT NULL,
    PERIOD_END_DT           DATE            NOT NULL,
    QUARTER_START_DT        DATE,
    QUARTER_END_DT          DATE,
    YEAR_START_DT           DATE,
    YEAR_END_DT             DATE,
    WORKING_DAY_FLG         VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    HOLIDAY_FLG             VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    HOLIDAY_NAME            VARCHAR2(80),
    HOLIDAY_COUNTRY_CD      VARCHAR2(2),
    ADJUSTMENT_PERIOD_FLG   VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    REGION_CD               VARCHAR2(4)     NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_CALENDAR_FISCAL PRIMARY KEY (CALENDAR_CD, CALENDAR_DT)
        USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_CALENDAR_QUARTER CHECK (FISCAL_QUARTER_NBR BETWEEN 1 AND 4),
    CONSTRAINT CK_CALENDAR_PERIOD CHECK (FISCAL_PERIOD_NBR BETWEEN 1 AND 13),
    CONSTRAINT CK_CALENDAR_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC')),
    CONSTRAINT CK_CALENDAR_FLAGS CHECK (
        WORKING_DAY_FLG IN ('Y', 'N') AND HOLIDAY_FLG IN ('Y', 'N')
        AND ADJUSTMENT_PERIOD_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_REF_DATA
/

CREATE INDEX WWI_REF.IX_CALENDAR_PERIOD
    ON WWI_REF.CALENDAR_FISCAL (CALENDAR_CD, PERIOD_CD) TABLESPACE WWI_IDX
/

CREATE INDEX WWI_REF.IX_CALENDAR_WORKING
    ON WWI_REF.CALENDAR_FISCAL (CALENDAR_DT, WORKING_DAY_FLG) TABLESPACE WWI_IDX
/
