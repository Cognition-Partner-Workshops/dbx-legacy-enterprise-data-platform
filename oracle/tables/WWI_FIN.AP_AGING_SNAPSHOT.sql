/* =====================================================================
 * Object       : TABLE WWI_FIN.AP_AGING_SNAPSHOT
 * Schema       : WWI_FIN (Oracle ERP - WWIGERP)
 * Deploy order : 75
 * Depends on   : WWI_FIN.AP_INVOICE_HDR, WWI_FIN.AP_PAYMENT_APPLY
 * Called by    : Month-end aging batch, treasury reporting extract
 *
 * Point-in-time AP aging, one row per supplier per snapshot per currency. The
 * buckets are hard-coded columns and the bucket boundaries differ by region
 * (NA 30/60/90/120, EU 30/60/90 plus a statutory late-payment bucket, APAC
 * 15/30/45/60), yet the same four bucket columns are reused - so BUCKET_3_AMT
 * means a different age range depending on the region on the row.
 * ===================================================================== */

CREATE SEQUENCE WWI_FIN.SEQ_AP_AGING_SNAPSHOT
    START WITH 400001 INCREMENT BY 1 CACHE 100 NOCYCLE
/

CREATE TABLE WWI_FIN.AP_AGING_SNAPSHOT
(
    SNAPSHOT_ID             NUMBER(12)      NOT NULL,
    SNAPSHOT_DT             DATE            NOT NULL,
    PERIOD_CD               VARCHAR2(7)     NOT NULL,
    SUPP_ID                 NUMBER(12)      NOT NULL,
    REGION_CD               VARCHAR2(4)     NOT NULL,
    LEGAL_ENTITY_CD         VARCHAR2(6)     NOT NULL,
    BALANCE_CURR_CD         VARCHAR2(3)     NOT NULL,
    CURRENT_AMT             NUMBER(15,5)    DEFAULT 0 NOT NULL,
    BUCKET_1_AMT            NUMBER(15,5)    DEFAULT 0 NOT NULL,
    BUCKET_2_AMT            NUMBER(15,5)    DEFAULT 0 NOT NULL,
    BUCKET_3_AMT            NUMBER(15,5)    DEFAULT 0 NOT NULL,
    BUCKET_4_AMT            NUMBER(15,5)    DEFAULT 0 NOT NULL,
    TOTAL_OUTSTANDING_AMT   NUMBER(15,5)    DEFAULT 0 NOT NULL,
    DISPUTED_AMT            NUMBER(15,5)    DEFAULT 0 NOT NULL,
    ON_HOLD_AMT             NUMBER(15,5)    DEFAULT 0 NOT NULL,
    OPEN_INVOICE_CNT        NUMBER(7)       DEFAULT 0 NOT NULL,
    OLDEST_INVOICE_DT       DATE,
    AVG_DAYS_OUTSTANDING    NUMBER(7,2),
    BUCKET_DEFINITION_CD    VARCHAR2(10)    NOT NULL,
    FX_RATE_USED            NUMBER(18,8),
    REPORTING_AMT_USD       NUMBER(15,5),
    CALCULATED_DT           DATE            DEFAULT SYSDATE NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_AP_AGING_SNAPSHOT PRIMARY KEY (SNAPSHOT_ID) USING INDEX TABLESPACE WWI_FIN_IDX,
    CONSTRAINT UK_AP_AGING UNIQUE (SNAPSHOT_DT, SUPP_ID, BALANCE_CURR_CD)
        USING INDEX TABLESPACE WWI_FIN_IDX,
    CONSTRAINT CK_AP_AGING_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC')),
    CONSTRAINT CK_AP_AGING_BUCKETDEF CHECK (
        BUCKET_DEFINITION_CD IN ('NA_30_120', 'EU_30_90_ST', 'APAC_15_60'))
)
PARTITION BY RANGE (SNAPSHOT_DT)
INTERVAL (NUMTOYMINTERVAL(1, 'YEAR'))
(
    PARTITION AP_AGING_2022 VALUES LESS THAN (TO_DATE('2023-01-01', 'YYYY-MM-DD'))
        TABLESPACE WWI_HIST_DATA
)
/

ALTER TABLE WWI_FIN.AP_AGING_SNAPSHOT ADD CONSTRAINT FK_AP_AGING_SUPP
    FOREIGN KEY (SUPP_ID) REFERENCES WWI_MDM.SUPP_MASTER (SUPP_ID)
/

CREATE INDEX WWI_FIN.IX_AP_AGING_PERIOD
    ON WWI_FIN.AP_AGING_SNAPSHOT (PERIOD_CD, REGION_CD) TABLESPACE WWI_FIN_IDX
/

COMMENT ON COLUMN WWI_FIN.AP_AGING_SNAPSHOT.BUCKET_3_AMT IS
    'Age range depends on BUCKET_DEFINITION_CD; not comparable across regions.'
/
