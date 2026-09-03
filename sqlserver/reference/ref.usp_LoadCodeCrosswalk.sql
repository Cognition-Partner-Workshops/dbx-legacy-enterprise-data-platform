/*
    ref.usp_LoadCodeCrosswalk

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_CodeTranslation (SSIS), and by the individual
                    reference packages for a single domain
    Reads         : ref.StatusCode, ref.ReasonCode, ref.Region
    Writes        : ref.CodeCrosswalk, err.RejectedConstraintViolation
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    This is the table the whole reference layer turns on. Every code either
    source system has ever sent is mapped here onto the conformed value, per
    code domain, with the source system named and the mapping effective-dated.
    stg.usp_TranslateSourceCodes reads nothing else.

    Two source systems, and they do not agree about anything:

        ORA_ERP   one-letter statuses on the masters (A / I / H / P / X),
                  three-letter codes on documents (APPR, PREC, RECV, CANC),
                  numeric-ish cost centres and its own UOM spellings.
        WWI_OLTP  whole words, mixed case, with spaces (Under Development,
                  Backorder, Credit Note), and package-based units.

    Effective dating
    ----------------
    A mapping is never updated in place when the conformed value changes: the
    current row is closed on the day before the new one opens, so a fact
    restated for an old period still translates the way it did then. Changing
    only the description or the note does update in place, because nothing
    downstream keys on either.

    IsDefaultForConformed marks the one source code per conformed value that a
    reverse lookup should pick - the dimension publishing reads it when it needs
    to show "the" source code behind a conformed one.

    Region-specific mappings carry RegionCode. The same source code can mean
    different things in different regions: OLTP channel code 'DIR' is a direct
    sales force in NA and a distributor in APAC, and both rows exist.
*/

IF OBJECT_ID(N'ref.usp_LoadCodeCrosswalk', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_LoadCodeCrosswalk;
GO

CREATE PROCEDURE ref.usp_LoadCodeCrosswalk
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @CodeDomainCode     NVARCHAR(30) = NULL,
    @EffectiveFromDate  DATE = NULL,
    @MaintainedByName   NVARCHAR(100) = N'REF_Load_CodeTranslation'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'ref.CodeCrosswalk';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @UpdatedRows  BIGINT = 0;
    DECLARE @ClosedRows   BIGINT = 0;
    DECLARE @RejectedRows BIGINT = 0;

    SET @EffectiveFromDate = ISNULL(@EffectiveFromDate, CONVERT(DATE, SYSUTCDATETIME()));

    BEGIN TRY
        --  The steward grid. Held in the procedure because the spreadsheet it
        --  used to be reloaded from was never under source control.
        SELECT *
        INTO #CrosswalkGrid
        FROM
        (
            VALUES
                --  ORDER status ---------------------------------------------
                (N'ORDER',    N'ORA_ERP',  N'ENT',            N'Entered',                 N'OPEN',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'ORDER',    N'ORA_ERP',  N'BOOK',           N'Booked',                  N'OPEN',      CONVERT(NVARCHAR(10), NULL), 0),
                (N'ORDER',    N'ORA_ERP',  N'BO',             N'Backordered',             N'BACKORDER', CONVERT(NVARCHAR(10), NULL), 1),
                (N'ORDER',    N'ORA_ERP',  N'HOLD',           N'On hold',                 N'HOLD',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'ORDER',    N'ORA_ERP',  N'SHIP',           N'Shipped',                 N'SHIPPED',   CONVERT(NVARCHAR(10), NULL), 1),
                (N'ORDER',    N'ORA_ERP',  N'INV',            N'Invoiced',                N'INVOICED',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'ORDER',    N'ORA_ERP',  N'CANC',           N'Cancelled',               N'CANCELLED', CONVERT(NVARCHAR(10), NULL), 1),
                (N'ORDER',    N'WWI_OLTP', N'UNDER DEVELOPMENT', N'Under development',    N'DRAFT',     CONVERT(NVARCHAR(10), NULL), 1),
                (N'ORDER',    N'WWI_OLTP', N'OPEN',           N'Open',                    N'OPEN',      CONVERT(NVARCHAR(10), NULL), 0),
                (N'ORDER',    N'WWI_OLTP', N'BACKORDER',      N'Backorder',               N'BACKORDER', CONVERT(NVARCHAR(10), NULL), 0),
                (N'ORDER',    N'WWI_OLTP', N'PICKING',        N'Being picked',            N'PICKING',   CONVERT(NVARCHAR(10), NULL), 1),
                (N'ORDER',    N'WWI_OLTP', N'PICKED',         N'Picked',                  N'PICKING',   CONVERT(NVARCHAR(10), NULL), 0),
                (N'ORDER',    N'WWI_OLTP', N'INVOICED',       N'Invoiced',                N'INVOICED',  CONVERT(NVARCHAR(10), NULL), 0),
                (N'ORDER',    N'WWI_OLTP', N'CANCELLED',      N'Cancelled',               N'CANCELLED', CONVERT(NVARCHAR(10), NULL), 0),
                --  INVOICE status -------------------------------------------
                (N'INVOICE',  N'ORA_ERP',  N'DRAFT',          N'Draft',                   N'DRAFT',     CONVERT(NVARCHAR(10), NULL), 1),
                (N'INVOICE',  N'ORA_ERP',  N'APPR',           N'Approved and issued',     N'ISSUED',    CONVERT(NVARCHAR(10), NULL), 1),
                (N'INVOICE',  N'ORA_ERP',  N'PPAY',           N'Partially paid',          N'PARTPAID',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'INVOICE',  N'ORA_ERP',  N'PAID',           N'Paid in full',            N'PAID',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'INVOICE',  N'ORA_ERP',  N'DISP',           N'In dispute',              N'DISPUTED',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'INVOICE',  N'ORA_ERP',  N'WOFF',           N'Written off',             N'WRITEOFF',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'INVOICE',  N'ORA_ERP',  N'CANC',           N'Cancelled',               N'CANCELLED', CONVERT(NVARCHAR(10), NULL), 1),
                (N'INVOICE',  N'WWI_OLTP', N'ISSUED',         N'Issued',                  N'ISSUED',    CONVERT(NVARCHAR(10), NULL), 0),
                (N'INVOICE',  N'WWI_OLTP', N'PART PAID',      N'Part paid',               N'PARTPAID',  CONVERT(NVARCHAR(10), NULL), 0),
                (N'INVOICE',  N'WWI_OLTP', N'PAID',           N'Paid',                    N'PAID',      CONVERT(NVARCHAR(10), NULL), 0),
                (N'INVOICE',  N'WWI_OLTP', N'CREDITED',       N'Credited',                N'CREDITED',  CONVERT(NVARCHAR(10), NULL), 1),
                --  SHIPMENT status ------------------------------------------
                (N'SHIPMENT', N'WWI_OLTP', N'PLANNED',        N'Planned',                 N'PLANNED',   CONVERT(NVARCHAR(10), NULL), 1),
                (N'SHIPMENT', N'WWI_OLTP', N'IN TRANSIT',     N'In transit',              N'INTRANSIT', CONVERT(NVARCHAR(10), NULL), 1),
                (N'SHIPMENT', N'WWI_OLTP', N'DELIVERED',      N'Delivered',               N'DELIVERED', CONVERT(NVARCHAR(10), NULL), 1),
                (N'SHIPMENT', N'WWI_OLTP', N'FAILED',         N'Delivery failed',         N'FAILED',    CONVERT(NVARCHAR(10), NULL), 1),
                (N'SHIPMENT', N'WWI_OLTP', N'RTS',            N'Returned to sender',      N'RETURNED',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'SHIPMENT', N'ORA_ERP',  N'DLV',            N'Delivered',               N'DELIVERED', CONVERT(NVARCHAR(10), NULL), 0),
                --  PO status ------------------------------------------------
                (N'PO',       N'ORA_ERP',  N'INCOMPLETE',     N'Incomplete',              N'DRAFT',     CONVERT(NVARCHAR(10), NULL), 1),
                (N'PO',       N'ORA_ERP',  N'APPR',           N'Approved',                N'APPROVED',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'PO',       N'ORA_ERP',  N'PREC',           N'Partially received',      N'PARTRECV',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'PO',       N'ORA_ERP',  N'RECV',           N'Received',                N'RECEIVED',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'PO',       N'ORA_ERP',  N'CLSD',           N'Closed',                  N'CLOSED',    CONVERT(NVARCHAR(10), NULL), 1),
                (N'PO',       N'ORA_ERP',  N'CANC',           N'Cancelled',               N'CANCELLED', CONVERT(NVARCHAR(10), NULL), 1),
                --  SUPPLIER status ------------------------------------------
                (N'SUPPLIER', N'ORA_ERP',  N'A',              N'Active',                  N'ACTIVE',    CONVERT(NVARCHAR(10), NULL), 1),
                (N'SUPPLIER', N'ORA_ERP',  N'I',              N'Inactive',                N'INACTIVE',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'SUPPLIER', N'ORA_ERP',  N'H',              N'On hold',                 N'HOLD',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'SUPPLIER', N'ORA_ERP',  N'P',              N'Pending approval',        N'PENDING',   CONVERT(NVARCHAR(10), NULL), 1),
                (N'SUPPLIER', N'ORA_ERP',  N'X',              N'Blocked',                 N'BLOCKED',   CONVERT(NVARCHAR(10), NULL), 1),
                --  CUSTOMER status ------------------------------------------
                (N'CUSTOMER', N'ORA_ERP',  N'A',              N'Active',                  N'ACTIVE',    CONVERT(NVARCHAR(10), NULL), 1),
                (N'CUSTOMER', N'ORA_ERP',  N'I',              N'Inactive',                N'INACTIVE',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'CUSTOMER', N'ORA_ERP',  N'H',              N'Credit hold',             N'HOLD',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'CUSTOMER', N'ORA_ERP',  N'P',              N'Prospect',                N'PROSPECT',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'CUSTOMER', N'ORA_ERP',  N'X',              N'Account closed',          N'CLOSED',    CONVERT(NVARCHAR(10), NULL), 1),
                --  RETURN reason --------------------------------------------
                (N'RETURN',   N'WWI_OLTP', N'DAMAGED',        N'Damaged',                 N'DAMAGED',   CONVERT(NVARCHAR(10), NULL), 1),
                (N'RETURN',   N'WWI_OLTP', N'FAULTY',         N'Faulty',                  N'DEFECTIVE', CONVERT(NVARCHAR(10), NULL), 1),
                (N'RETURN',   N'WWI_OLTP', N'WRONG ITEM',     N'Wrong item',              N'WRONGITEM', CONVERT(NVARCHAR(10), NULL), 1),
                (N'RETURN',   N'WWI_OLTP', N'TOO MANY',       N'Too many delivered',      N'OVERSHIP',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'RETURN',   N'WWI_OLTP', N'NOT REQUIRED',   N'No longer required',      N'NOTNEEDED', CONVERT(NVARCHAR(10), NULL), 1),
                (N'RETURN',   N'WWI_OLTP', N'LATE',           N'Arrived late',            N'LATE',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'RETURN',   N'ORA_ERP',  N'RTN-DMG',        N'Damaged on receipt',      N'DAMAGED',   CONVERT(NVARCHAR(10), NULL), 0),
                (N'RETURN',   N'ORA_ERP',  N'RTN-QLY',        N'Quality failure',         N'DEFECTIVE', CONVERT(NVARCHAR(10), NULL), 0),
                --  CREDIT reason --------------------------------------------
                (N'CREDIT',   N'WWI_OLTP', N'PRICE',          N'Price adjustment',        N'PRICEERR',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'CREDIT',   N'WWI_OLTP', N'GOODWILL',       N'Goodwill',                N'GOODWILL',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'CREDIT',   N'WWI_OLTP', N'RETURN',         N'Against a return',        N'RETURNCR',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'CREDIT',   N'WWI_OLTP', N'SHORT',          N'Short delivery',          N'SHORTSHIP', CONVERT(NVARCHAR(10), NULL), 1),
                (N'CREDIT',   N'ORA_ERP',  N'CM-TAX',         N'Tax correction',          N'TAXCORR',   CONVERT(NVARCHAR(10), NULL), 1),
                --  HOLD reason ----------------------------------------------
                (N'HOLD',     N'ORA_ERP',  N'CRED',           N'Credit limit',            N'CREDIT',    CONVERT(NVARCHAR(10), NULL), 1),
                (N'HOLD',     N'ORA_ERP',  N'PRIC',           N'Price approval',          N'PRICE',     CONVERT(NVARCHAR(10), NULL), 1),
                (N'HOLD',     N'ORA_ERP',  N'QUAL',           N'Quality inspection',      N'QUALITY',   CONVERT(NVARCHAR(10), NULL), 1),
                (N'HOLD',     N'ORA_ERP',  N'COMP',           N'Compliance review',       N'COMPLIANCE', CONVERT(NVARCHAR(10), NULL), 1),
                --  ADJUSTMENT reason ----------------------------------------
                (N'ADJUSTMENT', N'WWI_OLTP', N'STOCKTAKE',    N'Stocktake correction',    N'CYCLECOUNT', CONVERT(NVARCHAR(10), NULL), 1),
                (N'ADJUSTMENT', N'WWI_OLTP', N'SHRINK',       N'Shrinkage',               N'SHRINKAGE', CONVERT(NVARCHAR(10), NULL), 1),
                (N'ADJUSTMENT', N'WWI_OLTP', N'DAMAGE',       N'Damaged in warehouse',    N'DAMAGE',    CONVERT(NVARCHAR(10), NULL), 1),
                (N'ADJUSTMENT', N'ORA_ERP',  N'REVAL',        N'Revaluation',             N'REVALUE',   CONVERT(NVARCHAR(10), NULL), 1),
                --  REJECT reason --------------------------------------------
                (N'REJECT',   N'ORA_ERP',  N'REJ-QLY',        N'Failed inspection',       N'QUALITY',   CONVERT(NVARCHAR(10), NULL), 1),
                (N'REJECT',   N'ORA_ERP',  N'REJ-QTY',        N'Quantity mismatch',       N'QUANTITY',  CONVERT(NVARCHAR(10), NULL), 1),
                (N'REJECT',   N'ORA_ERP',  N'REJ-DOC',        N'Documentation',           N'DOCUMENT',  CONVERT(NVARCHAR(10), NULL), 1),
                --  REGION ---------------------------------------------------
                (N'REGION',   N'ORA_ERP',  N'AMER',           N'Americas',                N'NA',        CONVERT(NVARCHAR(10), NULL), 1),
                (N'REGION',   N'ORA_ERP',  N'NA',             N'North America',           N'NA',        CONVERT(NVARCHAR(10), NULL), 0),
                (N'REGION',   N'ORA_ERP',  N'EMEA',           N'Europe, Middle East and Africa', N'EU', CONVERT(NVARCHAR(10), NULL), 1),
                (N'REGION',   N'ORA_ERP',  N'EU',             N'Europe',                  N'EU',        CONVERT(NVARCHAR(10), NULL), 0),
                (N'REGION',   N'ORA_ERP',  N'APJ',            N'Asia Pacific and Japan',  N'APAC',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'REGION',   N'ORA_ERP',  N'APAC',           N'Asia Pacific',            N'APAC',      CONVERT(NVARCHAR(10), NULL), 0),
                --  COUNTRY (only the codes the ERP sends in the wrong shape) -
                (N'COUNTRY',  N'ORA_ERP',  N'USA',            N'United States (ISO3)',    N'US',        CONVERT(NVARCHAR(10), NULL), 1),
                (N'COUNTRY',  N'ORA_ERP',  N'GBR',            N'United Kingdom (ISO3)',   N'GB',        CONVERT(NVARCHAR(10), NULL), 1),
                (N'COUNTRY',  N'ORA_ERP',  N'UK',             N'United Kingdom (legacy)', N'GB',        CONVERT(NVARCHAR(10), NULL), 0),
                (N'COUNTRY',  N'ORA_ERP',  N'DEU',            N'Germany (ISO3)',          N'DE',        CONVERT(NVARCHAR(10), NULL), 1),
                (N'COUNTRY',  N'ORA_ERP',  N'AUS',            N'Australia (ISO3)',        N'AU',        CONVERT(NVARCHAR(10), NULL), 1),
                (N'COUNTRY',  N'ORA_ERP',  N'JPN',            N'Japan (ISO3)',            N'JP',        CONVERT(NVARCHAR(10), NULL), 1),
                --  CURRENCY (the OLTP database has no currency master) -------
                (N'CURRENCY', N'WWI_OLTP', N'US$',            N'US dollar',               N'USD',       CONVERT(NVARCHAR(10), NULL), 1),
                (N'CURRENCY', N'WWI_OLTP', N'STG',            N'Pound sterling',          N'GBP',       CONVERT(NVARCHAR(10), NULL), 1),
                (N'CURRENCY', N'ORA_ERP',  N'DEM',            N'Deutsche mark (retired)', N'EUR',       CONVERT(NVARCHAR(10), NULL), 0),
                --  UOM ------------------------------------------------------
                (N'UOM',      N'ORA_ERP',  N'EACH',           N'Each',                    N'EA',        CONVERT(NVARCHAR(10), NULL), 1),
                (N'UOM',      N'ORA_ERP',  N'CASE',           N'Case',                    N'CS',        CONVERT(NVARCHAR(10), NULL), 1),
                (N'UOM',      N'ORA_ERP',  N'PALLET',         N'Pallet',                  N'PLT',       CONVERT(NVARCHAR(10), NULL), 1),
                (N'UOM',      N'ORA_ERP',  N'KILO',           N'Kilogram',                N'KG',        CONVERT(NVARCHAR(10), NULL), 1),
                (N'UOM',      N'ORA_ERP',  N'POUND',          N'Pound',                   N'LB',        N'NA', 1),
                (N'UOM',      N'WWI_OLTP', N'PACKET',         N'Packet',                  N'PKT',       CONVERT(NVARCHAR(10), NULL), 1),
                (N'UOM',      N'WWI_OLTP', N'EACH',           N'Each',                    N'EA',        CONVERT(NVARCHAR(10), NULL), 0),
                --  PAYMENT_METHOD -------------------------------------------
                (N'PAYMENT_METHOD', N'ORA_ERP',  N'CHECK',    N'Check',                   N'CHQ',       N'NA', 1),
                (N'PAYMENT_METHOD', N'ORA_ERP',  N'CHEQUE',   N'Cheque',                  N'CHQ',       N'EU', 0),
                (N'PAYMENT_METHOD', N'ORA_ERP',  N'EFT',      N'Electronic funds transfer', N'BANK',    CONVERT(NVARCHAR(10), NULL), 1),
                (N'PAYMENT_METHOD', N'ORA_ERP',  N'DD',       N'Direct debit',            N'DDEBIT',    N'EU', 1),
                (N'PAYMENT_METHOD', N'WWI_OLTP', N'CREDIT CARD', N'Credit card',          N'CARD',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'PAYMENT_METHOD', N'WWI_OLTP', N'CASH',     N'Cash',                    N'CASH',      CONVERT(NVARCHAR(10), NULL), 1),
                --  PAYMENT_TERMS --------------------------------------------
                (N'PAYMENT_TERMS', N'ORA_ERP',  N'NET30',     N'Net 30 days',             N'NET30',     CONVERT(NVARCHAR(10), NULL), 1),
                (N'PAYMENT_TERMS', N'ORA_ERP',  N'NET60',     N'Net 60 days',             N'NET60',     CONVERT(NVARCHAR(10), NULL), 1),
                (N'PAYMENT_TERMS', N'ORA_ERP',  N'2/10NET30', N'2 percent 10 days, net 30', N'DISC210', CONVERT(NVARCHAR(10), NULL), 1),
                (N'PAYMENT_TERMS', N'ORA_ERP',  N'EOM',       N'End of month',            N'EOM',       N'EU', 1),
                (N'PAYMENT_TERMS', N'WWI_OLTP', N'7 DAYS',    N'Seven days',              N'NET07',     CONVERT(NVARCHAR(10), NULL), 1),
                (N'PAYMENT_TERMS', N'WWI_OLTP', N'30 DAYS',   N'Thirty days',             N'NET30',     CONVERT(NVARCHAR(10), NULL), 0),
                --  TRANSACTION_TYPE -----------------------------------------
                (N'TRANSACTION_TYPE', N'WWI_OLTP', N'STOCK ISSUE',   N'Stock issue',      N'SALE',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'TRANSACTION_TYPE', N'WWI_OLTP', N'STOCK RECEIPT', N'Stock receipt',    N'PURCH',     CONVERT(NVARCHAR(10), NULL), 1),
                (N'TRANSACTION_TYPE', N'WWI_OLTP', N'ADJUSTMENT',    N'Adjustment',       N'ADJ',       CONVERT(NVARCHAR(10), NULL), 1),
                (N'TRANSACTION_TYPE', N'WWI_OLTP', N'CUSTOMER RETURN', N'Customer return', N'RET',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'TRANSACTION_TYPE', N'ORA_ERP',  N'MISC ISSUE',    N'Miscellaneous issue', N'ADJ',    CONVERT(NVARCHAR(10), NULL), 0),
                --  SALES_CHANNEL (the same code means different things) ------
                (N'SALES_CHANNEL', N'WWI_OLTP', N'DIR',       N'Direct sales force',      N'DIRECT',    N'NA', 1),
                (N'SALES_CHANNEL', N'WWI_OLTP', N'DIR',       N'Distributor',             N'PARTNER',   N'APAC', 0),
                (N'SALES_CHANNEL', N'WWI_OLTP', N'WEB',       N'Web store',               N'ONLINE',    CONVERT(NVARCHAR(10), NULL), 1),
                (N'SALES_CHANNEL', N'WWI_OLTP', N'PHONE',     N'Telesales',               N'TELE',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'SALES_CHANNEL', N'ORA_ERP',  N'WHSL',      N'Wholesale',               N'PARTNER',   CONVERT(NVARCHAR(10), NULL), 0),
                --  LOYALTY_TIER ---------------------------------------------
                (N'LOYALTY_TIER', N'WWI_OLTP', N'BRONZE',     N'Bronze',                  N'TIER1',     CONVERT(NVARCHAR(10), NULL), 1),
                (N'LOYALTY_TIER', N'WWI_OLTP', N'SILVER',     N'Silver',                  N'TIER2',     CONVERT(NVARCHAR(10), NULL), 1),
                (N'LOYALTY_TIER', N'WWI_OLTP', N'GOLD',       N'Gold',                    N'TIER3',     CONVERT(NVARCHAR(10), NULL), 1),
                (N'LOYALTY_TIER', N'WWI_OLTP', N'PLATINUM',   N'Platinum',                N'TIER4',     CONVERT(NVARCHAR(10), NULL), 1),
                --  CARRIER --------------------------------------------------
                (N'CARRIER',  N'WWI_OLTP', N'FEDEX',          N'FedEx',                   N'FDX',       N'NA', 1),
                (N'CARRIER',  N'WWI_OLTP', N'UPS',            N'United Parcel Service',   N'UPS',       N'NA', 1),
                (N'CARRIER',  N'WWI_OLTP', N'DHL',            N'DHL',                     N'DHL',       N'EU', 1),
                (N'CARRIER',  N'WWI_OLTP', N'OWN FLEET',      N'Own fleet',               N'OWN',       CONVERT(NVARCHAR(10), NULL), 1),
                (N'CARRIER',  N'ORA_ERP',  N'TOLL',           N'Toll Group',              N'TOLL',      N'APAC', 1),
                --  COST_CENTER ----------------------------------------------
                (N'COST_CENTER', N'ORA_ERP', N'1000',         N'Corporate',               N'CORP',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'COST_CENTER', N'ORA_ERP', N'2000',         N'Sales',                   N'SALES',     CONVERT(NVARCHAR(10), NULL), 1),
                (N'COST_CENTER', N'ORA_ERP', N'3000',         N'Warehouse operations',    N'OPS',       CONVERT(NVARCHAR(10), NULL), 1),
                (N'COST_CENTER', N'ORA_ERP', N'4000',         N'Logistics',               N'LOGI',      CONVERT(NVARCHAR(10), NULL), 1),
                (N'COST_CENTER', N'ORA_ERP', N'9999',         N'Suspense',                N'SUSP',      CONVERT(NVARCHAR(10), NULL), 1)
        ) AS v (CodeDomainCode, SourceSystemCode, SourceCodeValue, SourceCodeDescription,
                ConformedCodeValue, RegionCode, IsDefaultForConformed);

        SELECT @SourceRows = COUNT_BIG(*)
        FROM #CrosswalkGrid AS g
        WHERE @CodeDomainCode IS NULL
           OR g.CodeDomainCode = @CodeDomainCode;

        SELECT *
        INTO #CrosswalkScope
        FROM #CrosswalkGrid AS g
        WHERE @CodeDomainCode IS NULL
           OR g.CodeDomainCode = @CodeDomainCode;

        --  A status or reason mapping that points at a conformed value the
        --  conformed list does not hold would translate a code into nothing.
        INSERT INTO err.RejectedConstraintViolation
        (
            BatchId, PackageExecutionId, TargetObjectName, ConstraintName, ConstraintTypeCode,
            ViolatingBusinessKey, ViolatingColumnName, ViolatingValue, RejectReasonCode,
            RejectReason, RejectStage, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'ref.CodeCrosswalk', N'CK_refCodeCrosswalk_Conformed', N'FK',
            CONCAT(g.CodeDomainCode, N'|', g.SourceSystemCode, N'|', g.SourceCodeValue),
            N'ConformedCodeValue', g.ConformedCodeValue, N'CONSTRAINT',
            N'mapping points at a conformed value that is not in ref.StatusCode or ref.ReasonCode',
            N'Reference',
            CONCAT(N'{"DOMAIN":"', g.CodeDomainCode, N'","SOURCE":"', g.SourceCodeValue, N'"}')
        FROM #CrosswalkScope AS g
        WHERE
        (
            g.CodeDomainCode IN (N'ORDER', N'INVOICE', N'SHIPMENT', N'PO', N'SUPPLIER', N'CUSTOMER')
            AND NOT EXISTS
                (
                    SELECT 1
                    FROM ref.StatusCode AS s
                    WHERE s.StatusDomainCode    = g.CodeDomainCode
                      AND s.ConformedStatusCode = g.ConformedCodeValue
                )
        )
        OR
        (
            g.CodeDomainCode IN (N'RETURN', N'CREDIT', N'HOLD', N'ADJUSTMENT', N'REJECT')
            AND NOT EXISTS
                (
                    SELECT 1
                    FROM ref.ReasonCode AS r
                    WHERE r.ReasonDomainCode    = g.CodeDomainCode
                      AND r.ConformedReasonCode = g.ConformedCodeValue
                )
        );

        SET @RejectedRows = @@ROWCOUNT;

        BEGIN TRANSACTION;

        --  A conformed value that has changed closes the old mapping the day
        --  before the new one opens. Nothing is ever updated across that line.
        UPDATE x
        SET x.EffectiveToDate  = DATEADD(day, -1, @EffectiveFromDate),
            x.MaintenanceNote  = CONCAT(N'Superseded on ', CONVERT(NVARCHAR(10), @EffectiveFromDate),
                                        N' by a mapping onto ', g.ConformedCodeValue, N'.')
        FROM ref.CodeCrosswalk AS x
        INNER JOIN #CrosswalkScope AS g
            ON  g.CodeDomainCode   = x.CodeDomainCode
            AND g.SourceSystemCode = x.SourceSystemCode
            AND g.SourceCodeValue  = x.SourceCodeValue
            AND ISNULL(g.RegionCode, N'*') = ISNULL(x.RegionCode, N'*')
        WHERE x.EffectiveToDate IS NULL
          AND x.EffectiveFromDate < @EffectiveFromDate
          AND x.ConformedCodeValue <> g.ConformedCodeValue;

        SET @ClosedRows = @@ROWCOUNT;

        --  Description-only differences are corrected in place.
        UPDATE x
        SET x.SourceCodeDescription = g.SourceCodeDescription,
            x.IsDefaultForConformed = g.IsDefaultForConformed,
            x.MaintainedByName      = @MaintainedByName
        FROM ref.CodeCrosswalk AS x
        INNER JOIN #CrosswalkScope AS g
            ON  g.CodeDomainCode   = x.CodeDomainCode
            AND g.SourceSystemCode = x.SourceSystemCode
            AND g.SourceCodeValue  = x.SourceCodeValue
            AND ISNULL(g.RegionCode, N'*') = ISNULL(x.RegionCode, N'*')
        WHERE x.EffectiveToDate IS NULL
          AND x.ConformedCodeValue = g.ConformedCodeValue
          AND
          (
              ISNULL(x.SourceCodeDescription, N'') <> ISNULL(g.SourceCodeDescription, N'')
              OR x.IsDefaultForConformed <> g.IsDefaultForConformed
          );

        SET @UpdatedRows = @@ROWCOUNT;

        INSERT INTO ref.CodeCrosswalk
        (
            CodeDomainCode, SourceSystemCode, SourceCodeValue, SourceCodeDescription,
            ConformedCodeValue, RegionCode, IsDefaultForConformed, EffectiveFromDate,
            EffectiveToDate, MaintainedByName, MaintenanceNote
        )
        SELECT
            g.CodeDomainCode,
            g.SourceSystemCode,
            g.SourceCodeValue,
            g.SourceCodeDescription,
            g.ConformedCodeValue,
            g.RegionCode,
            g.IsDefaultForConformed,
            --  The first load of a mapping is open from the beginning of time,
            --  otherwise history would not translate.
            CASE
                WHEN EXISTS
                     (
                         SELECT 1
                         FROM ref.CodeCrosswalk AS p
                         WHERE p.CodeDomainCode   = g.CodeDomainCode
                           AND p.SourceSystemCode = g.SourceSystemCode
                           AND p.SourceCodeValue  = g.SourceCodeValue
                     )
                    THEN @EffectiveFromDate
                ELSE CONVERT(DATE, N'1900-01-01')
            END,
            NULL,
            @MaintainedByName,
            N'Loaded from the steward grid held in ref.usp_LoadCodeCrosswalk.'
        FROM #CrosswalkScope AS g
        WHERE NOT EXISTS
              (
                  SELECT 1
                  FROM ref.CodeCrosswalk AS x
                  WHERE x.CodeDomainCode   = g.CodeDomainCode
                    AND x.SourceSystemCode = g.SourceSystemCode
                    AND x.SourceCodeValue  = g.SourceCodeValue
                    AND ISNULL(x.RegionCode, N'*') = ISNULL(g.RegionCode, N'*')
                    AND x.EffectiveToDate IS NULL
              );

        SET @InsertedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows + @UpdatedRows,
            @InsertRowCount     = @InsertedRows,
            @UpdateRowCount     = @UpdatedRows + @ClosedRows,
            @RejectRowCount     = @RejectedRows;

        DROP TABLE #CrosswalkGrid;
        DROP TABLE #CrosswalkScope;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'REF_Load_CodeTranslation',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'ref.usp_LoadCodeCrosswalk';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
