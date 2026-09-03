/*
    Seed          : [etl].[DataQualityRule]
    Deploy target : WWI_Staging
    Deploy order  : after 04_tables_data_quality.sql
    Read by       : etl.usp_EvaluateDataQualityRules and the DQ_* SSIS packages

    The starting rule set the stewards inherited. Each RuleExpression is the
    WHERE clause that selects the *offending* rows, so a clean object measures
    zero and ThresholdValue is the number of bad rows tolerated before the rule
    is considered failed.

    The thresholds are not uniform and are not meant to be. Several of them are
    the number that was breaching on the day the rule was turned on, raised just
    far enough to stop paging the on-call, which is why the EU tax rules tolerate
    more than the NA ones and why the APAC address rules tolerate the most.
*/

SET NOCOUNT ON;
GO

MERGE etl.DataQualityRule AS tgt
USING
(
    VALUES
    -- Customer screen -------------------------------------------------------
    (N'CUST_NULL_NAME',      N'CUSTOMER',  N'stg.Customer',            N'Customer name must be present',
     N'CustomerName IS NULL OR LTRIM(RTRIM(CustomerName)) = N''''',                      N'Completeness', N'FAIL',    0,     NULL,  NULL),
    (N'CUST_DUP_BKEY',       N'CUSTOMER',  N'stg.Customer',            N'Business key must be unique in the batch',
     N'CustomerBusinessKey IN (SELECT CustomerBusinessKey FROM stg.Customer GROUP BY CustomerBusinessKey HAVING COUNT_BIG(*) > 1)',
                                                                                         N'Uniqueness',   N'FAIL',    0,     NULL,  NULL),
    (N'CUST_NO_CATEGORY',    N'CUSTOMER',  N'stg.Customer',            N'Category must resolve to the conformed set',
     N'CustomerCategoryCode IS NULL',                                                    N'Validity',     N'WARN',    250,   NULL,  NULL),
    (N'CUST_BAD_POSTCODE_NA',N'CUSTOMER',  N'stg.Customer',            N'US and CA postal codes must match the country format',
     N'CountryCode IN (N''US'', N''CA'') AND PostalCodeIsValid = 0',                     N'Validity',     N'WARN',    50,    N'NA', NULL),
    (N'CUST_BAD_POSTCODE_EU',N'CUSTOMER',  N'stg.Customer',            N'EU postal codes must match the country format',
     N'RegionCode = N''EU'' AND PostalCodeIsValid = 0',                                  N'Validity',     N'WARN',    400,   N'EU', NULL),
    (N'CUST_BAD_POSTCODE_AP',N'CUSTOMER',  N'stg.Customer',            N'APAC postal codes must match the country format',
     N'RegionCode = N''APAC'' AND PostalCodeIsValid = 0',                                N'Validity',     N'WARN',    2000,  N'APAC', NULL),
    (N'CUST_FUTURE_OPENED',  N'CUSTOMER',  N'stg.Customer',            N'Account opened date cannot be in the future',
     N'AccountOpenedDate > CAST(SYSUTCDATETIME() AS DATE)',                              N'Validity',     N'FAIL',    0,     NULL,  N'ORA_ERP'),

    -- Supplier screen -------------------------------------------------------
    (N'SUPP_NULL_NAME',      N'SUPPLIER',  N'stg.Supplier',            N'Supplier name must be present',
     N'SupplierName IS NULL OR LTRIM(RTRIM(SupplierName)) = N''''',                      N'Completeness', N'FAIL',    0,     NULL,  NULL),
    (N'SUPP_NO_TAXID_EU',    N'SUPPLIER',  N'stg.Supplier',            N'EU suppliers must carry a VAT registration',
     N'RegionCode = N''EU'' AND (TaxRegistrationNumber IS NULL OR LEN(TaxRegistrationNumber) < 8)',
                                                                                         N'Completeness', N'FAIL',    5,     N'EU', NULL),
    (N'SUPP_NO_PAYMENT_TERM',N'SUPPLIER',  N'stg.Supplier',            N'Payment terms must resolve',
     N'PaymentTermsCode IS NULL',                                                        N'Validity',     N'WARN',    25,    NULL,  NULL),

    -- Order line screen -----------------------------------------------------
    (N'OL_NEG_QTY',          N'ORDERLINE', N'stg.OrderLine',           N'Ordered quantity must be positive',
     N'Quantity <= 0',                                                                   N'Validity',     N'FAIL',    0,     NULL,  NULL),
    (N'OL_NEG_PRICE',        N'ORDERLINE', N'stg.OrderLine',           N'Unit price must not be negative',
     N'UnitPrice < 0',                                                                   N'Validity',     N'FAIL',    0,     NULL,  NULL),
    (N'OL_ORPHAN_ORDER',     N'ORDERLINE', N'stg.OrderLine',           N'Every line must have a header',
     N'NOT EXISTS (SELECT 1 FROM stg.[Order] AS o WHERE o.OrderBusinessKey = stg.OrderLine.OrderBusinessKey)',
                                                                                         N'Integrity',    N'FAIL',    0,     NULL,  NULL),
    (N'OL_DISCOUNT_RANGE',   N'ORDERLINE', N'stg.OrderLine',           N'Discount percentage must be between 0 and 90',
     N'DiscountPercentage < 0 OR DiscountPercentage > 90',                               N'Validity',     N'WARN',    10,    NULL,  NULL),
    (N'OL_EXTENDED_MISMATCH',N'ORDERLINE', N'stg.OrderLine',           N'Extended amount must agree with quantity times net price',
     N'ABS(ExtendedAmount - (Quantity * UnitPrice * (1 - DiscountPercentage / 100.0))) > 0.01',
                                                                                         N'Accuracy',     N'WARN',    100,   NULL,  NULL),

    -- Invoice line screen ---------------------------------------------------
    (N'IL_NO_TAX_RATE',      N'INVOICE',   N'stg.InvoiceLine',         N'Tax rate must resolve from the jurisdiction',
     N'TaxRate IS NULL',                                                                 N'Completeness', N'FAIL',    0,     NULL,  NULL),
    (N'IL_TAX_MISMATCH_NA',  N'INVOICE',   N'stg.InvoiceLine',         N'NA sales tax must agree with the rate applied',
     N'RegionCode = N''NA'' AND ABS(TaxAmount - (ExtendedAmount * TaxRate / 100.0)) > 0.02',
                                                                                         N'Accuracy',     N'FAIL',    0,     N'NA', NULL),
    (N'IL_TAX_MISMATCH_EU',  N'INVOICE',   N'stg.InvoiceLine',         N'EU VAT must agree unless reverse charged',
     N'RegionCode = N''EU'' AND ReverseChargeFlag = 0 AND ABS(TaxAmount - (ExtendedAmount * TaxRate / 100.0)) > 0.02',
                                                                                         N'Accuracy',     N'WARN',    75,    N'EU', NULL),
    (N'IL_MARGIN_NEGATIVE',  N'INVOICE',   N'stg.InvoiceLine',         N'Margin below cost needs review',
     N'ExtendedAmount < CostAmount',                                                     N'Plausibility', N'WARN',    500,   NULL,  NULL),

    -- Payment screen --------------------------------------------------------
    (N'PAY_UNAPPLIED',       N'PAYMENT',   N'stg.SupplierPayment',     N'Payments must be applied to an invoice',
     N'InvoiceBusinessKey IS NULL',                                                      N'Integrity',    N'WARN',    40,    NULL,  NULL),
    (N'PAY_FX_MISSING',      N'PAYMENT',   N'stg.SupplierPayment',     N'Non-USD payments need an FX rate for the value date',
     N'CurrencyCode <> N''USD'' AND FxRateToUsd IS NULL',                                N'Completeness', N'FAIL',    0,     NULL,  NULL),
    (N'PAY_FUTURE_DATE',     N'PAYMENT',   N'stg.SupplierPayment',     N'Payment date cannot be in the future',
     N'PaymentDate > CAST(SYSUTCDATETIME() AS DATE)',                                    N'Validity',     N'FAIL',    0,     NULL,  N'ORA_ERP'),

    -- Referential screen ----------------------------------------------------
    (N'REF_UNMAPPED_CODE',   N'REFERENCE', N'ref.CodeCrosswalk',       N'Source codes must map to the conformed set',
     N'ConformedCode IS NULL',                                                           N'Integrity',    N'WARN',    30,    NULL,  NULL),
    (N'REF_FX_GAP',          N'REFERENCE', N'ref.FxRateDaily',         N'FX rates must exist for every trading day in the window',
     N'RateToUsd IS NULL',                                                               N'Completeness', N'FAIL',    0,     NULL,  NULL),
    (N'REF_TAX_EXPIRED',     N'REFERENCE', N'ref.TaxJurisdiction',     N'An active jurisdiction must not be expired',
     N'IsActive = 1 AND EffectiveTo < CAST(SYSUTCDATETIME() AS DATE)',                   N'Validity',     N'WARN',    5,     NULL,  NULL),

    -- Inventory and movement ------------------------------------------------
    (N'INV_NEG_ONHAND',      N'INVENTORY', N'stg.StockHolding',        N'On hand quantity must not be negative',
     N'QuantityOnHand < 0',                                                              N'Plausibility', N'WARN',    20,    NULL,  NULL),
    (N'MOV_NO_REASON',       N'INVENTORY', N'stg.Movement',            N'Adjustments must carry a reason code',
     N'MovementTypeCode = N''ADJ'' AND ReasonCode IS NULL',                              N'Completeness', N'WARN',    15,    NULL,  NULL),

    -- File ingestion --------------------------------------------------------
    (N'FILE_SHORT_ROW',      N'FILE',      N'err.RejectedFileRow',     N'Delimited rows must have the expected column count',
     N'RejectReasonCode = N''COLUMN_COUNT''',                                            N'Validity',     N'WARN',    100,   NULL,  NULL),
    (N'FILE_BAD_ENCODING',   N'FILE',      N'err.RejectedFileRow',     N'Inbound files must decode cleanly',
     N'RejectReasonCode = N''ENCODING''',                                                N'Validity',     N'FAIL',    0,     NULL,  NULL)
)
AS src (RuleCode, RuleGroupCode, ObjectName, RuleName, RuleExpression, DimensionCode,
        SeverityCode, ThresholdValue, RegionCode, SourceSystemCode)
    ON tgt.RuleCode = src.RuleCode
WHEN MATCHED THEN
    UPDATE SET tgt.RuleGroupCode     = src.RuleGroupCode,
               tgt.ObjectName        = src.ObjectName,
               tgt.RuleName          = src.RuleName,
               tgt.RuleExpression    = src.RuleExpression,
               tgt.DimensionCode     = src.DimensionCode,
               tgt.SeverityCode      = src.SeverityCode,
               tgt.ThresholdValue    = src.ThresholdValue,
               tgt.RegionCode        = src.RegionCode,
               tgt.SourceSystemCode  = src.SourceSystemCode,
               tgt.UpdatedAtUtc      = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (RuleCode, RuleGroupCode, ObjectName, RuleName, RuleExpression, DimensionCode,
            SeverityCode, ThresholdValue, RegionCode, SourceSystemCode, OwnerName)
    VALUES (src.RuleCode, src.RuleGroupCode, src.ObjectName, src.RuleName, src.RuleExpression,
            src.DimensionCode, src.SeverityCode, src.ThresholdValue, src.RegionCode,
            src.SourceSystemCode, N'Data Stewardship');
GO
