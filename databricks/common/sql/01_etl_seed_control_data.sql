-- Idempotent seed data for the WWI ETL control framework.
-- GENERATED from dbx_etl_common.seeds by databricks/common/tools/render_sql.py - do not edit by hand.
-- Legacy sources: sqlserver/control/03_seed_control_data.sql, 05_seed_data_quality_rules.sql.

MERGE INTO ${catalog}.etl.source_system AS tgt
USING (SELECT * FROM VALUES
    ('ORA_ERP', 'Oracle ERP - master data, procurement and finance', 'Oracle', 'GLOBAL', 'OracleHost', 'UTC'),
    ('ORA_ERP_NA', 'Oracle ERP - North America ledger', 'Oracle', 'NA', 'OracleHost', 'America/Chicago'),
    ('ORA_ERP_EU', 'Oracle ERP - Europe ledger', 'Oracle', 'EU', 'OracleHost', 'Europe/London'),
    ('ORA_ERP_AP', 'Oracle ERP - APAC ledger', 'Oracle', 'APAC', 'OracleHost', 'Asia/Singapore'),
    ('WWI_OLTP', 'WideWorldImporters OLTP', 'SQL Server', 'GLOBAL', 'SqlServerOltpDb', 'UTC'),
    ('WWI_WEB', 'WideWorldImporters ecommerce platform', 'SQL Server', 'GLOBAL', 'SqlServerOltpDb', 'UTC'),
    ('PARTNER_FL', 'Partner sales flat-file feed', 'File', 'GLOBAL', 'InboundFileRoot', 'UTC'),
    ('CARRIER_FL', 'Carrier delivery confirmation feed', 'File', 'GLOBAL', 'InboundFileRoot', 'UTC'),
    ('BANK_FL', 'Bank payment clearing feed', 'File', 'GLOBAL', 'InboundFileRoot', 'UTC'),
    ('FX_FEED', 'Daily FX rate feed', 'File', 'GLOBAL', 'InboundFileRoot', 'UTC'),
    ('MANUAL', 'Manual adjustments and corrections', 'Manual', 'GLOBAL', NULL, 'UTC')
  AS v (SourceSystemCode, SourceSystemName, Platform, RegionCode, ConnectionParameter, DefaultTimeZone)) AS src
ON tgt.SourceSystemCode = src.SourceSystemCode
WHEN MATCHED THEN UPDATE SET tgt.SourceSystemName = src.SourceSystemName, tgt.Platform = src.Platform, tgt.RegionCode = src.RegionCode, tgt.ConnectionParameter = src.ConnectionParameter, tgt.DefaultTimeZone = src.DefaultTimeZone
WHEN NOT MATCHED THEN INSERT (SourceSystemCode, SourceSystemName, Platform, RegionCode, ConnectionParameter, DefaultTimeZone) VALUES (src.SourceSystemCode, src.SourceSystemName, src.Platform, src.RegionCode, src.ConnectionParameter, src.DefaultTimeZone);

MERGE INTO ${catalog}.etl.configuration AS tgt
USING (SELECT * FROM VALUES
    ('EnvironmentCode', 'ALL', 'DEV', 'String', 'Deployment environment code (DEV, TEST, PROD).', false),
    ('WatermarkEpoch', 'ALL', '1900-01-01T00:00:00', 'Date', 'Lower bound used the first time an object is extracted.', false),
    ('MaxRejectPercent', 'ALL', '5', 'Decimal', 'Reject share of rows read above which a package execution is failed.', false),
    ('ReconAbsoluteTolerance', 'ALL', '0', 'Int', 'Absolute row-count variance tolerated by the reconciliation gate.', false),
    ('ReconPercentTolerance', 'ALL', '0.0', 'Decimal', 'Percentage row-count variance tolerated by the reconciliation gate.', false),
    ('DefaultBatchSize', 'ALL', '100000', 'Int', 'Fast-load commit size for OLE DB destinations (Delta: target rows per write batch).', false),
    ('SourceQueryTimeoutSeconds', 'ALL', '3600', 'Int', 'Command timeout applied to source extracts.', false),
    ('OracleFetchArraySize', 'ALL', '10000', 'Int', 'Array fetch size for the Oracle provider (JDBC fetchsize).', false),
    ('LateArrivingDimensionDays', 'ALL', '7', 'Int', 'Window in which a late-arriving dimension member is re-keyed into facts.', false),
    ('EarlyArrivingFactHoldDays', 'ALL', '3', 'Int', 'How long an early-arriving fact waits on the unknown member before escalation.', false),
    ('SnapshotRetentionMonths', 'ALL', '36', 'Int', 'Retention for periodic snapshot facts.', false),
    ('ControlTableRetentionDays', 'ALL', '400', 'Int', 'Retention for etl.package_execution, etl.row_count_audit and etl.error_log.', false),
    ('RejectRetentionDays', 'ALL', '180', 'Int', 'Retention for etl.rejected_record.', false),
    ('InboundFileRoot', 'ALL', '/Volumes/wwi/landing/inbound', 'String', 'Root of the inbound file drop (Unity Catalog volume). Overridden per environment.', false),
    ('ArchiveFileRoot', 'ALL', '/Volumes/wwi/landing/archive', 'String', 'Root of the processed-file archive (Unity Catalog volume).', false),
    ('RejectFileRoot', 'ALL', '/Volumes/wwi/landing/reject', 'String', 'Root of the malformed-record reject drop (Unity Catalog volume).', false),
    ('DefaultReportingCurrency', 'ALL', 'USD', 'String', 'Currency all warehouse monetary measures are converted to.', false),
    ('FxRateTolerancePercent', 'ALL', '2.0', 'Decimal', 'Day-over-day FX move above which the rate refresh warns.', false),
    ('UnknownMemberKey', 'ALL', '0', 'Int', 'Surrogate key of the unknown member in every dimension.', false),
    ('IntradayCycleMinutes', 'ALL', '30', 'Int', 'Interval of the intraday sales cycle.', false),
    ('OraclePasswordSecretName', 'ALL', 'ORACLE_PASSWORD', 'String', 'Name of the secret (scope wwi) holding the Oracle password.', true),
    ('SqlServerPasswordSecretName', 'ALL', 'SQLSERVER_PASSWORD', 'String', 'Name of the secret (scope wwi) holding the SQL Server password.', true),
    ('EnvironmentCode', 'PROD', 'PROD', 'String', 'Production environment code.', false),
    ('MaxRejectPercent', 'PROD', '1', 'Decimal', 'Production tolerates far fewer rejects than lower environments.', false),
    ('DefaultBatchSize', 'PROD', '250000', 'Int', 'Larger commit size on production hardware.', false)
  AS v (ConfigurationKey, EnvironmentCode, ConfigurationValue, ValueDataType, Description, IsSensitive)) AS src
ON tgt.ConfigurationKey = src.ConfigurationKey AND tgt.EnvironmentCode = src.EnvironmentCode
WHEN MATCHED THEN UPDATE SET tgt.ConfigurationValue = src.ConfigurationValue, tgt.ValueDataType = src.ValueDataType, tgt.Description = src.Description, tgt.IsSensitive = src.IsSensitive, tgt.ModifiedAtUtc = current_timestamp()
WHEN NOT MATCHED THEN INSERT (ConfigurationKey, EnvironmentCode, ConfigurationValue, ValueDataType, Description, IsSensitive) VALUES (src.ConfigurationKey, src.EnvironmentCode, src.ConfigurationValue, src.ValueDataType, src.Description, src.IsSensitive);

MERGE INTO ${catalog}.etl.reconciliation_exemption AS tgt
USING (SELECT * FROM VALUES
    ('work.CustomerDedup', 'Deduplication collapses rows by design.'),
    ('Dimension.Customer', 'SCD Type 2 expands one source row into multiple versions.'),
    ('Dimension.Stock Item', 'SCD Type 2 expands one source row into multiple versions.'),
    ('Dimension.Supplier', 'SCD Type 2 expands one source row into multiple versions.'),
    ('Aggregate.Sales Daily', 'Aggregation reduces the row count by design.'),
    ('Aggregate.Sales Monthly', 'Aggregation reduces the row count by design.'),
    ('Aggregate.Daily Inventory Health', 'Aggregation reduces the row count by design.'),
    ('Aggregate.Customer 360', 'Aggregation reduces the row count by design.'),
    ('Fact.Stock Holding', 'Periodic snapshot generates rows independent of the source count.'),
    ('Fact.Order Fulfilment', 'Accumulating snapshot updates existing rows rather than inserting.')
  AS v (ObjectName, Reason)) AS src
ON tgt.ObjectName = src.ObjectName
WHEN MATCHED THEN UPDATE SET tgt.Reason = src.Reason
WHEN NOT MATCHED THEN INSERT (ObjectName, Reason) VALUES (src.ObjectName, src.Reason);

MERGE INTO ${catalog}.etl.data_quality_rule AS tgt
USING (SELECT * FROM VALUES
    ('CUST_NULL_NAME', 'CUSTOMER', 'stg.Customer', 'Customer name must be present', 'CustomerName IS NULL OR trim(CustomerName) = ''''', 'Completeness', 'FAIL', 0, NULL, NULL),
    ('CUST_DUP_BKEY', 'CUSTOMER', 'stg.Customer', 'Business key must be unique in the batch', 'CustomerBusinessKey IN (SELECT CustomerBusinessKey FROM stg.Customer GROUP BY CustomerBusinessKey HAVING count(*) > 1)', 'Uniqueness', 'FAIL', 0, NULL, NULL),
    ('CUST_NO_CATEGORY', 'CUSTOMER', 'stg.Customer', 'Category must resolve to the conformed set', 'CustomerCategoryCode IS NULL', 'Validity', 'WARN', 250, NULL, NULL),
    ('CUST_BAD_POSTCODE_NA', 'CUSTOMER', 'stg.Customer', 'US and CA postal codes must match the country format', 'CountryCode IN (''US'', ''CA'') AND CAST(PostalCodeIsValid AS INT) = 0', 'Validity', 'WARN', 50, 'NA', NULL),
    ('CUST_BAD_POSTCODE_EU', 'CUSTOMER', 'stg.Customer', 'EU postal codes must match the country format', 'RegionCode = ''EU'' AND CAST(PostalCodeIsValid AS INT) = 0', 'Validity', 'WARN', 400, 'EU', NULL),
    ('CUST_BAD_POSTCODE_AP', 'CUSTOMER', 'stg.Customer', 'APAC postal codes must match the country format', 'RegionCode = ''APAC'' AND CAST(PostalCodeIsValid AS INT) = 0', 'Validity', 'WARN', 2000, 'APAC', NULL),
    ('CUST_FUTURE_OPENED', 'CUSTOMER', 'stg.Customer', 'Account opened date cannot be in the future', 'AccountOpenedDate > current_date()', 'Validity', 'FAIL', 0, NULL, 'ORA_ERP'),
    ('SUPP_NULL_NAME', 'SUPPLIER', 'stg.Supplier', 'Supplier name must be present', 'SupplierName IS NULL OR trim(SupplierName) = ''''', 'Completeness', 'FAIL', 0, NULL, NULL),
    ('SUPP_NO_TAXID_EU', 'SUPPLIER', 'stg.Supplier', 'EU suppliers must carry a VAT registration', 'RegionCode = ''EU'' AND (TaxRegistrationNumber IS NULL OR length(TaxRegistrationNumber) < 8)', 'Completeness', 'FAIL', 5, 'EU', NULL),
    ('SUPP_NO_PAYMENT_TERM', 'SUPPLIER', 'stg.Supplier', 'Payment terms must resolve', 'PaymentTermsCode IS NULL', 'Validity', 'WARN', 25, NULL, NULL),
    ('OL_NEG_QTY', 'ORDERLINE', 'stg.OrderLine', 'Ordered quantity must be positive', 'Quantity <= 0', 'Validity', 'FAIL', 0, NULL, NULL),
    ('OL_NEG_PRICE', 'ORDERLINE', 'stg.OrderLine', 'Unit price must not be negative', 'UnitPrice < 0', 'Validity', 'FAIL', 0, NULL, NULL),
    ('OL_ORPHAN_ORDER', 'ORDERLINE', 'stg.OrderLine', 'Every line must have a header', 'NOT EXISTS (SELECT 1 FROM stg.Order AS o WHERE o.OrderBusinessKey = OrderLine.OrderBusinessKey)', 'Integrity', 'FAIL', 0, NULL, NULL),
    ('OL_DISCOUNT_RANGE', 'ORDERLINE', 'stg.OrderLine', 'Discount percentage must be between 0 and 90', 'DiscountPercentage < 0 OR DiscountPercentage > 90', 'Validity', 'WARN', 10, NULL, NULL),
    ('OL_EXTENDED_MISMATCH', 'ORDERLINE', 'stg.OrderLine', 'Extended amount must agree with quantity times net price', 'abs(ExtendedAmount - (Quantity * UnitPrice * (1 - DiscountPercentage / 100.0))) > 0.01', 'Accuracy', 'WARN', 100, NULL, NULL),
    ('IL_NO_TAX_RATE', 'INVOICE', 'stg.InvoiceLine', 'Tax rate must resolve from the jurisdiction', 'TaxRate IS NULL', 'Completeness', 'FAIL', 0, NULL, NULL),
    ('IL_TAX_MISMATCH_NA', 'INVOICE', 'stg.InvoiceLine', 'NA sales tax must agree with the rate applied', 'RegionCode = ''NA'' AND abs(TaxAmount - (ExtendedAmount * TaxRate / 100.0)) > 0.02', 'Accuracy', 'FAIL', 0, 'NA', NULL),
    ('IL_TAX_MISMATCH_EU', 'INVOICE', 'stg.InvoiceLine', 'EU VAT must agree unless reverse charged', 'RegionCode = ''EU'' AND CAST(ReverseChargeFlag AS INT) = 0 AND abs(TaxAmount - (ExtendedAmount * TaxRate / 100.0)) > 0.02', 'Accuracy', 'WARN', 75, 'EU', NULL),
    ('IL_MARGIN_NEGATIVE', 'INVOICE', 'stg.InvoiceLine', 'Margin below cost needs review', 'ExtendedAmount < CostAmount', 'Plausibility', 'WARN', 500, NULL, NULL),
    ('PAY_UNAPPLIED', 'PAYMENT', 'stg.SupplierPayment', 'Payments must be applied to an invoice', 'InvoiceBusinessKey IS NULL', 'Integrity', 'WARN', 40, NULL, NULL),
    ('PAY_FX_MISSING', 'PAYMENT', 'stg.SupplierPayment', 'Non-USD payments need an FX rate for the value date', 'CurrencyCode <> ''USD'' AND FxRateToUsd IS NULL', 'Completeness', 'FAIL', 0, NULL, NULL),
    ('PAY_FUTURE_DATE', 'PAYMENT', 'stg.SupplierPayment', 'Payment date cannot be in the future', 'PaymentDate > current_date()', 'Validity', 'FAIL', 0, NULL, 'ORA_ERP'),
    ('REF_UNMAPPED_CODE', 'REFERENCE', 'ref.CodeCrosswalk', 'Source codes must map to the conformed set', 'ConformedCode IS NULL', 'Integrity', 'WARN', 30, NULL, NULL),
    ('REF_FX_GAP', 'REFERENCE', 'ref.FxRateDaily', 'FX rates must exist for every trading day in the window', 'RateToUsd IS NULL', 'Completeness', 'FAIL', 0, NULL, NULL),
    ('REF_TAX_EXPIRED', 'REFERENCE', 'ref.TaxJurisdiction', 'An active jurisdiction must not be expired', 'CAST(IsActive AS INT) = 1 AND EffectiveTo < current_date()', 'Validity', 'WARN', 5, NULL, NULL),
    ('INV_NEG_ONHAND', 'INVENTORY', 'stg.StockHolding', 'On hand quantity must not be negative', 'QuantityOnHand < 0', 'Plausibility', 'WARN', 20, NULL, NULL),
    ('MOV_NO_REASON', 'INVENTORY', 'stg.Movement', 'Adjustments must carry a reason code', 'MovementTypeCode = ''ADJ'' AND ReasonCode IS NULL', 'Completeness', 'WARN', 15, NULL, NULL),
    ('FILE_SHORT_ROW', 'FILE', 'err.RejectedFileRow', 'Delimited rows must have the expected column count', 'RejectReasonCode = ''COLUMN_COUNT''', 'Validity', 'WARN', 100, NULL, NULL),
    ('FILE_BAD_ENCODING', 'FILE', 'err.RejectedFileRow', 'Inbound files must decode cleanly', 'RejectReasonCode = ''ENCODING''', 'Validity', 'FAIL', 0, NULL, NULL)
  AS v (RuleCode, RuleGroupCode, ObjectName, RuleName, RuleExpression, DimensionCode, SeverityCode, ThresholdValue, RegionCode, SourceSystemCode)) AS src
ON tgt.RuleCode = src.RuleCode
WHEN MATCHED THEN UPDATE SET tgt.RuleGroupCode = src.RuleGroupCode, tgt.ObjectName = src.ObjectName, tgt.RuleName = src.RuleName, tgt.RuleExpression = src.RuleExpression, tgt.DimensionCode = src.DimensionCode, tgt.SeverityCode = src.SeverityCode, tgt.ThresholdValue = src.ThresholdValue, tgt.RegionCode = src.RegionCode, tgt.SourceSystemCode = src.SourceSystemCode, tgt.UpdatedAtUtc = current_timestamp()
WHEN NOT MATCHED THEN INSERT (RuleCode, RuleGroupCode, ObjectName, RuleName, RuleExpression, DimensionCode, SeverityCode, ThresholdValue, RegionCode, SourceSystemCode, OwnerName) VALUES (src.RuleCode, src.RuleGroupCode, src.ObjectName, src.RuleName, src.RuleExpression, src.DimensionCode, src.SeverityCode, src.ThresholdValue, src.RegionCode, src.SourceSystemCode, 'Data Stewardship');
