/*
    Seed data for the ETL control framework.

    Deploy target : WideWorldImportersStaging
    Deploy order  : 03
    Depends on    : 02_tables_control_framework.sql

    No credential values appear here. Rows flagged IsSensitive name the
    environment variable that supplies the value at deployment time; see
    .env.example and config/README.md.
*/

MERGE etl.SourceSystem AS tgt
USING (VALUES
    (N'ORA_ERP',    N'Oracle ERP - master data, procurement and finance', N'Oracle',     N'GLOBAL', N'OracleHost',       N'UTC'),
    (N'ORA_ERP_NA', N'Oracle ERP - North America ledger',                 N'Oracle',     N'NA',     N'OracleHost',       N'America/Chicago'),
    (N'ORA_ERP_EU', N'Oracle ERP - Europe ledger',                        N'Oracle',     N'EU',     N'OracleHost',       N'Europe/London'),
    (N'ORA_ERP_AP', N'Oracle ERP - APAC ledger',                          N'Oracle',     N'APAC',   N'OracleHost',       N'Asia/Singapore'),
    (N'WWI_OLTP',   N'WideWorldImporters OLTP',                           N'SQL Server', N'GLOBAL', N'SqlServerOltpDb',  N'UTC'),
    (N'WWI_WEB',    N'WideWorldImporters ecommerce platform',             N'SQL Server', N'GLOBAL', N'SqlServerOltpDb',  N'UTC'),
    (N'PARTNER_FL', N'Partner sales flat-file feed',                      N'File',       N'GLOBAL', N'InboundFileRoot',  N'UTC'),
    (N'CARRIER_FL', N'Carrier delivery confirmation feed',                N'File',       N'GLOBAL', N'InboundFileRoot',  N'UTC'),
    (N'BANK_FL',    N'Bank payment clearing feed',                        N'File',       N'GLOBAL', N'InboundFileRoot',  N'UTC'),
    (N'FX_FEED',    N'Daily FX rate feed',                                N'File',       N'GLOBAL', N'InboundFileRoot',  N'UTC'),
    (N'MANUAL',     N'Manual adjustments and corrections',                N'Manual',     N'GLOBAL', NULL,                N'UTC')
) AS src (SourceSystemCode, SourceSystemName, Platform, RegionCode, ConnectionParameter, DefaultTimeZone)
    ON tgt.SourceSystemCode = src.SourceSystemCode
WHEN MATCHED THEN
    UPDATE SET SourceSystemName = src.SourceSystemName,
               Platform = src.Platform,
               RegionCode = src.RegionCode,
               ConnectionParameter = src.ConnectionParameter,
               DefaultTimeZone = src.DefaultTimeZone
WHEN NOT MATCHED BY TARGET THEN
    INSERT (SourceSystemCode, SourceSystemName, Platform, RegionCode, ConnectionParameter, DefaultTimeZone)
    VALUES (src.SourceSystemCode, src.SourceSystemName, src.Platform, src.RegionCode,
            src.ConnectionParameter, src.DefaultTimeZone);
GO

MERGE etl.Configuration AS tgt
USING (VALUES
    (N'EnvironmentCode',            N'ALL', N'DEV',                   N'String',  N'Deployment environment code (DEV, TEST, PROD).', 0),
    (N'WatermarkEpoch',             N'ALL', N'1900-01-01T00:00:00',   N'Date',    N'Lower bound used the first time an object is extracted.', 0),
    (N'MaxRejectPercent',           N'ALL', N'5',                     N'Decimal', N'Reject share of rows read above which a package execution is failed.', 0),
    (N'ReconAbsoluteTolerance',     N'ALL', N'0',                     N'Int',     N'Absolute row-count variance tolerated by the reconciliation gate.', 0),
    (N'ReconPercentTolerance',      N'ALL', N'0.0',                   N'Decimal', N'Percentage row-count variance tolerated by the reconciliation gate.', 0),
    (N'DefaultBatchSize',           N'ALL', N'100000',                N'Int',     N'Fast-load commit size for OLE DB destinations.', 0),
    (N'SourceQueryTimeoutSeconds',  N'ALL', N'3600',                  N'Int',     N'Command timeout applied to source extracts.', 0),
    (N'OracleFetchArraySize',       N'ALL', N'10000',                 N'Int',     N'Array fetch size for the Oracle provider.', 0),
    (N'LateArrivingDimensionDays',  N'ALL', N'7',                     N'Int',     N'Window in which a late-arriving dimension member is re-keyed into facts.', 0),
    (N'EarlyArrivingFactHoldDays',  N'ALL', N'3',                     N'Int',     N'How long an early-arriving fact waits on the unknown member before escalation.', 0),
    (N'SnapshotRetentionMonths',    N'ALL', N'36',                    N'Int',     N'Retention for periodic snapshot facts.', 0),
    (N'ControlTableRetentionDays',  N'ALL', N'400',                   N'Int',     N'Retention for etl.PackageExecution, etl.RowCountAudit and etl.ErrorLog.', 0),
    (N'RejectRetentionDays',        N'ALL', N'180',                   N'Int',     N'Retention for etl.RejectedRecord.', 0),
    (N'InboundFileRoot',            N'ALL', N'D:\WWI\inbound',        N'String',  N'Root of the inbound file drop. Overridden per environment.', 0),
    (N'ArchiveFileRoot',            N'ALL', N'D:\WWI\archive',        N'String',  N'Root of the processed-file archive.', 0),
    (N'RejectFileRoot',             N'ALL', N'D:\WWI\reject',         N'String',  N'Root of the malformed-record reject drop.', 0),
    (N'DefaultReportingCurrency',   N'ALL', N'USD',                   N'String',  N'Currency all warehouse monetary measures are converted to.', 0),
    (N'FxRateTolerancePercent',     N'ALL', N'2.0',                   N'Decimal', N'Day-over-day FX move above which the rate refresh warns.', 0),
    (N'UnknownMemberKey',           N'ALL', N'0',                     N'Int',     N'Surrogate key of the unknown member in every dimension.', 0),
    (N'IntradayCycleMinutes',       N'ALL', N'30',                    N'Int',     N'Interval of the intraday sales cycle.', 0),
    (N'OraclePasswordSecretName',   N'ALL', N'ORACLE_PASSWORD',       N'String',  N'Name of the environment variable holding the Oracle password.', 1),
    (N'SqlServerPasswordSecretName',N'ALL', N'SQLSERVER_PASSWORD',    N'String',  N'Name of the environment variable holding the SQL Server password.', 1),
    (N'EnvironmentCode',            N'PROD',N'PROD',                  N'String',  N'Production environment code.', 0),
    (N'MaxRejectPercent',           N'PROD',N'1',                     N'Decimal', N'Production tolerates far fewer rejects than lower environments.', 0),
    (N'DefaultBatchSize',           N'PROD',N'250000',                N'Int',     N'Larger commit size on production hardware.', 0)
) AS src (ConfigurationKey, EnvironmentCode, ConfigurationValue, ValueDataType, Description, IsSensitive)
    ON tgt.ConfigurationKey = src.ConfigurationKey
   AND tgt.EnvironmentCode = src.EnvironmentCode
WHEN MATCHED THEN
    UPDATE SET ConfigurationValue = src.ConfigurationValue,
               ValueDataType = src.ValueDataType,
               Description = src.Description,
               IsSensitive = src.IsSensitive,
               ModifiedAtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ConfigurationKey, EnvironmentCode, ConfigurationValue, ValueDataType, Description, IsSensitive)
    VALUES (src.ConfigurationKey, src.EnvironmentCode, src.ConfigurationValue, src.ValueDataType,
            src.Description, src.IsSensitive);
GO

MERGE etl.ReconciliationExemption AS tgt
USING (VALUES
    (N'work.CustomerDedup',            N'Deduplication collapses rows by design.'),
    (N'Dimension.Customer',            N'SCD Type 2 expands one source row into multiple versions.'),
    (N'Dimension.Stock Item',          N'SCD Type 2 expands one source row into multiple versions.'),
    (N'Dimension.Supplier',            N'SCD Type 2 expands one source row into multiple versions.'),
    (N'Aggregate.Sales Daily',         N'Aggregation reduces the row count by design.'),
    (N'Aggregate.Sales Monthly',       N'Aggregation reduces the row count by design.'),
    (N'Aggregate.Inventory Health',    N'Aggregation reduces the row count by design.'),
    (N'Aggregate.Customer 360',        N'Aggregation reduces the row count by design.'),
    (N'Fact.Stock Holding',            N'Periodic snapshot generates rows independent of the source count.'),
    (N'Fact.Order Fulfilment',         N'Accumulating snapshot updates existing rows rather than inserting.')
) AS src (ObjectName, Reason)
    ON tgt.ObjectName = src.ObjectName
WHEN MATCHED THEN UPDATE SET Reason = src.Reason
WHEN NOT MATCHED BY TARGET THEN INSERT (ObjectName, Reason) VALUES (src.ObjectName, src.Reason);
GO
