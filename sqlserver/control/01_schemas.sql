/*
    Schemas used by the WideWorldImporters legacy staging database.

    Deploy target : WideWorldImportersStaging  (SQLSERVER_STAGING_DB)
    Deploy order  : 01

    raw   - landing zone, one table per source object, source-typed columns, no
            constraints, truncated or appended by the extract packages.
    stg   - conformed/typed staging, business keys resolved, source codes
            translated.
    work  - scratch/derived sets built by staging procedures (dedup keys,
            standardised addresses, allocation working sets).
    err   - rejected rows and data-quality screens.
    etl   - the batch/audit/control framework. Every package writes here.
    ref   - reference and mapping data that has no single system of record.
*/

IF SCHEMA_ID(N'raw') IS NULL EXEC (N'CREATE SCHEMA [raw] AUTHORIZATION [dbo];');
GO
IF SCHEMA_ID(N'stg') IS NULL EXEC (N'CREATE SCHEMA [stg] AUTHORIZATION [dbo];');
GO
IF SCHEMA_ID(N'work') IS NULL EXEC (N'CREATE SCHEMA [work] AUTHORIZATION [dbo];');
GO
IF SCHEMA_ID(N'err') IS NULL EXEC (N'CREATE SCHEMA [err] AUTHORIZATION [dbo];');
GO
IF SCHEMA_ID(N'etl') IS NULL EXEC (N'CREATE SCHEMA [etl] AUTHORIZATION [dbo];');
GO
IF SCHEMA_ID(N'ref') IS NULL EXEC (N'CREATE SCHEMA [ref] AUTHORIZATION [dbo];');
GO
