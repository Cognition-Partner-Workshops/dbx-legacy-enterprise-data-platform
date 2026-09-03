/*
    Aggregate schema

    Object        : the [Aggregate] schema that holds the pre-aggregated summary
                    tables the BI layer reads instead of the raw star schemas.
    Deploy target : WideWorldImportersDW
    Deploy order  : first file in sqlserver/warehouse/aggregates.
    Called by     : deployment only.

    The summaries were originally Analysis Services measure groups. When the
    cube was retired in 2015 the aggregations were dropped into relational
    tables under this schema and the report layer was re-pointed at them, which
    is why several of them still carry cube-era column names.
*/
SET NOCOUNT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'Aggregate')
    EXECUTE (N'CREATE SCHEMA [Aggregate] AUTHORIZATION [dbo];');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'Report')
    EXECUTE (N'CREATE SCHEMA [Report] AUTHORIZATION [dbo];');
GO
