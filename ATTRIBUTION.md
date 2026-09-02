# Attribution

## Wide World Importers

This repository originates from the Microsoft **WideWorldImporters** sample
database for SQL Server and Azure SQL Database, published by Microsoft in the
[`microsoft/sql-server-samples`](https://github.com/microsoft/sql-server-samples)
repository under the MIT License.

The following directories are the original Microsoft sample, preserved as
shipped:

- `wwi-ssdt/` - WideWorldImporters OLTP database project
- `wwi-dw-ssdt/` - WideWorldImportersDW warehouse database project
- `wwi-ssis/` - the original Daily ETL SSIS project
- `wwi-app/` - the sample WPF application
- `wwi-azure-functions/` - the sample Azure Functions
- `wwi-ssasmd/` - the Analysis Services multidimensional model
- `power-bi-dashboards/` - the sample Power BI content
- `sample-scripts/` - the sample scripts
- `workload-drivers/` - the workload drivers
- `wwi-sample.sln`

Copyright (c) Microsoft Corporation. Licensed under the MIT License. The
original sample's terms continue to apply to that content.

## The legacy estate expansion

Everything under `oracle/`, `sqlserver/`, `ssis/`, `generators/`, `config/`,
`deployment/`, `validation/`, `infrastructure/`, `tools/` and `docs/` is an
addition to the Microsoft sample. It extends the same fictional Wide World
Importers business - the same customers, suppliers, stock items, orders and
invoices - across a wider, deliberately legacy-shaped enterprise estate:
an Oracle ERP, a SQL Server OLTP, a staging layer, a warehouse, and the SSIS
and SQL Agent machinery that moves data between them.

The Wide World Importers name, schema and business concepts remain Microsoft's;
the expansion follows the sample's conventions so that the additions read as
part of the same estate. All data in this repository - existing and added - is
fictional and synthetically generated. No real customer, supplier, employee or
financial data appears anywhere in it.
