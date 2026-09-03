# Security definitions

Role, principal and grant *definitions* for the SQL Server side of the estate.
No script in this directory has been executed against a server, and none of them
contains a secret: every password is supplied at deploy time as a sqlcmd
variable sourced from the environment variables in `config/.env.example`.

## Order

| Script | Target | Purpose |
| --- | --- | --- |
| `00_server_principals.sql` | `master` | Windows logins, the two surviving SQL logins, server roles |
| `01_database_roles_staging.sql` | staging | loader / reader / steward / control-reader roles |
| `02_database_roles_warehouse.sql` | warehouse | loader, reader, finance and the three regional reader roles |
| `03_database_roles_oltp.sql` | OLTP | extract-view and change-tracking read roles |
| `04_msdb_and_ssisdb_permissions.sql` | `msdb`, `SSISDB` | job operator role, catalogue folder rights |
| `05_oracle_link_principals.sql` | `master` | `WWIGERP_LINK` linked server and the Oracle grants it assumes |

## Principals

| Principal | Kind | Used by |
| --- | --- | --- |
| `DOMAIN\<EtlServiceAccount>` | Windows | SSIS execution, Agent proxy `WWI_SSIS_Proxy` |
| `DOMAIN\<AppServiceAccount>` | Windows | the OLTP application pool |
| `DOMAIN\<ReportServiceAccount>` | Windows | the reporting gateway |
| `DOMAIN\WWI-DBA-<env>` | Windows group | platform DBAs, `sysadmin` |
| `DOMAIN\WWI-ETLOperators` | Windows group | run-book operators |
| `DOMAIN\WWI-DataStewards` | Windows group | reject triage |
| `DOMAIN\WWI-FinanceAnalysts` | Windows group | close reporting |
| `WWI_ETL` | SQL login | SSIS connection managers where Kerberos does not hop |
| `WWI_ReportGateway` | SQL login | the pre-domain-trust reporting gateway |

Both SQL logins are legacy. They exist because the reporting gateway and the
SSIS file-share hop predate the domain trust, and every attempt to retire them
has been deferred. A modern estate would use managed identities for both.

## Environment parameterisation

`$(EnvironmentCode)` is DEV, TEST or PROD and changes behaviour in two places:

- the DBA group is per-environment (`WWI-DBA-DEV` and so on);
- in DEV only, `WWI-DataStewards` is additionally granted `db_datareader` on
  staging so a data problem can be chased without a ticket.

Everything else is identical across environments by design, so that a permission
problem found in TEST reproduces in PROD.

## Regional divergence

The warehouse carries three regional reader roles that differ in what they can
see, not only in name:

- `WWI_RegionalReader_EU` is denied the customer contact and postal-code columns,
  because the EU dataset is held under the consent rules the customer-sync job
  enforces;
- `WWI_RegionalReader_APAC` is denied the contact column but keeps postal codes,
  which APAC needs for its prefecture and postcode-district reporting;
- `WWI_RegionalReader_NA` has neither restriction.

## Known gaps

- Column-level `DENY` statements name columns of the Microsoft sample
  `Dimension.Customer` table. If a work package renames those columns the DENY
  will fail at deploy time.
- `SSISDB` and `[extract]` grants are skipped with a printed message when the
  catalogue or schema is not present yet, so the stage can run in either order.
