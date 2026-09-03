# Infrastructure

The descriptive picture of what the WWI estate runs on. Nothing in this
directory executes, provisions or connects to anything - it is inventory and
explanation, kept next to the code so the two do not drift.

| File | Kind | Contents |
| --- | --- | --- |
| `infrastructure/servers.yaml` | inventory | datacentres, hosts, instances, versions, sizing, availability |
| `infrastructure/storage-layout.yaml` | inventory | database sizes, filegroups, drives, autogrowth, tempdb, shares |
| `infrastructure/network.yaml` | inventory | zones, flows, ports, and the flows that are explicitly not permitted |
| `infrastructure/service-accounts.yaml` | inventory | account names and required permissions only, plus vault paths |
| `infrastructure/backup-recovery.md` | explanation | RPO/RTO, schedules, DR, and what recovery does to the control tables |
| `infrastructure/monitoring.md` | explanation | alerts mapped onto the `etl` control views, routing, regional divergence |

## Shape of the estate

Three environments, three regions, three technologies.

- **PROD** runs on four servers plus two file servers: an OLTP instance, a
  combined warehouse-and-staging instance, an SSIS/Agent instance, and the
  Oracle ERP the platform team does not own. `infrastructure/servers.yaml` has
  the detail and the reasons.
- **TEST** collapses the warehouse, staging and SSIS catalogue onto one host, so
  a timing result there does not predict PROD.
- **DEV** is a single instance holding all four databases.

The one deliberate cross-region component is the APAC file server in Singapore,
which exists because the APAC bank will not deliver files outside the region.

## Things worth knowing before changing anything

- Staging and the warehouse share a PROD instance. Separating them has been
  planned since 2017. Until it happens, the 01:00-04:00 quiet window is the only
  time heavy staging work is safe, and every schedule in `sqlserver/agent` is
  built around it.
- No host in the database, ETL or file zones has internet egress. Every
  deployment is an internal-artifact deployment; nothing can be fetched at
  deploy time.
- The Oracle instance is owned by the ERP team. A new source table is a change
  request against `WWI_ETL_READER`, granted per table rather than by role.
- The control framework is deployed into both staging and the warehouse, so
  there are two `etl.Watermark` tables that must be kept in step by hand. This
  shows up in `infrastructure/backup-recovery.md` and in
  `infrastructure/monitoring.md` as a recurring source of confusion.

## What is not here

No provisioning code, no infrastructure-as-code templates, no cloud resources.
The estate is on-premises and is built by the platform team through their own
processes; this directory is the record of the result, not the mechanism.
