# Runbook: deploying the estate

For an operator standing up the estate in an empty environment, or refreshing
one. `deployment/README.md` describes the drivers; this describes the run.

**No part of this has been executed.** The scripts have never been run against
an Oracle instance, a SQL Server instance or an SSIS catalog, so the first real
run is a first run in every sense. Everything below is derived from the scripts
in `deployment/`, not from a deployment that happened.

## Before you start

You need, on the deploy host:

- `sqlplus` or `sql` (SQLcl) for the Oracle stage;
- `sqlcmd` for the SQL Server stage;
- on Windows, `ISDeploymentWizard.exe` or `dtutil.exe` and the `SqlServer`
  PowerShell module for the SSIS stage;
- `python3` with `pyyaml` only if you need to re-render the SSIS environment SQL.

You need, in the shell environment, every variable listed in
`config/.env.example` for the stages you are running. Values come from wherever
your organisation keeps them; nothing in this repository holds a credential, and
no credential should be typed onto a command line - the drivers pass the SQL
Server password through `SQLCMDPASSWORD` and the Oracle password on stdin.

The SSIS project can only be **built** on Windows with the Integration Services
tooling. On Linux you can deploy a pre-built `.ispac` and configure the
environment, but you cannot produce the artifact.

## The run

### 0. Dry run, always

```bash
deployment/deploy-all.sh --dry-run
```

Read the output. It resolves every file, order and target database and prints
them without connecting. A dry run still requires the environment variables to
be set - a half-populated environment is the most common failure this estate
has, and a dry run that tolerated it would hide it.

### 1. Preflight

```bash
deployment/preflight/preflight.sh
```

Checks tooling, environment variables, repository layout, the per-environment
config file, and the landing-zone directories when `WWI_LANDING_ROOT` is set. It
opens no connections; confirming that the servers are reachable is your job,
outside this repository.

### 2. Oracle

```bash
deployment/deploy-all.sh --stage oracle
```

Order, which is a dependency order and not alphabetical:

```
oracle/ddl -> tables -> views -> functions -> procedures -> packages
           -> reference -> seed -> schema recompile
```

The recompile at the end is the check that matters. Oracle will happily create
an invalid package; the recompile is what tells you it is invalid. Query
`ALL_OBJECTS` for `status = 'INVALID'` in the five `WWI_*` schemas before
declaring the stage done - **no PL/SQL in this repository has ever been
compiled**, so expect this to be where the first real problems surface.

### 3. SQL Server

```bash
deployment/deploy-all.sh --stage sqlserver
```

Six sub-stages, in this order:

| # | Sub-stage | Target | Note |
| --- | --- | --- | --- |
| 1 | control | staging **and** warehouse | deployed twice, on purpose |
| 2 | OLTP extensions | `WideWorldImporters` | requires the base sample to exist |
| 3 | staging | `WideWorldImporters_Staging` | needs control from step 1 |
| 4 | warehouse | `WideWorldImportersDW` | requires the base DW sample |
| 5 | security | all three | roles and grants, names only |
| 6 | agent | `msdb` | 12 jobs, two shipped disabled |

The OLTP and warehouse sub-stages **extend** the Microsoft sample databases;
they do not create them. `WideWorldImporters` and `WideWorldImportersDW` must
already be restored, or every `ALTER TABLE ... ADD` in
`sqlserver/*/**.Extensions.sql` fails.

Two control copies means two `etl.Watermark` tables, two `etl.Configuration`
tables and two batch histories, kept in step by hand.

### 4. SSIS

```powershell
.\deployment\deploy-all.ps1 -Stage ssis
```

Build the `.ispac`, deploy the project to the catalog, create the environment,
bind the parameters. The environment SQL is generated from
`config/environments/<env>.env.yaml` and committed under
`deployment/ssis/environments/`; if you changed the YAML, re-render and commit
before deploying:

```bash
python3 deployment/ssis/render_environment_sql.py --all
```

`Deploy-SsisEnvironment.ps1 -Verify` warns when the YAML is newer than the
rendered SQL but will not render it for you.

The parameter bindings are the part that silently goes wrong: a package whose
connection-manager parameter is unbound falls back to whatever the project
default is, which in this estate is DEV.

## PROD

`WWI_ENVIRONMENT=PROD` additionally requires `WWI_CONFIRM_PROD=I-UNDERSTAND`.
That gate exists because the DEV and PROD variables live in the same shell
profile on the jump host and the wrong one has been picked up before.

## After the deployment

In this order:

1. **Enable jobs.** Enable what the environment file's `agent.enabled_jobs`
   lists. `WWI - Month End` and `WWI - Finance Close` stay disabled until the
   fiscal-calendar rows for the year are loaded.
2. **Seed `etl.Configuration`** from the environment file's `etl_configuration`
   block - in both databases.
3. **Set initial watermarks.** There is no automation for this. An unset
   watermark makes an incremental load behave as a full load on its first run,
   which for the transactional extracts is a very long first run.
4. **Run the static checks** against the deployed tree to confirm nothing was
   deployed from a modified working copy:
   ```bash
   python3 validation/static/run_all_checks.py
   python3 validation/checks/run_deep_checks.py
   ```
5. **Run the runtime validation** in `validation/runtime/`, which has also never
   been run, and expect to fix things.
6. **Watch the first nightly run** in `etl.vw_BatchStatus`, then
   `etl.vw_RowCountReconciliation` and `etl.vw_RejectSummary`.

## Rollback

There is none, and this is a genuine gap. The SQL Server scripts are guarded
(`IF OBJECT_ID(...) IS NULL`) so re-running is safe, but nothing removes what a
partial run created, and the Oracle stage has no equivalent guard on every
object. A failed deployment is recovered by restoring the target databases from
backup, which means you take one first.

## If a stage fails

| Symptom | Look at |
| --- | --- |
| Driver stops naming missing variables | your shell environment; the driver lists every one it needs |
| Oracle objects created but invalid | the recompile output, then `ALL_ERRORS` for the schema |
| `ALTER TABLE ... ADD` fails on the OLTP or DW stage | the base Microsoft sample is not restored, or is a different version |
| Staging scripts fail on `etl.*` | the control sub-stage did not run against that database |
| SSIS deploy succeeds, packages fail immediately | environment not bound, or bound to the wrong environment |
| Agent jobs exist but never fire | the Agent service, then the schedule, then whether the job ships disabled |
