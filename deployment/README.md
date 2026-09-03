# Deployment

Ordered deployment scaffolding for the three technologies the estate runs on:
Oracle (WWIGERP), SQL Server (OLTP, staging, warehouse, msdb) and SSIS.

**None of these scripts has been executed against any server.** There is no
Oracle instance, SQL Server instance or SSIS catalogue in this repository, and
nothing here was run against one. They are the deployment definition, not a
deployment record. Treat the first real run as an untested run: use
`--dry-run` / `-DryRun` first and read what it says it would do.

**No credentials.** Every connection detail is read from the environment
variables documented in `config/.env.example`. A driver that finds a required
variable unset stops immediately and names every variable that is missing.
Secrets are never passed as command-line arguments: `sqlcmd` receives them
through `SQLCMDPASSWORD`, the Oracle clients receive them on stdin.

## Entry point

```bash
deployment/deploy-all.sh --dry-run          # what would happen
deployment/deploy-all.sh                    # DEV, everything
deployment/deploy-all.sh --stage sqlserver  # one stage
```

```powershell
.\deployment\deploy-all.ps1 -DryRun
.\deployment\deploy-all.ps1 -Stage ssis
```

The PowerShell entry point is the complete one. The SSIS project can only be
*built* on a Windows host with the Integration Services tooling, so
`deployment/ssis/deploy-ssis.sh` deploys a pre-built `.ispac` and configures the
environment but cannot produce the artifact.

## Order, and why

| # | Stage | What it does | Why here |
| --- | --- | --- | --- |
| 0 | preflight | tooling, environment variables, repo layout, landing zone | fails in seconds instead of halfway through stage 2 |
| 1 | oracle | `oracle/ddl` → `tables` → `views` → `functions` → `procedures` → `packages` → `reference` → `seed`, then a schema recompile | the SQL Server extract views and SSIS parameters name Oracle objects |
| 2 | sqlserver | control → OLTP extensions → staging → warehouse → security → agent | control tables must exist before any load procedure references them; security needs the schemas; the agent jobs name the SSIS folder but not the packages |
| 3 | ssis | build `.ispac` → deploy project → create environment → bind parameters | binds to databases that must already exist |

Within the SQL Server stage the control framework is deployed **twice**, into
staging and into the warehouse. That is deliberate and long-standing: the
warehouse keeps its own `etl.*` copy so that a staging outage cannot stop a
close reconciliation. It also means two watermark tables that have to be kept in
step by hand, which is exactly the kind of thing a migration should remove.

## Scripts

| Path | Variant | Purpose |
| --- | --- | --- |
| `deployment/deploy-all.sh` / `deployment/deploy-all.ps1` | both | entry point |
| `deployment/preflight/preflight.sh` / `Preflight.ps1` | both | prerequisite checks, no connections |
| `deployment/oracle/deploy-oracle.sh` / `Deploy-Oracle.ps1` | both | SQL\*Plus or sqlcl driver, dependency-ordered |
| `deployment/sqlserver/deploy-sqlserver.sh` / `Deploy-SqlServer.ps1` | both | sqlcmd driver for all six SQL Server stages |
| `deployment/ssis/Build-SsisProject.ps1` | PowerShell | devenv build, or a hand-assembled `.ispac` fallback |
| `deployment/ssis/Deploy-SsisCatalog.ps1` | PowerShell | `ISDeploymentWizard` or `catalog.deploy_project` |
| `deployment/ssis/Deploy-SsisEnvironment.ps1` | PowerShell | applies the rendered environment SQL |
| `deployment/ssis/deploy-ssis.sh` | shell | deploy `.ispac` + environment, no build |
| `deployment/ssis/render_environment_sql.py` | Python | renders `config/environments/*.env.yaml` into environment SQL |
| `deployment/ssis/environments/*.sql` | generated | committed output of the renderer |
| `deployment/lib/common.sh` / `Common.ps1` | both | shared logging, env checks, dry-run plumbing |

## Dry run

Both entry points and every stage driver accept `--dry-run` (`-DryRun`). In dry
run the drivers resolve everything they would do - which files, in which order,
against which database - and print it without invoking a client. Secret-bearing
arguments are never printed, in dry run or otherwise.

Dry run still requires the environment variables to be set. That is intentional:
the most common deployment mistake in this estate has been running with a
half-populated environment, and a dry run that ignored it would hide exactly the
problem it is there to find.

## Preflight

`preflight` checks what can be known without a connection:

- `sqlplus`/`sql`, `sqlcmd`, and on Windows `ISDeploymentWizard.exe`/`dtutil.exe`
  and the `SqlServer` module;
- every required environment variable for the stages being run;
- repository layout and the per-environment configuration file;
- the landing-zone subdirectories, when `WWI_LANDING_ROOT` is set.

It deliberately does **not** open a connection to anything. Connectivity is
confirmed by the operator outside this repository.

## PROD

`WWI_ENVIRONMENT=PROD` additionally requires `WWI_CONFIRM_PROD=I-UNDERSTAND`.
This exists because DEV and PROD variables live in the same shell profile on the
jump host and the wrong one has been picked up before.

## Regenerating the SSIS environment SQL

```bash
pip install pyyaml
python3 deployment/ssis/render_environment_sql.py --all
```

Commit the result. `Deploy-SsisEnvironment.ps1 -Verify` warns when the YAML is
newer than the rendered SQL, but it will not render it for you: the Windows
deploy hosts do not all have Python.

## After a deployment

1. Enable the jobs the environment file lists under `agent.enabled_jobs`; the
   two period-close jobs stay disabled until the close calendar is loaded.
2. Seed `etl.Configuration` from the environment file's `etl_configuration`
   block.
3. Set the initial watermarks for the incremental loads. There is no automation
   for this; the run book has the values.
4. Check `etl.vw_BatchStatus` after the first nightly run.
