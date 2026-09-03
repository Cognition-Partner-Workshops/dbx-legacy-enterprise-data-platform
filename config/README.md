# Configuration

Everything the estate needs to know about *where* it is running and *how* it
should behave there. No secret value is stored in this directory, or anywhere
else in this repository.

`config/estate-catalog.yaml` is not part of this: it is the foundation-owned
object catalogue, and it describes the estate's contents rather than its
configuration.

## Files

| File | What it is |
| --- | --- |
| `config/.env.example` | every environment variable the deployment drivers read, with a placeholder and a comment |
| `config/environments/dev.env.yaml` | DEV: servers, SSIS environment variables, `etl.Configuration` seed, agent enablement |
| `config/environments/test.env.yaml` | TEST, the sign-off environment |
| `config/environments/prod.env.yaml` | PROD |
| `config/connections/connection-managers.yaml` | what each SSIS connection manager is for and which parameters build it |
| `config/landing-zone.yaml` | the file landing-zone directory layout and retention rules |

## How a value resolves

A package asks for a setting and gets the first of these that has an answer:

1. **Package parameter** - set on the job step or on an execution. Used for
   one-off reruns: `/Par "$Package::LoadDate";"2019-03-01"`. Highest precedence
   and deliberately awkward to set, because it is not recorded anywhere.
2. **Project parameter, bound to the SSIS catalogue environment** - the normal
   case. `deployment/ssis/Deploy-SsisEnvironment.ps1` creates the environment
   from `config/environments/<env>.env.yaml` and binds every project parameter
   to a variable in it, so a value change is a catalogue change, not a rebuild.
3. **`etl.Configuration`** - runtime behaviour that operators change without a
   deployment: batch gates, per-region staleness tolerances, retention windows,
   parallelism. Read with `etl.ufn_GetConfigurationValue`. The seed values for
   each environment are the `etl_configuration` block in the environment YAML.
4. **The project parameter's design-time default** - the value baked in by
   `tools/ssisgen/project.py`. A package that falls through to this is a package
   whose environment binding is missing, and that is a defect, not a fallback.

The split between 2 and 3 has drifted over the years. `MaxRejectPercent` is a
project parameter, while `Reject.MaxAttempts` is an `etl.Configuration` row, for
no better reason than which team needed to change theirs without a deployment
window. A migration should collapse the two tiers.

## How secrets are supplied

They are not stored. At deploy time:

1. the operator sources an env file, held outside the repository, whose values
   come from the vault entries named in `config/.env.example`;
2. the deployment driver reads the variables and passes them to the client
   through a non-argument channel - `SQLCMDPASSWORD` for sqlcmd, stdin for the
   Oracle clients - so they do not appear in the process table or in logs;
3. sensitive SSIS parameters (`OraclePassword`, `SqlServerPassword`) are created
   as *sensitive* catalogue environment variables, which SSISDB encrypts with
   the catalogue master key. Their values are never read back, which is why a
   rotation is a re-run of the environment stage rather than an update;
4. Agent proxy credentials are created by dynamic SQL in
   `sqlserver/agent/02_create_proxies_and_credentials.sql` from a sqlcmd
   variable, for the same reason.

What this means for rotation: rotating `SQLSERVER_PASSWORD` means updating the
vault entry, re-running `deployment/deploy-all.ps1 -Stage ssis`, and re-running
the agent stage so the proxy credential is recreated. There is no single place
to change it. That is a real cost of the current design and one of the clearer
arguments for change.

## Environment differences that matter

| | DEV | TEST | PROD |
| --- | --- | --- | --- |
| Data | masked monthly restore | pseudonymised quarterly extract | live |
| Scheduled jobs | nightly only | all but the close jobs | all but the close jobs |
| `MaxRejectPercent` | 25 | 5 | 2 |
| `Hourly.LookbackMinutes` | 240 | 90 | 45 |
| `Close.RequireFxComplete` | 0 | 1 | 1 |
| Staging control retention | 14 days | 30 days | 45 days |
| Warehouse control retention | 90 days | 400 days | 1100 days |
| Extra grants | stewards get `db_datareader` on staging | none | none |

TEST is intentionally close to PROD so that a sign-off means something. DEV is
intentionally not.

## Changing configuration

- **A value for one environment**: edit `config/environments/<env>.env.yaml`,
  re-render with `python3 deployment/ssis/render_environment_sql.py --all`,
  commit both, and run the `ssis` stage.
- **A new project parameter**: it must be added to
  `tools/ssisgen/project.py` first (foundation-owned), then to all three
  environment YAML files, then re-rendered. A parameter that exists in the
  project but not in an environment silently falls through to tier 4.
- **An operational threshold**: change the `etl.Configuration` row directly in
  the environment and update the YAML so the next deployment does not undo it.
  This is the step most often forgotten.
