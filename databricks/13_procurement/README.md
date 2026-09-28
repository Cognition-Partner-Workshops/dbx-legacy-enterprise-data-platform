# wwi_13_procurement — Databricks bundle for `ssis/13_procurement`

Migration of the five `PRC_*` packages of the WWI_Procurement SSIS project (phase 92 "Procurement
Mart") to PySpark notebooks orchestrated by one Databricks Job. Mapping and open decisions:
`docs/migration/13_procurement-package-mapping.md`.

```
databricks.yml                      bundle (targets dev / prod, variables catalog, ...)
resources/wwi_13_procurement.job.yml  job wwi_13_procurement, task_key == legacy package name
notebooks/PRC_*.py                  one source-format notebook per package
src/procurement_lib/                pure DataFrame transforms shared by notebooks and tests
validation/PRC_Reconcile_Procurement.py  row-count / hash reconciliation -> etl.row_count_log
validation/baseline_example.json    shape of the SQL Server baseline input
tests/                              pytest (local PySpark) + test-only fake of dbx_etl_common
```

## Local checks
```bash
pip install pyspark==3.5.1 delta-spark==3.2.0 pytest
python -m py_compile notebooks/*.py validation/*.py
python -m pytest tests -q                 # PRC_TESTS_WITH_DELTA=0 to skip the Delta MERGE test
databricks bundle validate -t dev
```

## Deploy / run (not done by the migration session)
```bash
databricks bundle deploy -t dev
databricks bundle run -t dev wwi_13_procurement \
  --params BatchId=123,BusinessDate=2024-03-31,StatementPeriod=2024-03
```
`dbx_etl_common` (session 00) is attached to every task as a wheel library from the workspace
path in variable `dbx_etl_common_wheel`; override it per target once the artifact is published.
