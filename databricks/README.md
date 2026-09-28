# Databricks sales lakehouse

Databricks/Delta Lake replacement for the WideWorldImporters sales pipeline
(SQL Server OLTP -> staging -> DW -> aggregates -> reporting, orchestrated by
`Master_Daily_ETL` / `Master_Hourly_Incremental` / `Master_Month_End` SSIS).

Runs end-to-end on mock data with no production connectivity. See
`CONVENTIONS.md` for the layer contract and `docs/migration/sales-etl-mapping.md`
(added by the integration pass) for the legacy -> lakehouse artifact map.

```
cd databricks
python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
python -m sales_lakehouse.mock_data.generate --scale small     # mock sources
python -m sales_lakehouse.orchestration.run_pipeline           # bronze -> silver -> gold
pytest tests -q
```
