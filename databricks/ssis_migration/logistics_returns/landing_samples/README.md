# Representative landing-zone samples

No carrier files exist in the repo or on the SSIS host (`raw.FileCarrierScan` is empty on the baseline), so these two files
were generated from the `carrier_scan` layout in `config/landing-zone.yaml` (windows-1252, comma separated, header row,
`carrier_scan_{yyyyMMdd}_{seq3}.csv` + `ROWCOUNT=<n>` `.ctl` sidecar). `ShipmentReference` carries the WWI `ShipmentID`
of real December-2024 shipments so the scans attach to `silver_shipment` / `gold_fact_shipment`.

* `carrier_scan_20250109_001.csv` – 18 clean scan events (pickup / transit / exception / delivered / lost).
* `carrier_scan_20250109_002.csv` – 9 rows exercising the reject and duplicate paths: one exact duplicate scan, one missing
  tracking number, one missing status code, one unparseable timestamp.

The `stage_landing_files` job task copies them into `/Volumes/otterorders_migration/ssis_logistics_returns/landing/inbound/carrier/`.
