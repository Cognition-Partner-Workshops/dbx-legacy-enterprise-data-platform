# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
# Creates the control tables, the landing/export volumes and uploads the representative
# supplier_catalog_*.psv sample files (Auto Loader / read_files input for ING_FILE_SupplierCatalog).
import os
import shutil

from procurement import io
from procurement.catalog_ingest import FEED_SUBDIR
from procurement.config import CATALOG, EXPORT_VOLUME, LANDING_VOLUME, SCHEMA, volumePath

for volume in (LANDING_VOLUME, EXPORT_VOLUME):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{volume}")

landingDir = f"{volumePath(LANDING_VOLUME)}/{FEED_SUBDIR}"
os.makedirs(landingDir, exist_ok=True)
os.makedirs(f"{volumePath(EXPORT_VOLUME)}/supplier_statement", exist_ok=True)
samplesDir = os.path.abspath(os.path.join(os.getcwd(), "..", "samples", "supplier_catalog"))
uploaded = []
for name in sorted(os.listdir(samplesDir)):
    if name.endswith(".psv"):
        shutil.copyfile(os.path.join(samplesDir, name), os.path.join(landingDir, name))
        uploaded.append(name)
io.ensureControlTables(spark)
print(f"landing volume {landingDir}: uploaded {uploaded}")
