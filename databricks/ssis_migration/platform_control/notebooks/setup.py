# Databricks notebook source
# Create + seed the Delta control tables, create the landing volume and upload the sample files.
# MAGIC %run ./_bootstrap

# COMMAND ----------
import json
import os
import shutil

from platform_control.control import ControlFramework
from platform_control.seed import seedAll
from platform_control.spark import getSpark
from platform_control.tables import ensureControlTables, ensureLandingVolume, ensureSchema

cfg = platformConfig()
spark = getSpark()
ensureSchema(spark, cfg)
created = ensureControlTables(spark, cfg)
volume = ensureLandingVolume(spark, cfg)
cf = ControlFramework(spark, cfg)
seeded = seedAll(cf)

uploaded = []
samplesRoot = os.path.abspath(os.path.join(os.getcwd(), "..", "samples", "landing"))
if volume and os.path.isdir(samplesRoot):
    for folder in sorted(os.listdir(samplesRoot)):
        source = os.path.join(samplesRoot, folder)
        target = os.path.join(volume, folder)
        os.makedirs(target, exist_ok=True)
        for fileName in sorted(os.listdir(source)):
            if not os.path.exists(os.path.join(target, fileName)):
                shutil.copyfile(os.path.join(source, fileName), os.path.join(target, fileName))
                uploaded.append(f"{folder}/{fileName}")
    for folder in ("processed", "archive", "errors", "quarantine"):
        os.makedirs(os.path.join(volume, folder), exist_ok=True)

summary = {"tables": len(created), "volume": volume, "seeded": seeded, "uploaded": uploaded}
print(json.dumps(summary, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))
