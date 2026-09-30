# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
"""SLS_Export_PartnerFeed -> /Volumes/.../landing/outbound/partner_feed/partner_feed_YYYYMMDD.csv"""

import json
import os

from sales_performance import config
from sales_performance.partner_feed import runPartnerFeed

batchId = batchIdParam()
outboundDir = taskParam("outbound_dir", os.path.join(config.volumeRoot(), "outbound", "partner_feed"))
partnerScope = taskParam("partner_scope", "ALL")
suppress = taskParam("suppress_unconsented_eu_rows", "true").lower() == "true"
feedFromDate = taskParam("feed_from_date", "") or None
print(json.dumps({"batch_id": batchId, **runPartnerFeed(spark, batchId, outboundDir, partnerScope, suppress, feedFromDate)}, default=str))
