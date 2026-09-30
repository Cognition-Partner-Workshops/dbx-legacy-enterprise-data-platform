from datetime import date

from finance.config import FinanceConfig
from finance.io import WATERMARK_EPOCH, ensureControlTables, getWatermark, nextBatchId, setWatermark


def test_watermark_roundtrip_and_batch_ids(spark):
    cfg = FinanceConfig(catalog="spark_catalog", schema="fin_test", businessDate=date(2024, 12, 31))
    ensureControlTables(spark, cfg)
    assert getWatermark(spark, cfg, "EXT_ORA_ApPayment") == WATERMARK_EPOCH
    setWatermark(spark, cfg, "EXT_ORA_ApPayment", "last_upd_dt", "2024-12-01 10:00:00", 5)
    setWatermark(spark, cfg, "EXT_ORA_ApPayment", "last_upd_dt", "2024-12-02 10:00:00", 7)
    assert getWatermark(spark, cfg, "EXT_ORA_ApPayment") == "2024-12-02 10:00:00"
    assert getWatermark(spark, cfg, "EXT_ORA_ApInvoiceLine") == WATERMARK_EPOCH
    b1, b2 = nextBatchId(spark, cfg), nextBatchId(spark, cfg)
    assert b2 >= b1
