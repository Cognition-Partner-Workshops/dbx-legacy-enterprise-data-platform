from decimal import Decimal

from pyspark.sql import Row

from dbx_etl_common import control
from dq_quality import delta_io


def test_register_rejects_calls_shared_api_once_per_reason(spark):
    control.calls.clear()
    df = spark.createDataFrame([Row(Key="a", RejectReasonCode="R1"), Row(Key="b", RejectReasonCode="R1"),
                                Row(Key="c", RejectReasonCode="R2")])
    total = delta_io.registerRejects(spark, "wwi_test", df, "stg.X", 7, 1, "SRC", "Key",
                                     control.logRejectedRecordSet, rejectStage="Quality")
    assert total == 3
    assert [c[2] for c in control.calls if c[0] == "logRejectedRecordSet"] == ["R1", "R2"]


def test_measure_row_status_and_payload(spark):
    assert delta_io.measureRow(1, 2, "o", "c", 3.0, thresholdValue=2)["ResultStatus"] == "Warned"
    assert delta_io.measureRow(1, 2, "o", "c", 1.0, thresholdValue=2)["ResultStatus"] == "Passed"
    assert delta_io.measureRow(1, 2, "o", "c", -1)["ResultStatus"] == "NotEvaluated"
    df = delta_io.withPayload(spark.createDataFrame([Row(A=1, RejectBranch="x")]), excludeColumns=["RejectBranch"])
    assert df.collect()[0]["RecordPayload"] == '{"A":1}'


def test_configuration_decimal_falls_back():
    assert delta_io.configurationDecimal(control.getConfiguration, None, "c", "MaxRejectPercent", "DEV", 5) == Decimal("5")

    def configured(spark, catalog, key, environmentCode=None):
        return "1"
    assert delta_io.configurationDecimal(configured, None, "c", "MaxRejectPercent", "PROD", 5) == Decimal("1")
