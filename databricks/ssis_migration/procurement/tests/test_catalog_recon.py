import os

from pyspark.sql import Row

from procurement.catalog_ingest import RAW_SCHEMA, transformSupplierCatalog
from procurement.recon import checksum, normalize

SAMPLES = os.path.join(os.path.dirname(__file__), "..", "samples", "supplier_catalog")


def test_supplier_catalog_footer_and_row_validation(spark):
    raw = (
        spark.read.format("csv").schema(RAW_SCHEMA).option("sep", "|").option("encoding", "ISO-8859-1").load(f"{SAMPLES}/*.psv")
        .withColumn("source_file_path", __import__("pyspark.sql.functions", fromlist=["col"]).col("_metadata.file_path"))
    )
    from pyspark.sql import functions as F
    raw = raw.withColumn("source_file_name", F.element_at(F.split(F.col("source_file_path"), "/"), -1))
    landed, rejected, files = transformSupplierCatalog(raw, batchId=1)
    status = {r.source_file_name: r.file_status for r in files.collect()}
    assert status["supplier_catalog_20260101_001.psv"] == "Processed"
    assert status["supplier_catalog_20260401_001.psv"] == "Processed", "malformed detail rows are rejected row-wise, the file still lands"
    assert status["supplier_catalog_20260401_002.psv"] == "Quarantined", "footer checksum mismatch quarantines the whole file"
    assert landed.where("source_file_name = 'supplier_catalog_20260401_002.psv'").count() == 0
    assert landed.where("source_file_name = 'supplier_catalog_20260101_001.psv'").count() == 40
    reasons = {r.reject_reason_code for r in rejected.where("source_file_name = 'supplier_catalog_20260401_001.psv'").collect()}
    assert reasons and reasons <= {"MISSING_ITEM_CODE", "INVALID_NET_PRICE", "LIST_BELOW_NET", "INVALID_EFFECTIVE_FROM"}
    assert landed.where("hazardous_flag = 'Y'").count() >= 1


def test_checksum_is_order_independent_and_type_normalised(spark):
    cols = {"k": "long", "amt": "decimal(19,4)", "flag": "int"}
    a = spark.createDataFrame([Row(k=1, amt=1.5, flag=True), Row(k=2, amt=2.0, flag=False)])
    b = spark.createDataFrame([Row(k=2, amt="2.00", flag=0), Row(k=1, amt="1.50", flag=1)])
    assert checksum(normalize(a, cols), cols) == checksum(normalize(b, cols), cols)
    c = spark.createDataFrame([Row(k=1, amt=1.5, flag=True), Row(k=2, amt=2.5, flag=False)])
    assert checksum(normalize(a, cols), cols) != checksum(normalize(c, cols), cols)
