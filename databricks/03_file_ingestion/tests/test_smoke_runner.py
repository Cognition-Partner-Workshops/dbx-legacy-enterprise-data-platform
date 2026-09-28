"""End-to-end run of one package against local Delta + a temp 'volume' tree (batch file discovery)."""

import os

from wwi_file_ingestion import control_totals as ct
from wwi_file_ingestion import feeds, runner

from conftest import CATALOG

NA_FILE = (
    "RecordType,PartnerCode,StoreNumber,TransactionNumber,TransactionDateText,ProductCode,Upc,QuantityText,"
    "UnitPriceText,StateTaxText,CountyTaxText,LineTotalText,CurrencyCode,StateCode,PostalCode,FooterRecordCountText\n"
    "H,ACME,,,,,,,,,,,,,,\n"
    "D,ACME,0101,T-1,05/17/2024,P-1,012345678905,2,10.00,0.50,0.25,20.75,USD,WA,98101,\n"
    "D,ACME,0101,T-2,05/17/2024,P-2,012345678912,1,5.00,0.25,0.10,5.35,USD,WA,98101,\n"
    "D,ACME,0101,,05/17/2024,P-3,012345678929,1,5.00,0.25,0.10,5.35,USD,WA,98101,\n"
    "D,ACME,0101,T-4,05/17/2024,P-4,012345678936,abc,5.00,0.25,0.10,5.35,USD,WA,98101,\n"
    "D,ACME,0101,T-5,05/17/2024,P-5\n"
    "T,,,,,,,,,,,,,,,2\n"
)


def dropFile(catalog, spec, name, text, codec=None):
    folder = runner.inboundPath(catalog, spec)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, name)
    with open(path, "wb") as handle:
        handle.write(text.encode(codec or spec.pythonCodec))
    return path


def test_partner_sales_na_end_to_end(spark, volumeRoot, dbutils, control, cleanTables):
    spec = feeds.PARTNER_SALES_NA
    path = dropFile(CATALOG, spec, "partner_sales_na_20240517_001.csv", NA_FILE)

    summary = runner.runPackage(spark, dbutils, control, CATALOG, batchId=7, packageExecutionId=70,
                                spec=spec, useAutoLoader=False)

    assert summary.filesDiscovered == 1
    assert summary.rowsInserted == 2
    assert summary.rowsRejected == 3  # missing TransactionNumber, bad quantity, short line
    landed = spark.table("%s.bronze.raw_file_partner_sales" % CATALOG)
    rows = {r["TransactionNumber"]: r for r in landed.collect()}
    assert set(rows) == {"T-1", "T-2"}
    assert str(rows["T-1"]["TaxAmount"]) == "0.7500"
    assert rows["T-1"]["TaxTreatmentCode"] == "SALESTAX"
    assert rows["T-1"]["RegionCode"] == "NA"
    assert rows["T-1"]["BatchId"] == 7 and rows["T-1"]["PackageExecutionId"] == 70
    assert rows["T-1"]["FileName"] == "partner_sales_na_20240517_001.csv"
    assert rows["T-1"]["SourceRowNumber"] == 3

    err = spark.table("%s.silver.%s" % (CATALOG, feeds.ERR_REJECTED_FILE_ROW))
    reasons = {r["SourceRowNumber"]: r["RejectReasonCode"] for r in err.collect()}
    assert reasons == {5: "MALFORMED", 6: "CONVERSION", 7: "COLUMN_COUNT", 8: "FOOTER"}

    # archived (yyyy/MM layout) and .rej written
    assert not os.path.exists(path)
    archived = [dst for _, dst in dbutils.fs.moves]
    assert len(archived) == 1 and "/archive/partner/na/" in archived[0]
    rej = ct.rejectFilePath(CATALOG, spec, "partner_sales_na_20240517_001.csv")
    assert os.path.exists(rej)

    log = spark.table("%s.etl.%s" % (CATALOG, feeds.FILE_INGESTION_LOG)).collect()
    assert len(log) == 1 and log[0]["Status"] == "Processed" and log[0]["CompletedAtUtc"] is not None

    codes = sorted(c["rejectReasonCode"] for c in control.callsNamed("logRejectedRecordSet"))
    assert codes == ["COLUMN_COUNT", "CONVERSION", "MALFORMED"]
    assert control.callsNamed("logRowCount")[0]["targetRowCount"] == 2
