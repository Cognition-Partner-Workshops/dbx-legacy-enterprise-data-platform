import json
import os

import pytest

from platform_control import maintenance, recon
from platform_control.config import ALL_OWNED_PACKAGES
from platform_control.control import ControlFramework
from platform_control.files import FileOps
from platform_control.runners import PACKAGE_RUNNERS, runStandalone


@pytest.fixture
def cf(spark, cfg):
    return ControlFramework(spark, cfg)


def testRunnerRegistryCoversEveryNonMasterPackage():
    masters = {p for p in ALL_OWNED_PACKAGES if p.startswith("Master_")}
    assert set(PACKAGE_RUNNERS) == set(ALL_OWNED_PACKAGES) - masters


def testPurgeControlHistoryDryRunAndReal(spark, cf):
    batchId = cf.startBatch("Old", "Adhoc", jobRunId="old")
    cf.endBatch(batchId)
    spark.sql(f"UPDATE {cf.t('etl_batch')} SET started_at_utc = timestamp'2020-01-01', completed_at_utc = timestamp'2020-01-01' WHERE batch_id = {batchId}")
    dry = maintenance.purgeControlHistory(cf, None, dryRun=True)
    assert spark.sql(f"SELECT COUNT(*) AS n FROM {cf.t('etl_batch')} WHERE batch_id = {batchId}").first()["n"] == 1
    real = maintenance.purgeControlHistory(cf, None, dryRun=False)
    assert spark.sql(f"SELECT COUNT(*) AS n FROM {cf.t('etl_batch')} WHERE batch_id = {batchId}").first()["n"] == 0
    assert dry and real


def testOptimizeAndStatisticsRunLocally(cf):
    assert maintenance.rebuildIndexes(cf, None, tables=["etl_batch"])
    assert maintenance.updateStatistics(cf, None, modificationThresholdRows=0, tables=["etl_batch"])


def testArchiveAndDiskSpace(cf, cfg):
    fileOps = FileOps(cfg.volumeRoot)
    fileOps.ensureDir(fileOps.join("processed"))
    path = os.path.join(fileOps.join("processed"), "old_partner_sales_na.csv")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("x\n")
    os.utime(path, (0, 0))
    result = maintenance.archiveProcessedFiles(cf, None, fileOps, minimumFileAgeHours=6)
    assert result["archived"] >= 1 if "archived" in result else result
    disk = maintenance.checkDiskSpace(cf, None, fileOps, minimumFreePercent=0)
    assert disk


def testValidateConfigurationFlagsMissingKeys(cf):
    cf.insertRows(
        "etl_required_configuration_key",
        [{"configuration_key": "Unit.MissingKey", "environment_code": "ALL", "is_mandatory": True, "description": "unit"}],
    )
    with pytest.raises(Exception):  # noqa: B017
        maintenance.validateConfiguration(cf, None, failOnMissingKey=True)
    result = maintenance.validateConfiguration(cf, None, failOnMissingKey=False)
    assert result


def testStandaloneRunnerOpensAndClosesBatch(cf):
    result = runStandalone(cf, "MNT_Update_Statistics", {"ModificationThresholdRows": "0"})
    assert result["batch"]["batchStatus"] in ("Succeeded", "SucceededWithWarnings")


def testReconWritesOneRowPerPackage(cf, cfg):
    result = recon.runRecon(cf)
    assert result["rowCount"] == len(ALL_OWNED_PACKAGES)
    rows = cf.spark.sql(f"SELECT unit, verdict, checks FROM {cfg.evidenceTable()} WHERE run_id = '{result['runId']}'").collect()
    assert sorted(r["unit"] for r in rows) == sorted(ALL_OWNED_PACKAGES)
    for r in rows:
        checks = json.loads(r["checks"])
        assert {c["check"] for c in checks} >= {"row_count", "checksum"}
        assert r["verdict"] in ("PASS", "FAIL", "PARTIAL", "NOT_APPLICABLE")
    masters = {r["unit"]: r["verdict"] for r in rows if r["unit"].startswith("Master_")}
    assert set(masters.values()) == {"PARTIAL"}
