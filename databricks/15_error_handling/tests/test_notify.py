from err_handling import notify


def test_criticality_lookup_comes_from_inventory():
    crit = notify.loadPackageCriticality()
    assert crit["ERR_Handle_PackageFailure"] == "high"
    assert crit["EXT_ORA_CustomerMaster"] == "high"
    assert len(crit) > 100


def test_derive_severity():
    assert notify.deriveSeverity(0, 0) == "INFO"
    assert notify.deriveSeverity(2, 0) == "WARNING"
    assert notify.deriveSeverity(2, 1) == "CRITICAL"


def test_message_content_matches_legacy_send_mail_text():
    assert notify.batchFailureSubject("DEV", 42, "Daily") == "[DEV] Batch 42 (Daily) failed"
    assert notify.batchFailureBody(["Extract Oracle", "Stage Load"], "ORA-12541") == \
        "Failed steps: Extract Oracle, Stage Load. Last error: ORA-12541"
    assert notify.batchFailureBody(["X"], None) == "Failed steps: X. Last error: none recorded"
    assert notify.batchWarningBody(7) == "Rejected rows in this batch: 7"
    assert notify.BATCH_WARNING_SUBJECT == "Batch completed with rejected rows"


def test_webhook_payload():
    row = {"BatchId": 1, "NotificationTypeCode": "BATCH_FAILURE", "Severity": "CRITICAL", "Subject": "s", "Body": "b", "RaisedAtUtc": "2026-01-01"}
    payload = notify.webhookPayload(row, "PROD")
    assert payload["environmentCode"] == "PROD" and payload["severity"] == "CRITICAL"
    assert payload["text"] == "CRITICAL: s\nb"
