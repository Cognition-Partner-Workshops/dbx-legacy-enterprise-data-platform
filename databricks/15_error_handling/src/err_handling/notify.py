"""ERR_Notify_Operations: severity derivation and message content."""

import csv
import os

CRITICALITY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "package_criticality.csv")


def loadPackageCriticality(path=CRITICALITY_FILE):
    """package name -> criticality, taken from docs/inventories/ssis-packages.csv."""
    with open(path, newline="") as handle:
        return {row["package"]: row["criticality"] for row in csv.DictReader(handle)}


def deriveSeverity(failedStepCount, highCriticalityFailures):
    """Legacy Assess Batch Outcome: CRITICAL if any high-criticality step failed,
    WARNING if anything failed, otherwise INFO."""
    if int(highCriticalityFailures) > 0:
        return "CRITICAL"
    if int(failedStepCount) > 0:
        return "WARNING"
    return "INFO"


def batchFailureSubject(environmentCode, batchId, batchType):
    return "[%s] Batch %s (%s) failed" % (environmentCode, batchId, batchType)


def batchFailureBody(failedStepNames, lastErrorDescription):
    steps = ", ".join(failedStepNames)
    return "Failed steps: %s. Last error: %s" % (steps, lastErrorDescription or "none recorded")


def batchWarningBody(rejectedRowCount):
    return "Rejected rows in this batch: %s" % int(rejectedRowCount)


BATCH_WARNING_SUBJECT = "Batch completed with rejected rows"


def webhookPayload(notification, environmentCode):
    """JSON body posted to the operations webhook, one call per notification row."""
    return {
        "source": "wwi_15_error_handling",
        "environmentCode": environmentCode,
        "batchId": notification["BatchId"],
        "notificationTypeCode": notification["NotificationTypeCode"],
        "severity": notification["Severity"],
        "subject": notification["Subject"],
        "body": notification["Body"],
        "raisedAtUtc": str(notification["RaisedAtUtc"]),
        "text": "%s: %s\n%s" % (notification["Severity"], notification["Subject"], notification["Body"]),
    }
