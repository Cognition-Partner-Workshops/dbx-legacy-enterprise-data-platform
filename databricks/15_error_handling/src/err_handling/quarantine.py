"""ERR_Quarantine_BadFiles: reason classification and quarantine path building."""

import posixpath


def quarantineReasonCode(fileSizeBytes, structuralCheckStatus, feedCode):
    """Legacy Gather Bad Files CASE expression, evaluated in the same order."""
    if fileSizeBytes == 0:
        return "ZERO_LENGTH"
    if structuralCheckStatus == "Failed":
        return "STRUCTURE"
    if feedCode is None:
        return "UNKNOWN_FEED"
    return "OTHER"


def quarantineFolder(quarantineRoot, quarantineFolderName, asOfDate):
    """Legacy Build Quarantine Path: <QuarantineFolder>\\YYYYMM, rooted in the quarantine volume."""
    return posixpath.join(quarantineRoot.rstrip("/"), quarantineFolderName.strip("/"), asOfDate.strftime("%Y%m"))


def quarantineDestination(quarantineRoot, quarantineFolderName, asOfDate, fileName):
    return posixpath.join(quarantineFolder(quarantineRoot, quarantineFolderName, asOfDate), fileName)
