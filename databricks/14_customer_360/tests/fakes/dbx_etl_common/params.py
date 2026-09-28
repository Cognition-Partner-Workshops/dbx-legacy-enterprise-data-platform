from datetime import date


def getJobParams(dbutils):
    def get(name, default):
        try:
            return dbutils.widgets.get(name) or default
        except Exception:  # noqa: BLE001
            return default

    return {
        "batchId": int(get("BatchId", "0")),
        "businessDate": date.fromisoformat(get("BusinessDate", "1900-01-01")),
        "reloadFullHistory": str(get("ReloadFullHistory", "False")).lower() == "true",
        "environmentCode": get("EnvironmentCode", "DEV"),
        "restartFromStep": get("RestartFromStep", ""),
        "catalog": get("catalog", "wwi_test"),
    }
