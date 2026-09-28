from datetime import date


def getJobParams(dbutils):
    def get(name, default):
        try:
            return dbutils.widgets.get(name)
        except Exception:
            return default
    raw = get("BusinessDate", "1900-01-01")
    return {
        "batchId": int(get("BatchId", "0") or 0),
        "businessDate": date.fromisoformat(raw) if raw and raw != "yyyy-MM-dd" else date.today(),
        "reloadFullHistory": get("ReloadFullHistory", "False").lower() == "true",
        "environmentCode": get("EnvironmentCode", "DEV"),
        "restartFromStep": get("RestartFromStep", ""),
        "catalog": get("catalog", "wwi_test"),
    }
