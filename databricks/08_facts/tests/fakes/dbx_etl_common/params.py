from datetime import date


def getJobParams(dbutils):
    def get(name, default):
        try:
            return dbutils.widgets.get(name)
        except Exception:
            return default

    return {
        "batchId": int(get("BatchId", "0") or 0),
        "businessDate": date.fromisoformat(get("BusinessDate", date.today().isoformat())),
        "reloadFullHistory": str(get("ReloadFullHistory", "False")).lower() in ("1", "true", "yes"),
        "environmentCode": get("EnvironmentCode", "DEV"),
        "restartFromStep": get("RestartFromStep", ""),
        "catalog": get("catalog", "wwi_dev"),
    }
