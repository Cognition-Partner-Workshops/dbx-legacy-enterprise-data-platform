from datetime import date


def getJobParams(dbutils):
    get = dbutils.widgets.get
    return {
        "batchId": int(get("BatchId") or 0),
        "businessDate": date.fromisoformat(get("BusinessDate") or "1900-01-01"),
        "reloadFullHistory": (get("ReloadFullHistory") or "False").lower() == "true",
        "environmentCode": get("EnvironmentCode") or "DEV",
        "restartFromStep": get("RestartFromStep") or "",
        "catalog": get("catalog"),
    }
