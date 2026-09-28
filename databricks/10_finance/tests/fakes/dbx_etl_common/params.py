import datetime as dt


def getJobParams(dbutils) -> dict:
    get = dbutils.widgets.get
    return {
        "batchId": int(get("BatchId") or 0),
        "businessDate": dt.date.fromisoformat(get("BusinessDate")),
        "reloadFullHistory": str(get("ReloadFullHistory")).lower() == "true",
        "environmentCode": get("EnvironmentCode"),
        "restartFromStep": get("RestartFromStep"),
        "catalog": get("catalog"),
    }
