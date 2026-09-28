from datetime import datetime


def getJobParams(dbutils):
    def get(name, default=""):
        try:
            return dbutils.widgets.get(name)
        except Exception:
            return default

    businessDate = get("BusinessDate")
    return {
        "batchId": int(get("BatchId", "0") or 0),
        "businessDate": datetime.strptime(businessDate, "%Y-%m-%d").date() if businessDate else None,
        "reloadFullHistory": str(get("ReloadFullHistory", "False")).lower() == "true",
        "environmentCode": get("EnvironmentCode", "DEV"),
        "restartFromStep": get("RestartFromStep", ""),
        "catalog": get("catalog", ""),
    }
