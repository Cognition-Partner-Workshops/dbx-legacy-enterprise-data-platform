import datetime


def getJobParams(dbutils):
    def get(name, default):
        try:
            return dbutils.widgets.get(name)
        except Exception:
            return default

    businessDate = get("BusinessDate", "")
    return {
        "batchId": int(get("BatchId", "0") or 0),
        "businessDate": datetime.date.fromisoformat(businessDate) if businessDate else datetime.date.today(),
        "reloadFullHistory": str(get("ReloadFullHistory", "False")).lower() in ("1", "true", "yes"),
        "environmentCode": get("EnvironmentCode", "DEV"),
        "restartFromStep": get("RestartFromStep", ""),
        "catalog": get("catalog", "wwi_dev"),
    }
