from datetime import date


def getJobParams(dbutils):
    def get(name, default):
        try:
            value = dbutils.widgets.get(name)
        except Exception:
            return default
        return default if value in (None, "") else value

    return {
        "batchId": int(get("BatchId", "0")),
        "businessDate": date.fromisoformat(get("BusinessDate", date.today().isoformat())),
        "reloadFullHistory": str(get("ReloadFullHistory", "False")).strip().lower() in ("true", "1", "yes"),
        "environmentCode": get("EnvironmentCode", "DEV"),
        "restartFromStep": get("RestartFromStep", ""),
        "catalog": get("catalog", "wwi_dev"),
    }
