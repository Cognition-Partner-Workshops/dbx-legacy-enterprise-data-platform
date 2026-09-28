from datetime import date


def getJobParams(dbutils) -> dict:
    get = dbutils.widgets.get
    businessDate = get("BusinessDate")
    return {
        "batchId": int(get("BatchId") or 0),
        "businessDate": date.fromisoformat(businessDate) if businessDate else None,
        "reloadFullHistory": str(get("ReloadFullHistory")).strip().lower() in ("true", "1", "yes"),
        "environmentCode": get("EnvironmentCode") or "DEV",
        "restartFromStep": get("RestartFromStep") or "",
        "catalog": get("catalog"),
    }
