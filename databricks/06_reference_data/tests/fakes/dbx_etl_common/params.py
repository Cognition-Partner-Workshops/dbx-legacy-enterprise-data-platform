import datetime


def _get(dbutils, name, default=""):
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return default
    return default if value is None or value == "" else value


def getJobParams(dbutils):
    businessDate = _get(dbutils, "BusinessDate", "")
    return {
        "batchId": int(_get(dbutils, "BatchId", "0") or 0),
        "businessDate": datetime.date.fromisoformat(businessDate[:10]) if businessDate else None,
        "reloadFullHistory": _get(dbutils, "ReloadFullHistory", "False").lower() in ("true", "1", "y", "yes"),
        "environmentCode": _get(dbutils, "EnvironmentCode", "DEV"),
        "restartFromStep": _get(dbutils, "RestartFromStep", ""),
        "catalog": _get(dbutils, "catalog", "spark_catalog"),
    }
