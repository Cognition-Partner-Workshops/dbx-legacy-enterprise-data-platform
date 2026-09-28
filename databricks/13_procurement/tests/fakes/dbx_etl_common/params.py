from datetime import date, datetime


def _widget(dbutils, name, default=""):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


def getJobParams(dbutils):
    businessDate = _widget(dbutils, "BusinessDate", "")
    return {
        "batchId": int(_widget(dbutils, "BatchId", "0") or 0),
        "businessDate": datetime.strptime(businessDate, "%Y-%m-%d").date() if businessDate else date.today(),
        "reloadFullHistory": _widget(dbutils, "ReloadFullHistory", "False").strip().lower() == "true",
        "environmentCode": _widget(dbutils, "EnvironmentCode", "DEV"),
        "restartFromStep": _widget(dbutils, "RestartFromStep", ""),
        "catalog": _widget(dbutils, "catalog", "wwi_dev"),
    }
