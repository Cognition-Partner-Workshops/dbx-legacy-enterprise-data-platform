from datetime import date


def getJobParams(dbutils):
    return {
        "batchId": 0, "businessDate": date.today(), "reloadFullHistory": False,
        "environmentCode": "DEV", "restartFromStep": "", "catalog": "wwi_test",
    }
