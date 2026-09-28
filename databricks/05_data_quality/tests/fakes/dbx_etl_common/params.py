def getJobParams(dbutils) -> dict:
    return {"batchId": 0, "businessDate": None, "reloadFullHistory": False, "environmentCode": "DEV",
            "restartFromStep": "", "catalog": "wwi_test"}
