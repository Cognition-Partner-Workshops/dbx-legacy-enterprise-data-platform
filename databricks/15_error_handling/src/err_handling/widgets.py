"""Package-level parameters that sit beside the six standard job parameters."""


def getText(dbutils, name, default=""):
    """Return a widget/job parameter as a string, creating the widget when absent."""
    try:
        dbutils.widgets.text(name, default)
    except Exception:  # widget already exists with a different default
        pass
    value = dbutils.widgets.get(name)
    return default if value is None or value == "" else value


def getInt(dbutils, name, default):
    return int(getText(dbutils, name, str(default)))


def getBool(dbutils, name, default):
    return parseBool(getText(dbutils, name, str(default)))


def parseBool(value):
    return str(value).strip().lower() in ("1", "true", "yes", "y")
