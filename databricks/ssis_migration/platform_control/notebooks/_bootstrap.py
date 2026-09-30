# Databricks notebook source
# Shared bootstrap for the thin task notebooks: put ../src on sys.path and build PlatformConfig from widgets.
import json
import os
import sys

_here = os.getcwd()
for candidate in (os.path.join(_here, "..", "src"), os.path.join(_here, "src")):
    if os.path.isdir(candidate) and candidate not in sys.path:
        sys.path.insert(0, os.path.abspath(candidate))


def widget(name: str, default: str = "") -> str:
    try:
        value = dbutils.widgets.get(name)  # noqa: F821
    except Exception:  # noqa: BLE001 - widget not defined for this run
        return default
    return default if value is None else value


def platformConfig():
    from platform_control.config import PlatformConfig

    return PlatformConfig.fromEnv(
        catalog=widget("catalog", "otterorders_migration"),
        schema=widget("schema", "ssis_platform_control"),
        environmentCode=widget("environment_code", "DEV"),
        gitSha=widget("git_sha", "unknown"),
        siblingDispatchOnMissing=widget("sibling_dispatch_on_missing", "skip"),
    )


def parametersJson(name: str = "parameters_json") -> dict:
    raw = widget(name, "{}")
    return json.loads(raw) if raw else {}
