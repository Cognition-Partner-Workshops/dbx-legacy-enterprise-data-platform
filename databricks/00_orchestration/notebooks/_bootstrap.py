# Databricks notebook source
# MAGIC %md
# MAGIC Shared prologue for the `00_orchestration` notebooks (`%run ./_bootstrap`): resolves widgets into
# MAGIC `ctx`, imports `dbx_etl_common` (installed from the bundle wheel via the job environment) and the
# MAGIC `wwi_orchestration` helpers from `../src`, and exposes `readVariables()` / `evaluateEdges()`.

# COMMAND ----------
import json
import os
import sys
from typing import Any, Dict, List

from dbx_etl_common import control, naming  # noqa: F401  (installed from ../../common/dbx_etl_common/dist/*.whl)

_notebookDir = os.getcwd()
try:
    _notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
    _notebookDir = os.path.join("/Workspace", _notebookPath.lstrip("/"), "..")
except Exception:  # pragma: no cover - interactive / non-Databricks execution
    pass
for candidate in (os.path.abspath(os.path.join(_notebookDir, "..", "src")), os.path.abspath(os.path.join(os.getcwd(), "..", "src"))):
    if os.path.isdir(candidate) and candidate not in sys.path:
        sys.path.insert(0, candidate)
from wwi_orchestration import plan_expression, tsql_translate  # noqa: E402,F401


def widget(name: str, default: str = "") -> str:
    try:
        return dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        return default


def isUnresolved(text: str) -> bool:
    """A `{{tasks.x.values.y}}` reference of a task that never ran arrives verbatim."""
    return text is None or text == "" or text.startswith("{{")


def widgetInt(name: str, default: int = 0) -> int:
    return int(plan_expression.coerce(widget(name), default))


ctx: Dict[str, Any] = {
    "catalog": widget("catalog"),
    "masterName": widget("masterName"),
    "jobRunId": widget("jobRunId"),
}
if isUnresolved(ctx["catalog"]):
    raise ValueError("the 'catalog' job parameter is required (naming contract: catalog = wwi_${bundle.target})")


def readVariables() -> Dict[str, Any]:
    """The plan variables this node needs: `{"User::X": {"ref": <resolved or literal>, "default": ...}}`."""
    spec = json.loads(widget("variables", "{}") or "{}")
    table: Dict[str, Any] = {}
    for name, entry in spec.items():
        default = plan_expression.planValue(entry.get("default"))
        ref = entry.get("ref")
        if isinstance(ref, str):
            table[name] = plan_expression.coerce(ref, default if default is not None else "")
        else:
            table[name] = plan_expression.planValue(ref) if ref is not None else default
    return table


def evaluateEdges(variables: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Evaluate this node's outgoing expression edges and publish `edge_<n>` task values."""
    edges = json.loads(widget("edges", "[]") or "[]")
    results = []
    for edge in edges:
        verdict = plan_expression.test(edge["expression"], variables)
        dbutils.jobs.taskValues.set(key=edge["key"], value="true" if verdict else "false")  # noqa: F821
        results.append({"key": edge["key"], "expression": edge["expression"], "result": verdict})
    return results


def setTaskValue(key: str, value: Any) -> None:
    dbutils.jobs.taskValues.set(key=key, value="" if value is None else str(value))  # noqa: F821
