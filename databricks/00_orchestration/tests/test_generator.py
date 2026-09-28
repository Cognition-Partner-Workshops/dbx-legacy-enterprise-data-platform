"""build_jobs_from_plan.py output is checked in, current, and carries the plan's node/edge sets."""
import glob
import os
import re
import subprocess
import sys

import build_jobs_from_plan as gen
import check_plan_parity as parity

ORCH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MASTERS = ["Master_Daily_ETL", "Master_Hourly_Incremental", "Master_Customer_Sync", "Master_Month_End",
           "Master_Intraday_Inventory", "Master_Weekly_Reference_Load", "Master_File_Ingestion",
           "Master_Finance_Close", "Master_Weekly_Maintenance"]


def test_generated_yaml_is_current(plan):
    roots = {r["root"] for r in plan["roots"]}
    assert roots == set(MASTERS)
    for root in plan["roots"]:
        path = os.path.join(ORCH, "resources", root["root"] + ".yml")
        assert open(path, encoding="utf-8").read() == gen.render(root), f"{path} is stale; re-run tools/build_jobs_from_plan.py"


def test_parity_checker_passes():
    assert parity.main() == 0


def test_referenced_notebooks_exist_and_parameters_declared():
    for path in glob.glob(os.path.join(ORCH, "resources", "Master_*.yml")):
        text = open(path, encoding="utf-8").read()
        for nb in set(re.findall(r"notebook_path: (\S+)", text)):
            target = os.path.normpath(os.path.join(ORCH, "resources", nb.strip('"')))
            assert os.path.exists(target), (path, nb)
        for p in gen.STANDARD_PARAMETERS + ("catalog",):
            assert re.search(rf"- name: {p}\b", text), (path, p)


def test_schedules_and_paused_masters():
    for master, (cron, paused, _) in gen.SCHEDULES.items():
        text = open(os.path.join(ORCH, "resources", master + ".yml"), encoding="utf-8").read()
        assert f'quartz_cron_expression: "{cron}"' in text
        if paused:
            assert "pause_status: PAUSED" in text
        else:
            assert "pause_status: \"${var.schedule_pause_status}\"" in text


def test_generator_check_mode_exit_code():
    r = subprocess.run([sys.executable, os.path.join(ORCH, "tools", "build_jobs_from_plan.py"), "--check"], capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
