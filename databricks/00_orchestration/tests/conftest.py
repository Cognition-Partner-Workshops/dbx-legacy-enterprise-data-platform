import json
import os
import sys

import pytest

ORCH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPO = os.path.abspath(os.path.join(ORCH, "..", ".."))
for p in (os.path.join(ORCH, "src"), os.path.join(ORCH, "tools")):
    if p not in sys.path:
        sys.path.insert(0, p)


@pytest.fixture(scope="session")
def plan():
    with open(os.path.join(REPO, "ssis", "orchestration-plan.json"), encoding="utf-8") as fh:
        return json.load(fh)
