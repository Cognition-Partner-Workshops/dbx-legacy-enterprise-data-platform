# Databricks notebook source
# Shared bootstrap: puts the bundle's src/ on sys.path so notebooks stay thin.
import os
import sys

_here = os.getcwd()
for _candidate in (os.path.join(_here, "..", "src"), os.path.join(_here, "src")):
    _src = os.path.abspath(_candidate)
    if os.path.isdir(_src) and _src not in sys.path:
        sys.path.insert(0, _src)
        break
