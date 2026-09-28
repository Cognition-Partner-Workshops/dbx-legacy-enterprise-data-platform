"""Transformation logic for the WWI_Procurement (ssis/13_procurement) migration.

Every function is a pure DataFrame -> DataFrame (or DataFrame -> value) mapping so the
same code runs in the notebooks and in the local pytest suite. Nothing here touches
the control framework; that is the notebooks' job through dbx_etl_common.
"""

PROJECT_NAME = "WWI_Procurement"
SOURCE_SYSTEM_CODE = "ORAERP"
