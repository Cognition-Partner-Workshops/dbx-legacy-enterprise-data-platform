"""Sales Facts slice: pure PySpark transforms shared by the pipeline and the fixtures.

Every function here is DataFrame in / DataFrame out and has no side effects, so the
same code runs under the declarative pipeline on Databricks and under a local
SparkSession in the fixture suite.
"""
