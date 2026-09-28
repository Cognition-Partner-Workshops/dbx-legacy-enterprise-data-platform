"""Build binaryFile-shaped DataFrames from text so the parsers can be exercised without a file system."""

import datetime

from pyspark.sql import types as T

from wwi_file_ingestion import runner

BINARY_FILE_SCHEMA = T.StructType([
    T.StructField("path", T.StringType()),
    T.StructField("modificationTime", T.TimestampType()),
    T.StructField("length", T.LongType()),
    T.StructField("content", T.BinaryType()),
])


def binaryFileDf(spark, spec, fileName, text, codec=None, modifiedAt=None):
    content = text.encode(codec or spec.pythonCodec)
    row = (
        "/Volumes/wwi_test/bronze/inbound/%s/%s" % (spec.landingPath.replace("inbound/", ""), fileName),
        modifiedAt or datetime.datetime(2024, 5, 17, 6, 30, 0),
        len(content),
        bytearray(content),
    )
    return spark.createDataFrame([row], BINARY_FILE_SCHEMA)


def parseText(spark, spec, text, fileName=None, publishedRatesDf=None, codec=None):
    fileName = fileName or spec.fileGlob.replace("*", "20240517_001")
    return runner.parseFile(spark, spec, binaryFileDf(spark, spec, fileName, text, codec), publishedRatesDf)


def byRow(parsed):
    return {r["SourceRowNumber"]: r for r in parsed.collect()}
