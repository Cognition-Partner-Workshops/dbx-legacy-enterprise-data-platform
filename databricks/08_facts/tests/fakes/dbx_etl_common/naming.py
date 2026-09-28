def table(catalog, schema, table):
    """Local Spark has no Unity Catalog: the default spark_catalog cannot be
    spelled as a three-part name in DeltaTable.forName, so drop it."""
    if catalog == "spark_catalog":
        return "%s.%s" % (schema, table)
    return "%s.%s.%s" % (catalog, schema, table)
