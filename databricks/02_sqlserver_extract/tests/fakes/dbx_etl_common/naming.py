def table(catalog: str, schema: str, tableName: str) -> str:
    # Local Spark has no Unity Catalog, so the fake flattens catalog.schema into one database.
    return "%s_%s.%s" % (catalog, schema, tableName)
